use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use std::time::{SystemTime, UNIX_EPOCH};

use mysql_async::{Conn, Pool, params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use uuid::{Builder, Uuid};

use crate::{
    profile::{self, ProfileBasedIn},
    user::UserId,
};

const MAX_MESSAGE: usize = 300;
// ER_DUP_ENTRY, vtgate passes the shard's error code through
const DUPLICATE_ENTRY: u16 = 1062;
const ALREADY_INVITED: &str = "You already sent this person an invite";
// how old an invite has to be before it can be sent again, unless it was accepted
const RESEND_AFTER_DAYS: u32 = 120;
// invites per page of the requests screen. a page shorter than this is the last one, the web app stops there
const LIST_PAGE_SIZE: usize = 20;

/// An invite with a message, from a post or a profile. it's to the person, not the post: the post is only where they were found
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateInviteRequest {
    to_user_id: Uuid,
    message: String,
}

impl CreateInviteRequest {
    /// The request with its message trimmed, when it's valid
    fn validate(mut self, from_user_id: Uuid) -> Result<Self, &'static str> {
        if self.to_user_id == from_user_id {
            return Err("You can't invite yourself");
        }
        let message = self.message.trim();
        if message.is_empty() || message.chars().count() > MAX_MESSAGE {
            return Err("Enter a message of up to 300 characters");
        }
        self.message = message.to_owned();
        Ok(self)
    }
}

/// `POST /invite`. one invite per pair of people: inviting the same person again is a conflict, unless the invite
/// is at least RESEND_AFTER_DAYS old and wasn't accepted, declined or not. then it's sent again in its place.
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn send(
    UserId(from_user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<CreateInviteRequest>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let invite = request
        .validate(from_user_id)
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let mut conn = pool.get_conn().await.map_err(failed)?;
    // the recipient's requests show the sender's name, which is on their profile
    if !profile::exists(&mut conn, from_user_id)
        .await
        .map_err(failed)?
    {
        return Err((StatusCode::BAD_REQUEST, "Set up your profile first"));
    }
    if !profile::exists(&mut conn, invite.to_user_id)
        .await
        .map_err(failed)?
    {
        return Err((StatusCode::NOT_FOUND, "User not found"));
    }

    // an old enough one is sent again in its place, it's one row per pair
    if resend(&mut conn, from_user_id, &invite)
        .await
        .map_err(failed)?
    {
        return Ok(StatusCode::CREATED);
    }
    match insert(&mut conn, from_user_id, &invite).await {
        Ok(()) => Ok(StatusCode::CREATED),
        // there's one already, accepted or too recent to send again
        Err(mysql_async::Error::Server(err)) if err.code == DUPLICATE_ENTRY => {
            Err((StatusCode::CONFLICT, ALREADY_INVITED))
        }
        Err(err) => Err(failed(err)),
    }
}

fn failed(err: mysql_async::Error) -> (StatusCode, &'static str) {
    tracing::error!(error = %err, "fail to invite");
    (StatusCode::INTERNAL_SERVER_ERROR, "Failed to invite")
}

/// Sends an earlier invite again with the new message, as if it were new: pending, with a new id so it's on top of
/// the requests again and its age starts over. only one that isn't accepted and is at least RESEND_AFTER_DAYS old, whether it was declined
/// or left unanswered. whether there was one, none when there's no invite or it can't be sent again yet.
/// the age is checked in the same statement that changes it, so two sends at once can't both send it again:
/// the second finds it sent just now
async fn resend(
    conn: &mut Conn,
    from_user_id: Uuid,
    invite: &CreateInviteRequest,
) -> mysql_async::Result<bool> {
    conn.exec_drop(
        r"-- sql
    UPDATE invite
    SET id = :id, message = :message, status = 'pending'
    WHERE to_user_id = :to_user_id AND from_user_id = :from_user_id
        -- AND status <> 'accepted'
        -- sent before the cutoff: a v7 id starts with when it was made, so ids compare as their times do
        AND id < :cutoff",
        params! {
            "id" => Uuid::now_v7().as_bytes().to_vec(),
            "cutoff" => resend_cutoff(now_millis()).as_bytes().to_vec(),
            "to_user_id" => invite.to_user_id.as_bytes().to_vec(),
            "from_user_id" => from_user_id.as_bytes().to_vec(),
            "message" => invite.message.as_str(),
        },
    )
    .await?;
    Ok(conn.affected_rows() > 0)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or_default()
}

/// The smallest v7 id of RESEND_AFTER_DAYS before now: every id below it was made before then.
/// the app's clock, the same one that makes the ids
fn resend_cutoff(now_millis: u64) -> Uuid {
    let resend_after_millis = u64::from(RESEND_AFTER_DAYS) * 24 * 60 * 60 * 1000;
    Builder::from_unix_timestamp_millis(now_millis.saturating_sub(resend_after_millis), &[0; 10])
        .into_uuid()
}

/// Sharded by to_user_id, so the primary key (to_user_id, from_user_id) keeps a pair to one invite within a shard
async fn insert(
    conn: &mut Conn,
    from_user_id: Uuid,
    invite: &CreateInviteRequest,
) -> mysql_async::Result<()> {
    conn.exec_drop(
        r"-- sql
    INSERT INTO invite (id, to_user_id, from_user_id, message)
    VALUES (:id, :to_user_id, :from_user_id, :message)",
        params! {
            // v7 is time ordered, so the requests are newest first by id
            "id" => Uuid::now_v7().as_bytes().to_vec(),
            "to_user_id" => invite.to_user_id.as_bytes().to_vec(),
            "from_user_id" => from_user_id.as_bytes().to_vec(),
            "message" => invite.message.as_str(),
        },
    )
    .await
}

/// `InviteListItem` in the web app: one invite sent to the person asking
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InviteListItem {
    /// uuid v7, so it also holds when it was last sent, to the millisecond: the web app reads the time from it
    id: Uuid,
    from_user_id: Uuid,
    /// empty when the sender's profile is gone
    from_name: String,
    /// as much as the sender shows of where they're based in, none when they show neither
    from_based_in: Option<ProfileBasedIn>,
    message: String,
}

/// `GET /invite/list`
#[derive(Debug, Deserialize)]
pub struct ListInvitesQuery {
    /// the id of the last invite of the page before, the older ones come next. the first page leaves it out.
    /// an id and not a page number, so invites arriving or answered while scrolling don't shift the pages
    before: Option<Uuid>,
}

/// `GET /invite/list`: the pending invites sent to the person asking, newest first, a page at a time
pub async fn list(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Query(query): Query<ListInvitesQuery>,
) -> Result<Json<Vec<InviteListItem>>, (StatusCode, &'static str)> {
    select_received(&pool, user_id, query.before)
        .await
        .map(Json)
        .map_err(|err| {
            tracing::error!(error = %err, "fail to list invites");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load invites")
        })
}

/// id, from_user_id, message
type InviteRow = (Vec<u8>, Vec<u8>, String);

// two round trips are intended for vitess
async fn select_received(
    pool: &Pool,
    user_id: Uuid,
    before: Option<Uuid>,
) -> mysql_async::Result<Vec<InviteListItem>> {
    let mut conn = pool.get_conn().await?;
    let invites: Vec<InviteRow> = conn
        .exec(
            format!(
                r"-- sql
    SELECT
        id,
        from_user_id,
        message
    FROM invite
    WHERE to_user_id = :user_id AND status = 'pending'
        AND (:before IS NULL OR id < :before)
    ORDER BY id DESC
    LIMIT {LIST_PAGE_SIZE}"
            ),
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
                "before" => before.map(|before| before.as_bytes().to_vec()),
            },
        )
        .await?;
    if invites.is_empty() {
        return Ok(Vec::new());
    }

    let senders = profile::select_brief_profiles(
        &mut conn,
        invites
            .iter()
            .map(|(_, from_user_id, ..)| from_user_id)
            .collect(),
    )
    .await?;

    Ok(invites
        .into_iter()
        .map(|(id, from_user_id, message)| {
            let sender = senders.get(&from_user_id);
            let id = Uuid::from_slice(&id).unwrap_or_default();
            InviteListItem {
                id,
                from_user_id: Uuid::from_slice(&from_user_id).unwrap_or_default(),
                from_name: sender.map(|sender| sender.name.clone()).unwrap_or_default(),
                from_based_in: sender.and_then(|sender| sender.based_in.clone()),
                message,
            }
        })
        .collect())
}

/// `POST /invite/{id}/decline`: the person asking declines an invite sent to them.
/// it stays, declined, so its sender can't send another until it's old enough
pub async fn decline(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let declined = update_pending_to_declined(&pool, user_id, id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "fail to decline invite");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to decline invite",
            )
        })?;
    if !declined {
        // none, not to them, already answered, or sent again since with a new id
        return Err((StatusCode::NOT_FOUND, "Invite not found"));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Whether there was a pending invite to decline. to_user_id makes it one shard's write, and only the recipient's
async fn update_pending_to_declined(
    pool: &Pool,
    to_user_id: Uuid,
    id: Uuid,
) -> mysql_async::Result<bool> {
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        r"-- sql
    UPDATE invite
    SET status = 'declined'
    WHERE to_user_id = :to_user_id AND id = :id AND status = 'pending'",
        params! {
            "to_user_id" => to_user_id.as_bytes().to_vec(),
            "id" => id.as_bytes().to_vec(),
        },
    )
    .await?;
    Ok(conn.affected_rows() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn me() -> Uuid {
        Uuid::from_u128(1)
    }

    fn them() -> Uuid {
        Uuid::from_u128(2)
    }

    fn valid() -> CreateInviteRequest {
        CreateInviteRequest {
            to_user_id: them(),
            message: "Hey! I'm visiting Tokyo next month".into(),
        }
    }

    #[test]
    fn accepts_an_invite() {
        let invite = valid().validate(me()).unwrap();
        assert_eq!(invite.to_user_id, them());
    }

    #[test]
    fn trims_the_message() {
        let request = CreateInviteRequest {
            message: "  Hi  ".into(),
            ..valid()
        };
        assert_eq!(request.validate(me()).unwrap().message, "Hi");
    }

    #[test]
    fn rejects_a_blank_message() {
        let request = CreateInviteRequest {
            message: " \n ".into(),
            ..valid()
        };
        assert!(request.validate(me()).is_err());
    }

    #[test]
    fn counts_characters_not_bytes() {
        let request = CreateInviteRequest {
            message: "가".repeat(MAX_MESSAGE),
            ..valid()
        };
        assert!(request.validate(me()).is_ok());
    }

    #[test]
    fn rejects_a_too_long_message() {
        let request = CreateInviteRequest {
            message: "a".repeat(MAX_MESSAGE + 1),
            ..valid()
        };
        assert!(request.validate(me()).is_err());
    }

    #[test]
    fn reads_the_user_id_as_a_uuid() {
        let request: CreateInviteRequest = serde_json::from_str(
            r#"{"toUserId":"00000000-0000-0000-0000-000000000002","message":"Hi"}"#,
        )
        .unwrap();
        assert_eq!(request.to_user_id, them());
    }

    #[test]
    fn rejects_an_invalid_user_id() {
        let request =
            serde_json::from_str::<CreateInviteRequest>(r#"{"toUserId":"mika","message":"Hi"}"#);
        assert!(request.is_err());
    }

    // 2026-11-11T09:30:15.123Z
    const SENT_MILLIS: u64 = 1_794_389_415_123;

    fn id_at(millis: u64) -> Uuid {
        Builder::from_unix_timestamp_millis(millis, &[0xff; 10]).into_uuid()
    }

    #[test]
    fn resends_only_ids_older_than_the_cutoff() {
        let resend_after_millis = u64::from(RESEND_AFTER_DAYS) * 24 * 60 * 60 * 1000;
        let now = SENT_MILLIS + resend_after_millis;
        let cutoff = resend_cutoff(now);
        // the largest id of the millisecond before is still below it
        assert!(id_at(SENT_MILLIS - 1) < cutoff);
        // the smallest id of the cutoff's millisecond isn't
        assert!(Builder::from_unix_timestamp_millis(SENT_MILLIS, &[0; 10]).into_uuid() >= cutoff);
        assert!(id_at(SENT_MILLIS) > cutoff);
    }

    #[test]
    fn rejects_inviting_yourself() {
        let request = CreateInviteRequest {
            to_user_id: me(),
            ..valid()
        };
        assert!(request.validate(me()).is_err());
    }
}
