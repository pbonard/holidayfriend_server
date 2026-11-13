use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use mysql_async::{Pool, TxOpts};
use serde::{Deserialize, Serialize};
use uuid::{Builder, Uuid};

use crate::{
    db::{
        invite,
        profile::{self, ProfileBasedIn},
    },
    grpc_client::{GrpcClient, InviteCheck},
    user::UserId,
};

const MAX_MESSAGE: usize = 300;
const ALREADY_INVITED: &str = "You already sent this person an invite";
const ALREADY_CHATTING: &str = "You're already chatting with this person";
// how old an invite has to be before it can be sent again
const RESEND_AFTER_DAYS: u32 = 120;
// invites per page of the requests screen. a page shorter than this is the last one, the web app stops there
const LIST_PAGE_SIZE: usize = 20;
// the topic of the ChatMessages of my-server's chat, the cdc-job sends to holidayfriend.chat-message
const CHAT_MESSAGE_TOPIC: &str = "chat-message";

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

/// What sending an invite does, from what chat knows about the pair
#[derive(Debug, PartialEq, Eq)]
enum InviteDecision {
    /// the invitee blocked the sender: nothing is sent, but the sender gets the answer of a sent one, as with
    /// a message to who blocked them, so they can't tell
    Hide,
    /// the sender still has their chat: they write there, the message reaches the invitee even when the invitee left
    AlreadyChatting,
    Send,
}

/// A sender who still has the chat writes there instead, the invite is the way back for one who left it. only the
/// sender's chat decides: the answer then never shows the sender whether the invitee left
fn decide(check: InviteCheck) -> InviteDecision {
    if check.blocked {
        InviteDecision::Hide
    } else if check.sender_has_chat {
        InviteDecision::AlreadyChatting
    } else {
        InviteDecision::Send
    }
}

/// `POST /invite`. one invite per pair of people: inviting the same person again is a conflict, unless the invite
/// is at least RESEND_AFTER_DAYS old, accepted, declined or not. then it's sent again in its place. chat decides
/// first: see decide.
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn send(
    UserId(from_user_id): UserId,
    State(pool): State<Pool>,
    State(grpc_client): State<GrpcClient>,
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
    let check = grpc_client
        .check_invite(from_user_id, invite.to_user_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "fail to check invite with chat");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to invite")
        })?;
    match decide(check) {
        InviteDecision::Hide => return Ok(StatusCode::CREATED),
        InviteDecision::AlreadyChatting => return Err((StatusCode::CONFLICT, ALREADY_CHATTING)),
        InviteDecision::Send => {}
    }

    // v7 is time ordered, so the requests are newest first by id
    let id = Uuid::now_v7();
    // an old enough one is sent again in its place, it's one row per pair
    if invite::resend(
        &mut conn,
        id,
        resend_cutoff(now_millis()),
        invite.to_user_id,
        from_user_id,
        &invite.message,
    )
    .await
    .map_err(failed)?
    {
        return Ok(StatusCode::CREATED);
    }
    if invite::insert(
        &mut conn,
        id,
        invite.to_user_id,
        from_user_id,
        &invite.message,
    )
    .await
    .map_err(failed)?
    {
        return Ok(StatusCode::CREATED);
    }
    // there's one already, too recent to send again
    Err((StatusCode::CONFLICT, ALREADY_INVITED))
}

fn failed(err: mysql_async::Error) -> (StatusCode, &'static str) {
    tracing::error!(error = %err, "fail to invite");
    (StatusCode::INTERNAL_SERVER_ERROR, "Failed to invite")
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

/// `GET /invite/list`: the pending invites sent to the person asking, newest first, a page at a time.
/// two round trips, the invites and then their senders, as profiles are on the shards of their own ids
pub async fn list(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Query(query): Query<ListInvitesQuery>,
) -> Result<Json<Vec<InviteListItem>>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to list invites");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load invites")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let invites = invite::select_pending_received(&mut conn, user_id, query.before, LIST_PAGE_SIZE)
        .await
        .map_err(failed)?;
    if invites.is_empty() {
        return Ok(Json(Vec::new()));
    }

    let senders = profile::select_brief_profiles(
        &mut conn,
        invites.iter().map(|invite| invite.from_user_id).collect(),
    )
    .await
    .map_err(failed)?;

    Ok(Json(
        invites
            .into_iter()
            .map(|invite| {
                let sender = senders.get(&invite.from_user_id);
                InviteListItem {
                    id: invite.id,
                    from_user_id: invite.from_user_id,
                    from_name: sender.map(|sender| sender.name.clone()).unwrap_or_default(),
                    from_based_in: sender.and_then(|sender| sender.based_in.clone()),
                    message: invite.message,
                }
            })
            .collect(),
    ))
}

/// `POST /invite/{id}/decline`: the person asking declines an invite sent to them.
/// it stays, declined, so its sender can't send another until it's old enough
pub async fn decline(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to decline invite");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to decline invite",
        )
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let declined = invite::update_pending_to_declined(&mut conn, user_id, id)
        .await
        .map_err(failed)?;
    if !declined {
        // none, not to them, already answered, or sent again since with a new id
        return Err((StatusCode::NOT_FOUND, "Invite not found"));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /invite/{id}/accept`: the person asking accepts an invite sent to them, and its message becomes the first
/// message of their chat with its sender. the chat gets it through the outbox in the transaction that accepts it,
/// so it's sent exactly when the invite is accepted: not when accepting fails, and not twice when it's accepted again.
/// the invite and its outbox row are on the shard of to_user_id, so it's one shard's transaction
pub async fn accept(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to accept invite");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to accept invite")
    };
    // none, not to them, already answered, or sent again since with a new id
    let not_found = (StatusCode::NOT_FOUND, "Invite not found");

    let mut conn = pool.get_conn().await.map_err(failed)?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    // the update first: it locks the row, so what's read next is the invite this accepted
    if !invite::update_pending_to_accepted(&mut tx, user_id, id)
        .await
        .map_err(failed)?
    {
        tx.rollback().await.map_err(failed)?;
        return Err(not_found);
    }
    let Some((from_user_id, message)) = invite::select_sender_and_message(&mut tx, user_id, id)
        .await
        .map_err(failed)?
    else {
        tx.rollback().await.map_err(failed)?;
        return Err(not_found);
    };

    // the invite's message as a message from its sender to the person who accepted it, in their personal chat.
    // no push, the holidayfriend web app has none
    let payload = serde_json::to_string(&ChatMessage {
        id,
        from_id: from_user_id,
        to_id_type: "personal",
        to_id: user_id,
        content_type: "text",
        contents: vec![message],
        no_push: true,
    })
    // a struct of strings and bools always serializes
    .unwrap_or_default();
    // the invite id is the message's id too, it's the kafka key and the id the chat keeps it by
    invite::outbox_publish(&mut tx, id, user_id, CHAT_MESSAGE_TOPIC, None, &payload)
        .await
        .map_err(failed)?;
    tx.commit().await.map_err(failed)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `ChatMessage` of my-server's backend/common/payload.go. the ids are uuid strings, as serde writes a Uuid
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ChatMessage {
    id: Uuid,
    from_id: Uuid,
    to_id_type: &'static str,
    to_id: Uuid,
    content_type: &'static str,
    contents: Vec<String>,
    no_push: bool,
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

    fn check(blocked: bool, sender_has_chat: bool) -> InviteCheck {
        InviteCheck {
            blocked,
            sender_has_chat,
        }
    }

    #[test]
    fn hides_an_invite_to_who_blocked_the_sender() {
        assert_eq!(decide(check(true, false)), InviteDecision::Hide);
        assert_eq!(decide(check(true, true)), InviteDecision::Hide);
    }

    #[test]
    fn skips_an_invite_while_the_sender_has_the_chat() {
        assert_eq!(decide(check(false, true)), InviteDecision::AlreadyChatting);
    }

    #[test]
    fn sends_when_the_sender_left_the_chat() {
        assert_eq!(decide(check(false, false)), InviteDecision::Send);
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
    fn writes_the_chat_message_as_go_reads_it() {
        let payload = serde_json::to_value(ChatMessage {
            id: Uuid::from_u128(3),
            from_id: them(),
            to_id_type: "personal",
            to_id: me(),
            content_type: "text",
            contents: vec!["Hi".into()],
            no_push: true,
        })
        .unwrap();
        assert_eq!(
            payload,
            serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000003",
                "fromId": "00000000-0000-0000-0000-000000000002",
                "toIdType": "personal",
                "toId": "00000000-0000-0000-0000-000000000001",
                "contentType": "text",
                "contents": ["Hi"],
                "noPush": true,
            })
        );
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
