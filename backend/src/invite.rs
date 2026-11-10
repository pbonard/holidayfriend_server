use axum::{Json, extract::State, http::StatusCode};
use mysql_async::{Conn, Pool, params, prelude::Queryable};
use serde::Deserialize;
use uuid::Uuid;

use crate::{profile, user::UserId};

const MAX_MESSAGE: usize = 300;
// ER_DUP_ENTRY, vtgate passes the shard's error code through
const DUPLICATE_ENTRY: u16 = 1062;

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

/// `POST /invite`. one invite per pair of people: inviting again while one is waiting is a conflict.
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

    match insert(&mut conn, from_user_id, &invite).await {
        Ok(()) => Ok(StatusCode::CREATED),
        Err(mysql_async::Error::Server(err)) if err.code == DUPLICATE_ENTRY => {
            Err((StatusCode::CONFLICT, "You already invited them"))
        }
        Err(err) => Err(failed(err)),
    }
}

fn failed(err: mysql_async::Error) -> (StatusCode, &'static str) {
    tracing::error!(error = %err, "fail to invite");
    (StatusCode::INTERNAL_SERVER_ERROR, "Failed to invite")
}

/// Sharded by to_user_id, so the primary key (to_user_id, from_user_id) keeps a pair to one invitation within a shard
async fn insert(
    conn: &mut Conn,
    from_user_id: Uuid,
    invite: &CreateInviteRequest,
) -> mysql_async::Result<()> {
    conn.exec_drop(
        r"-- sql
    INSERT INTO invitation (to_user_id, from_user_id, message)
    VALUES (:to_user_id, :from_user_id, :message)",
        params! {
            "to_user_id" => invite.to_user_id.as_bytes().to_vec(),
            "from_user_id" => from_user_id.as_bytes().to_vec(),
            "message" => invite.message.as_str(),
        },
    )
    .await
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

    #[test]
    fn rejects_inviting_yourself() {
        let request = CreateInviteRequest {
            to_user_id: me(),
            ..valid()
        };
        assert!(request.validate(me()).is_err());
    }
}
