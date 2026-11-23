use mysql_async::{Conn, Transaction, params, prelude::Queryable};
use uuid::Uuid;

// ER_DUP_ENTRY, vtgate passes the shard's error code through
const DUPLICATE_ENTRY: u16 = 1062;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteStatus {
    Accepted,
    Declined,
    Reported,
}

impl InviteStatus {
    fn as_str(self) -> &'static str {
        match self {
            InviteStatus::Accepted => "accepted",
            InviteStatus::Declined => "declined",
            InviteStatus::Reported => "reported",
        }
    }
}

/// Sends an earlier invite again with the new message, as if it were new: pending, with the new id so it's on top
/// of the requests again and its age starts over. only one sent before the cutoff, declined, left unanswered or
/// accepted: an accepted one only gets here when the sender left their chat, which chat told send. whether there
/// was one, none when there's no invite or it can't be sent again yet. the age is checked in the same statement that changes it, so two sends at once can't both send
/// it again: the second finds it sent just now
pub async fn resend(
    conn: &mut Conn,
    id: Uuid,
    cutoff: Uuid,
    to_user_id: Uuid,
    from_user_id: Uuid,
    message: &str,
) -> mysql_async::Result<bool> {
    conn.exec_drop(
        r"-- sql
    UPDATE invite
    SET id = :id, message = :message, status = 'pending'
    WHERE to_user_id = :to_user_id AND from_user_id = :from_user_id
        -- sent before the cutoff: a v7 id starts with when it was made, so ids compare as their times do
        AND id < :cutoff AND status <> 'reported'",
        params! {
            "id" => id.as_bytes().to_vec(),
            "cutoff" => cutoff.as_bytes().to_vec(),
            "to_user_id" => to_user_id.as_bytes().to_vec(),
            "from_user_id" => from_user_id.as_bytes().to_vec(),
            message,
        },
    )
    .await?;
    Ok(conn.affected_rows() > 0)
}

/// Whether it was inserted: not when the pair has one already. sharded by to_user_id, so the primary key
/// (to_user_id, from_user_id) keeps a pair to one invite within a shard
pub async fn insert(
    conn: &mut Conn,
    id: Uuid,
    to_user_id: Uuid,
    from_user_id: Uuid,
    message: &str,
) -> mysql_async::Result<bool> {
    let inserted = conn
        .exec_drop(
            r"-- sql
    INSERT INTO invite (id, to_user_id, from_user_id, message)
    VALUES (:id, :to_user_id, :from_user_id, :message)",
            params! {
                "id" => id.as_bytes().to_vec(),
                "to_user_id" => to_user_id.as_bytes().to_vec(),
                "from_user_id" => from_user_id.as_bytes().to_vec(),
                message,
            },
        )
        .await;
    match inserted {
        Ok(()) => Ok(true),
        Err(mysql_async::Error::Server(err)) if err.code == DUPLICATE_ENTRY => Ok(false),
        Err(err) => Err(err),
    }
}

/// A pending invite sent to the person
pub struct ReceivedInvite {
    pub id: Uuid,
    pub from_user_id: Uuid,
    pub message: String,
}

/// id, from_user_id, message
type InviteRow = (Vec<u8>, Vec<u8>, String);

/// The pending invites sent to the person, newest first, `limit` of them older than `before`
pub async fn select_pending_received(
    conn: &mut Conn,
    user_id: Uuid,
    before: Option<Uuid>,
    limit: usize,
) -> mysql_async::Result<Vec<ReceivedInvite>> {
    let rows: Vec<InviteRow> = conn
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
    LIMIT {limit}"
            ),
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
                "before" => before.map(|before| before.as_bytes().to_vec()),
            },
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, from_user_id, message)| ReceivedInvite {
            // BINARY(16), so always the 16 bytes of a uuid
            id: Uuid::from_slice(&id).unwrap_or_default(),
            from_user_id: Uuid::from_slice(&from_user_id).unwrap_or_default(),
            message,
        })
        .collect())
}

/// Whether there was a pending invite to decline. to_user_id makes it one shard's write, and only the recipient's
/// Whether there was a pending invite to accept. it locks the row until the transaction ends,
/// so of two accepts at once only one finds it pending
pub async fn update_pending_to(
    queryable: &mut impl Queryable,
    to_user_id: Uuid,
    id: Uuid,
    status: InviteStatus,
) -> mysql_async::Result<bool> {
    let result = queryable
        .exec_iter(
            r"-- sql
    UPDATE invite
    SET status = :status
    WHERE to_user_id = :to_user_id AND id = :id AND status = 'pending'",
            params! {
                "to_user_id" => to_user_id.as_bytes().to_vec(),
                "id" => id.as_bytes().to_vec(),
                "status" => status.as_str(),
            },
        )
        .await?;
    let updated = result.affected_rows() > 0;
    result.drop_result().await?;
    Ok(updated)
}

/// Who sent the invite and its message
pub async fn select_sender_and_message(
    tx: &mut Transaction<'_>,
    to_user_id: Uuid,
    id: Uuid,
) -> mysql_async::Result<Option<(Uuid, String)>> {
    let row: Option<(Vec<u8>, String)> = tx
        .exec_first(
            r"-- sql
    SELECT from_user_id, message
    FROM invite
    WHERE to_user_id = :to_user_id AND id = :id",
            params! {
                "to_user_id" => to_user_id.as_bytes().to_vec(),
                "id" => id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.map(|(from_user_id, message)| {
        (Uuid::from_slice(&from_user_id).unwrap_or_default(), message)
    }))
}

pub async fn select_report_target(
    conn: &mut Conn,
    to_user_id: Uuid,
    id: Uuid,
) -> mysql_async::Result<Option<(Uuid, String)>> {
    let row: Option<(Vec<u8>, String)> = conn
        .exec_first(
            r"-- sql
    SELECT from_user_id, message
    FROM invite
    WHERE to_user_id = :to_user_id AND id = :id",
            params! {
                "to_user_id" => to_user_id.as_bytes().to_vec(),
                "id" => id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.map(|(from_user_id, message)| {
        (Uuid::from_slice(&from_user_id).unwrap_or_default(), message)
    }))
}
