use mysql_async::{Conn, Transaction, params, prelude::Queryable};
use uuid::Uuid;

// ER_DUP_ENTRY, vtgate passes the shard's error code through
const DUPLICATE_ENTRY: u16 = 1062;

/// Sends an earlier invite again with the new message, as if it were new: pending, with the new id so it's on top
/// of the requests again and its age starts over. only one that isn't accepted and was sent before the cutoff,
/// whether it was declined or left unanswered. whether there was one, none when there's no invite or it can't be
/// sent again yet. the age is checked in the same statement that changes it, so two sends at once can't both send
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
        AND status <> 'accepted'
        -- sent before the cutoff: a v7 id starts with when it was made, so ids compare as their times do
        AND id < :cutoff",
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
    /// the bytes, as select_brief_profiles keys its senders
    pub from_user_id: Vec<u8>,
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
            id: Uuid::from_slice(&id).unwrap_or_default(),
            from_user_id,
            message,
        })
        .collect())
}

/// Whether there was a pending invite to decline. to_user_id makes it one shard's write, and only the recipient's
pub async fn update_pending_to_declined(
    conn: &mut Conn,
    to_user_id: Uuid,
    id: Uuid,
) -> mysql_async::Result<bool> {
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

/// Whether there was a pending invite to accept. it locks the row until the transaction ends,
/// so of two accepts at once only one finds it pending
pub async fn update_pending_to_accepted(
    tx: &mut Transaction<'_>,
    to_user_id: Uuid,
    id: Uuid,
) -> mysql_async::Result<bool> {
    tx.exec_drop(
        r"-- sql
    UPDATE invite
    SET status = 'accepted'
    WHERE to_user_id = :to_user_id AND id = :id AND status = 'pending'",
        params! {
            "to_user_id" => to_user_id.as_bytes().to_vec(),
            "id" => id.as_bytes().to_vec(),
        },
    )
    .await?;
    Ok(tx.affected_rows() > 0)
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

/// Sends the payload to the kafka topic once the transaction commits, and never if it rolls back: the cdc-job of
/// my-server streams the rows inserted into outbox to their topic. the row is deleted in the same transaction,
/// the cdc-job reads the insert from the binlog and skips the delete, so the table stays empty.
/// to_user_id is the shard key: the row is on the shard of what the transaction changes, like the invite
pub async fn outbox_publish(
    tx: &mut Transaction<'_>,
    id: Uuid,
    to_user_id: Uuid,
    topic: &str,
    task_type: Option<&str>,
    payload: &str,
) -> mysql_async::Result<()> {
    let id = id.as_bytes().to_vec();
    let to_user_id = to_user_id.as_bytes().to_vec();
    tx.exec_drop(
        r"-- sql
    INSERT INTO outbox (id, to_user_id, topic, task_type, payload)
    VALUES (:id, :to_user_id, :topic, :task_type, :payload)",
        params! {
            "id" => &id,
            "to_user_id" => &to_user_id,
            topic,
            task_type,
            payload,
        },
    )
    .await?;
    tx.exec_drop(
        r"-- sql
    -- id is the primary key. to_user_id is for vtgate: it routes by it, without it the delete goes to every shard
    -- and the transaction is no longer one shard's
    DELETE FROM outbox WHERE to_user_id = :to_user_id AND id = :id",
        params! {
            "id" => &id,
            "to_user_id" => &to_user_id,
        },
    )
    .await
}
