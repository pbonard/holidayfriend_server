use mysql_async::{Transaction, params, prelude::Queryable};
use uuid::Uuid;

use crate::kafka;

const AUTH_TOPIC: &str = "auth";

/// Sends the payload to the kafka topic once the transaction commits, and never if it rolls back: the cdc-job of
/// my-server streams the rows inserted into outbox to their topic. the row is deleted in the same transaction,
/// the cdc-job reads the insert from the binlog and skips the delete, so the table stays empty.
/// to_user_id is the shard key: the row is on the shard of what the transaction changes, like the invite
pub async fn publish(
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

pub async fn publish_ban(tx: &mut Transaction<'_>, member_id: Uuid) -> mysql_async::Result<()> {
    publish(
        tx,
        member_id,
        member_id,
        AUTH_TOPIC,
        Some(kafka::BAN_TASK),
        "",
    )
    .await
}
