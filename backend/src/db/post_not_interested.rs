use mysql_async::{Conn, params, prelude::Queryable};
use uuid::Uuid;

// ER_DUP_ENTRY, vtgate passes the shard's error code through
const DUPLICATE_ENTRY: u16 = 1062;

/// Hides the post from the person. marking it again is a no-op, so a retried
/// request or a second tap succeeds too. sharded by user_id, so it's one shard's write
pub async fn insert(conn: &mut Conn, user_id: Uuid, post_id: Uuid) -> mysql_async::Result<()> {
    let inserted = conn
        .exec_drop(
            r"-- sql
    INSERT INTO post_not_interested (user_id, post_id)
    VALUES (:user_id, :post_id)",
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
                "post_id" => post_id.as_bytes().to_vec(),
            },
        )
        .await;
    match inserted {
        Err(mysql_async::Error::Server(err)) if err.code == DUPLICATE_ENTRY => Ok(()),
        result => result,
    }
}

/// Every post the person hid. the primary key starts with user_id, so it's one shard's read of one range
pub async fn select_post_ids(conn: &mut Conn, user_id: Uuid) -> mysql_async::Result<Vec<Uuid>> {
    let rows: Vec<Vec<u8>> = conn
        .exec(
            r"-- sql
    SELECT post_id
    FROM post_not_interested
    WHERE user_id = :user_id",
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(rows
        .into_iter()
        // BINARY(16), so always the 16 bytes of a uuid
        .map(|post_id| Uuid::from_slice(&post_id).unwrap_or_default())
        .collect())
}
