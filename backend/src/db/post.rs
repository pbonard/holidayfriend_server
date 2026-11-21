use mysql_async::{Conn, Params, Transaction, Value, params, prelude::Queryable};
use serde::Deserialize;
use uuid::Uuid;

use crate::place::Place;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PostRole {
    Local,
    Traveler,
}

impl PostRole {
    fn as_str(self) -> &'static str {
        match self {
            PostRole::Local => "local",
            PostRole::Traveler => "traveler",
        }
    }
}

/// A post as it's saved, already checked and trimmed. every column by name, so the two ids or the title and body
/// can't be swapped
pub struct NewPost<'a> {
    pub id: Uuid,
    pub user_id: Uuid,
    pub role: PostRole,
    pub title: &'a str,
    pub body: &'a str,
}

/// The post alone, insert_places adds its cities in the same transaction. both are sharded by the post's id,
/// so it's one shard's transaction
pub async fn insert(tx: &mut Transaction<'_>, post: &NewPost<'_>) -> mysql_async::Result<()> {
    tx.exec_drop(
        r"-- sql
    INSERT INTO post (id, user_id, role, title, body)
    VALUES (:id, :user_id, :role, :title, :body)",
        params! {
            "id" => post.id.as_bytes().to_vec(),
            "user_id" => post.user_id.as_bytes().to_vec(),
            "role" => post.role.as_str(),
            "title" => post.title,
            "body" => post.body,
        },
    )
    .await
}

pub async fn insert_places(
    tx: &mut Transaction<'_>,
    post_id: Uuid,
    places: &[Place],
) -> mysql_async::Result<()> {
    tx.exec_batch(
        r"-- sql
    INSERT INTO post_place (post_id, country, city)
    VALUES (:post_id, :country, :city)",
        places.iter().map(|place| {
            params! {
                "post_id" => post_id.as_bytes().to_vec(),
                "country" => place.country.as_str(),
                "city" => place.city.as_str(),
            }
        }),
    )
    .await
}

/// Who wrote the post and as what, none when there's no such post. locks the row until the transaction ends, so
/// an edit and a delete of the same post can't cross: the second waits and then finds it changed or gone
pub async fn select_author_for_update(
    tx: &mut Transaction<'_>,
    id: Uuid,
) -> mysql_async::Result<Option<(Uuid, PostRole)>> {
    let row: Option<(Vec<u8>, String)> = tx
        .exec_first(
            r"-- sql
    SELECT user_id, role
    FROM post
    WHERE id = :id
    FOR UPDATE",
            params! {
                "id" => id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.map(|(user_id, role)| {
        (
            // BINARY(16), so always the 16 bytes of a uuid
            Uuid::from_slice(&user_id).unwrap_or_default(),
            // only ever written from PostRole
            if role == PostRole::Local.as_str() {
                PostRole::Local
            } else {
                PostRole::Traveler
            },
        )
    }))
}

/// The post's own columns, already checked and trimmed. never user_id: a post stays its author's, and the
/// post_user_id vindex is by it
pub async fn update(
    tx: &mut Transaction<'_>,
    id: Uuid,
    role: PostRole,
    title: &str,
    body: &str,
) -> mysql_async::Result<()> {
    tx.exec_drop(
        r"-- sql
    UPDATE post
    SET role = :role, title = :title, body = :body
    WHERE id = :id",
        params! {
            "id" => id.as_bytes().to_vec(),
            "role" => role.as_str(),
            title,
            body,
        },
    )
    .await
}

/// The post's cities that aren't in `places`, the first half of an edit's new cities: the ones it keeps aren't
/// touched, so an edit of only the words writes no city rows. by post_id, so vtgate sends it to the post's shard.
/// positional parameters, as the number of cities varies. `places` is never empty, the request's validation makes
/// sure: none would be `NOT IN ()`, which isn't sql
pub async fn delete_places_except(
    tx: &mut Transaction<'_>,
    post_id: Uuid,
    places: &[Place],
) -> mysql_async::Result<()> {
    tx.exec_drop(
        format!(
            r"-- sql
    DELETE FROM post_place
    WHERE post_id = ? AND (country, city) NOT IN ({})",
            // a (country, city) pair for each place
            vec!["(?, ?)"; places.len()].join(", ")
        ),
        // the post's id, then each place's country and city, in the order of the ?s
        Params::Positional(
            std::iter::once(Value::from(post_id.as_bytes().to_vec()))
                .chain(places.iter().flat_map(|place| {
                    [
                        Value::from(place.country.as_str()),
                        Value::from(place.city.as_str()),
                    ]
                }))
                .collect(),
        ),
    )
    .await
}

/// The cities of `places` the post doesn't have yet, the second half of an edit's new cities. a city it already has
/// is a duplicate of its primary key, and setting city to itself leaves it as it was. not INSERT IGNORE, which would
/// hide other errors too. not post_id: vitess won't update the sharding column. never empty, as delete_places_except
pub async fn insert_missing_places(
    tx: &mut Transaction<'_>,
    post_id: Uuid,
    places: &[Place],
) -> mysql_async::Result<()> {
    let post_id = Value::from(post_id.as_bytes().to_vec());
    tx.exec_drop(
        format!(
            r"-- sql
    INSERT INTO post_place (post_id, country, city)
    VALUES {}
    ON DUPLICATE KEY UPDATE city = city",
            // a (post_id, country, city) row for each place
            vec!["(?, ?, ?)"; places.len()].join(", ")
        ),
        // each place's post id, country and city, in the order of the ?s
        Params::Positional(
            places
                .iter()
                .flat_map(|place| {
                    [
                        post_id.clone(),
                        Value::from(place.country.as_str()),
                        Value::from(place.city.as_str()),
                    ]
                })
                .collect(),
        ),
    )
    .await
}

/// Every city of the post, before the post itself goes
pub async fn delete_places(tx: &mut Transaction<'_>, post_id: Uuid) -> mysql_async::Result<()> {
    tx.exec_drop(
        r"-- sql
    DELETE FROM post_place
    WHERE post_id = :post_id",
        params! {
            "post_id" => post_id.as_bytes().to_vec(),
        },
    )
    .await
}

/// The post itself, only when user_id wrote it. whether there was one, none when there's no such post or it's
/// someone else's. vtgate deletes its post_user_id_lookup row along with it
pub async fn delete(
    tx: &mut Transaction<'_>,
    id: Uuid,
    user_id: Uuid,
) -> mysql_async::Result<bool> {
    tx.exec_drop(
        r"-- sql
    DELETE FROM post
    WHERE id = :id AND user_id = :user_id",
        params! {
            "id" => id.as_bytes().to_vec(),
            "user_id" => user_id.as_bytes().to_vec(),
        },
    )
    .await?;
    Ok(tx.affected_rows() > 0)
}

/// A post with every one of its cities
pub struct PostWithPlaces {
    pub id: Uuid,
    /// the author
    pub user_id: Uuid,
    pub role: String,
    pub title: String,
    /// the whole body, or for a feed its first body_preview characters when it's longer
    pub body: String,
    /// every city of the post, the one asked for too
    pub places: Vec<Place>,
}

/// id, user_id, role, title, body, and every city of the post as a json array of places
type PostRow = (Vec<u8>, Vec<u8>, String, String, String, String);

/// A city's posts, newest first, `limit` of them older than `before`.
/// a post's places are on its own shard, so the joins and the grouping by post id all stay in each shard.
/// a city's posts are on every shard, vtgate asks them all and merges by id, which is the newest first since
/// post ids are uuid v7.
pub async fn select_city_posts(
    conn: &mut Conn,
    country: &str,
    city: &str,
    before: Option<Uuid>,
    limit: usize,
    body_preview: usize,
) -> mysql_async::Result<Vec<PostWithPlaces>> {
    let rows: Vec<PostRow> = conn
        .exec(
            format!(
                r"-- sql
    SELECT
        post.id,
        post.user_id,
        post.role,
        post.title,
        LEFT(post.body, {body_preview}),
        JSON_ARRAYAGG(JSON_OBJECT('country', place.country, 'city', place.city))
    FROM post_place AS here
    JOIN post ON post.id = here.post_id
    -- every city of the post, not only the one asked for
    JOIN post_place AS place ON place.post_id = post.id
    WHERE here.country = :country AND here.city = :city
        AND (:before IS NULL OR post.id < :before)
    GROUP BY post.id
    ORDER BY post.id DESC
    LIMIT {limit}"
            ),
            params! {
                "country" => country,
                "city" => city,
                "before" => before.map(|before| before.as_bytes().to_vec()),
            },
        )
        .await?;
    Ok(rows.into_iter().map(post_with_places).collect())
}

/// A person's posts, newest first, `limit` of them older than `before`, each with every one of its cities.
/// the post_user_id lookup vindex sends it only to the shards that hold their posts, and KEY (user_id) (with the
/// primary key innodb appends to it) finds them there in id order. vtgate merges the shards by id, the newest first
/// since post ids are uuid v7
pub async fn select_user_posts(
    conn: &mut Conn,
    user_id: Uuid,
    before: Option<Uuid>,
    limit: usize,
    body_preview: usize,
) -> mysql_async::Result<Vec<PostWithPlaces>> {
    let rows: Vec<PostRow> = conn
        .exec(
            format!(
                r"-- sql
    SELECT
        post.id,
        post.user_id,
        post.role,
        post.title,
        LEFT(post.body, {body_preview}),
        JSON_ARRAYAGG(JSON_OBJECT('country', place.country, 'city', place.city))
    FROM post
    JOIN post_place AS place ON place.post_id = post.id
    WHERE post.user_id = :user_id
        AND (:before IS NULL OR post.id < :before)
    GROUP BY post.id
    ORDER BY post.id DESC
    LIMIT {limit}"
            ),
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
                "before" => before.map(|before| before.as_bytes().to_vec()),
            },
        )
        .await?;
    Ok(rows.into_iter().map(post_with_places).collect())
}

/// The post of id with its whole body, none when there's none. both are sharded by the post's id, so it's one
/// shard's query
pub async fn select_post(conn: &mut Conn, id: Uuid) -> mysql_async::Result<Option<PostWithPlaces>> {
    let row: Option<PostRow> = conn
        .exec_first(
            r"-- sql
    SELECT
        post.id,
        post.user_id,
        post.role,
        post.title,
        post.body,
        JSON_ARRAYAGG(JSON_OBJECT('country', place.country, 'city', place.city))
    FROM post
    JOIN post_place AS place ON place.post_id = post.id
    WHERE post.id = :id
    GROUP BY post.id",
            params! {
                "id" => id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.map(post_with_places))
}

fn post_with_places((id, user_id, role, title, body, places): PostRow) -> PostWithPlaces {
    PostWithPlaces {
        // BINARY(16), so always the 16 bytes of a uuid
        id: Uuid::from_slice(&id).unwrap_or_default(),
        user_id: Uuid::from_slice(&user_id).unwrap_or_default(),
        role,
        title,
        body,
        places: parse_places(&places),
    }
}

/// The json array JSON_ARRAYAGG built. mysql made it, so it's always this shape: a post shows
/// without its cities rather than failing the whole page if it ever isn't
fn parse_places(json: &str) -> Vec<Place> {
    serde_json::from_str(json).unwrap_or_else(|err| {
        tracing::warn!(error = %err, "fail to read a post's places");
        Vec::new()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_places_json_arrayagg_builds() {
        let places = parse_places(
            r#"[{"city": "Tokyo", "country": "JP"}, {"city": "Kyoto", "country": "JP"}]"#,
        );
        assert_eq!(
            places,
            vec![
                Place {
                    country: "JP".into(),
                    city: "Tokyo".into()
                },
                Place {
                    country: "JP".into(),
                    city: "Kyoto".into()
                }
            ]
        );
    }

    #[test]
    fn reads_broken_places_as_none() {
        assert!(parse_places("not json").is_empty());
    }
}
