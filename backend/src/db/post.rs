use mysql_async::{Conn, Transaction, params, prelude::Queryable};
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

/// A post as it's saved, already checked and trimmed
pub struct NewPost<'a> {
    pub role: PostRole,
    pub title: &'a str,
    pub body: &'a str,
}

/// The post alone, insert_places adds its cities in the same transaction. both are sharded by the post's id,
/// so it's one shard's transaction
pub async fn insert(
    tx: &mut Transaction<'_>,
    post_id: Uuid,
    user_id: Uuid,
    post: &NewPost<'_>,
) -> mysql_async::Result<()> {
    tx.exec_drop(
        r"-- sql
    INSERT INTO post (id, user_id, role, title, body)
    VALUES (:post_id, :user_id, :role, :title, :body)",
        params! {
            "post_id" => post_id.as_bytes().to_vec(),
            "user_id" => user_id.as_bytes().to_vec(),
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

/// A post of a city's feed
pub struct CityPost {
    pub id: Uuid,
    /// the bytes, as select_brief_profiles keys its authors
    pub user_id: Vec<u8>,
    pub role: String,
    pub title: String,
    /// the whole body, or its first body_preview characters when it's longer
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
) -> mysql_async::Result<Vec<CityPost>> {
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
    Ok(rows
        .into_iter()
        .map(|(id, user_id, role, title, body, places)| CityPost {
            // BINARY(16), so always the 16 bytes of a uuid
            id: Uuid::from_slice(&id).unwrap_or_default(),
            user_id,
            role,
            title,
            body,
            places: parse_places(&places),
        })
        .collect())
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
