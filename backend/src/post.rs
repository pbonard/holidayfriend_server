use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use mysql_async::{Conn, Pool, TxOpts, params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cities,
    place::Place,
    profile::{self, AuthorBasedIn},
    user::UserId,
};

// reddit's limits, the same as the maxLength of the web app's post form
const MAX_TITLE: usize = 300;
const MAX_BODY: usize = 40_000;
const MAX_PLACES: usize = 10;
// posts per page of a city's feed. a page shorter than this is the last one, the web app stops there
const FEED_PAGE_SIZE: usize = 5;
// the start of a post's body in the feed: more than a card's lines (the web app cuts it to them and adds …), the
// whole body (up to MAX_BODY) is for the post's own page. characters and not bytes, so a multi-byte letter is never
// cut in half
const FEED_BODY_PREVIEW: usize = 650;

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

/// `CreatePostRequest` in the web app.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePostRequest {
    role: PostRole,
    /// where a traveler is going, often a few cities near each other
    #[serde(default)]
    places: Vec<Place>,
    title: String,
    body: String,
}

/// The request once it's valid: trimmed, and without what a local's post doesn't have.
#[derive(Debug, PartialEq, Eq)]
struct NewPost {
    role: PostRole,
    /// the cities a traveler picked. empty for a local, whose city is the one their profile is based in
    places: Vec<Place>,
    title: String,
    body: String,
}

impl CreatePostRequest {
    fn validate(self) -> Result<NewPost, &'static str> {
        let title = self.title.trim();
        if title.is_empty() || title.chars().count() > MAX_TITLE {
            return Err("Enter a title of up to 300 characters");
        }
        if title.contains(['\n', '\r']) {
            return Err("A title has to be one line");
        }
        let body = self.body.trim();
        if body.is_empty() || body.chars().count() > MAX_BODY {
            return Err("Enter a body of up to 40,000 characters");
        }

        // the web form keeps a traveler's places when switching to local, a local's city is the one their profile is based in
        let places = match self.role {
            PostRole::Local => Vec::new(),
            PostRole::Traveler => validate_places(self.places)?,
        };

        Ok(NewPost {
            role: self.role,
            places,
            title: title.to_owned(),
            body: body.to_owned(),
        })
    }
}

fn validate_places(places: Vec<Place>) -> Result<Vec<Place>, &'static str> {
    if places.is_empty() {
        return Err("Add where you travel");
    }
    if places.len() > MAX_PLACES {
        return Err("Add up to 10 places");
    }
    let mut valid: Vec<Place> = Vec::with_capacity(places.len());
    for place in places {
        let country = place.country.trim();
        let city = place.city.trim();
        if !cities::is_city(country, city) {
            return Err("A place is invalid");
        }
        let place = Place {
            country: country.to_owned(),
            city: city.to_owned(),
        };
        // the same place again adds nothing, so it's dropped instead of failing the post
        if valid.contains(&place) {
            continue;
        }
        valid.push(place);
    }
    Ok(valid)
}

#[derive(Debug, Serialize)]
pub struct CreatePostResponse {
    id: String,
}

/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn create(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<CreatePostRequest>,
) -> Result<(StatusCode, Json<CreatePostResponse>), (StatusCode, &'static str)> {
    let mut post = request
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let mut conn = pool.get_conn().await.map_err(failed)?;
    // every post has a city, so a city's posts are both its travelers' and its locals'.
    // a local's is copied, so their posts keep the city they were written in when they move
    if post.role == PostRole::Local {
        let based_in = profile::select_based_in(&mut conn, user_id)
            .await
            .map_err(failed)?
            // onboarding saves the profile before anything else can be done
            .ok_or((StatusCode::BAD_REQUEST, "Set up your profile first"))?;
        post.places = vec![based_in];
    }

    // v7 is time ordered, so new posts land at the end of the primary key instead of all over it
    let post_id = Uuid::now_v7();
    insert(&mut conn, post_id, user_id, &post)
        .await
        .map_err(failed)?;

    Ok((
        StatusCode::CREATED,
        Json(CreatePostResponse {
            id: post_id.to_string(),
        }),
    ))
}

fn failed(err: mysql_async::Error) -> (StatusCode, &'static str) {
    tracing::error!(error = %err, "fail to create post");
    (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create post")
}

/// The post and its places together or not at all. both are sharded by the post's id, so it's one shard's transaction.
async fn insert(
    conn: &mut Conn,
    post_id: Uuid,
    user_id: Uuid,
    post: &NewPost,
) -> mysql_async::Result<()> {
    let mut tx = conn.start_transaction(TxOpts::default()).await?;

    tx.exec_drop(
        r"-- sql
    INSERT INTO post (id, user_id, role, title, body)
    VALUES (:post_id, :user_id, :role, :title, :body)",
        params! {
            "post_id" => post_id.as_bytes().to_vec(),
            "user_id" => user_id.as_bytes().to_vec(),
            "role" => post.role.as_str(),
            "title" => post.title.as_str(),
            "body" => post.body.as_str(),
        },
    )
    .await?;

    tx.exec_batch(
        r"-- sql
    INSERT INTO post_place (post_id, country, city)
    VALUES (:post_id, :country, :city)",
        post.places.iter().map(|place| {
            params! {
                "post_id" => post_id.as_bytes().to_vec(),
                "country" => place.country.as_str(),
                "city" => place.city.as_str(),
            }
        }),
    )
    .await?;

    tx.commit().await
}

/// `GET /posts`: a city's posts, newest first, locals' and travelers' both. the web app's tabs filter them itself
#[derive(Debug, Deserialize)]
pub struct ListPostsQuery {
    /// ISO 3166-1 alpha-2
    country: String,
    city: String,
    /// the last post id of the page before, the posts older than it come next. the first page leaves it out.
    /// a post id and not a page number, so posts written while scrolling don't shift the pages
    before: Option<String>,
}

/// `PostFeedItem` in the web app.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostFeedItem {
    id: String,
    role: String,
    title: String,
    /// the whole body, or its first FEED_BODY_PREVIEW characters when it's longer
    body: String,
    author_name: String,
    /// where the author's profile is based in, as much as they show: for the web app to tell a viewer
    /// from the same city or country. none when they show neither, or the profile is gone
    author_based_in: Option<AuthorBasedIn>,
    /// every city of the post, the one being explored too. a local's post has just theirs
    places: Vec<Place>,
}

pub async fn list(
    _: UserId,
    State(pool): State<Pool>,
    Query(query): Query<ListPostsQuery>,
) -> Result<Json<Vec<PostFeedItem>>, (StatusCode, &'static str)> {
    let country = query.country.trim();
    let city = query.city.trim();
    if !cities::is_city(country, city) {
        return Err((StatusCode::BAD_REQUEST, "The city is invalid"));
    }
    let before = query
        .before
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| (StatusCode::BAD_REQUEST, "The page is invalid"))?;
    select_city_posts(&pool, country, city, before)
        .await
        .map(Json)
        .map_err(|err| {
            tracing::error!(error = %err, "fail to list posts");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load posts")
        })
}

/// id, user_id, role, title, body, and every city of the post as a json array of places
type PostRow = (Vec<u8>, Vec<u8>, String, String, String, String);

/// Two round trips: the posts with their cities, then their authors.
/// a post's places are on its own shard, so the joins and the grouping by post id all stay in each shard.
/// a city's posts are on every shard, vtgate asks them all and merges by id, which is the newest first since
/// post ids are uuid v7.
async fn select_city_posts(
    pool: &Pool,
    country: &str,
    city: &str,
    before: Option<Uuid>,
) -> mysql_async::Result<Vec<PostFeedItem>> {
    let mut conn = pool.get_conn().await?;
    let posts: Vec<PostRow> = conn
        .exec(
            format!(
                r"-- sql
    SELECT
        post.id,
        post.user_id,
        post.role,
        post.title,
        LEFT(post.body, {FEED_BODY_PREVIEW}),
        JSON_ARRAYAGG(JSON_OBJECT('country', place.country, 'city', place.city))
    FROM post_place AS here
    JOIN post ON post.id = here.post_id
    -- every city of the post, not only the one asked for
    JOIN post_place AS place ON place.post_id = post.id
    WHERE here.country = :country AND here.city = :city
        AND (:before IS NULL OR post.id < :before)
    GROUP BY post.id
    ORDER BY post.id DESC
    LIMIT {FEED_PAGE_SIZE}"
            ),
            params! {
                "country" => country,
                "city" => city,
                "before" => before.map(|before| before.as_bytes().to_vec()),
            },
        )
        .await?;
    if posts.is_empty() {
        return Ok(Vec::new());
    }

    // the authors' profiles are sharded by their own id, so they're read apart instead of joined
    let authors = profile::select_authors(
        &mut conn,
        posts.iter().map(|(_, user_id, ..)| user_id).collect(),
    )
    .await?;

    Ok(posts
        .into_iter()
        .map(|(id, user_id, role, title, body, places)| PostFeedItem {
            places: parse_places(&places),
            // BINARY(16), so always the 16 bytes of a uuid
            id: Uuid::from_slice(&id)
                .map(|id| id.to_string())
                .unwrap_or_default(),
            role,
            title,
            body,
            author_name: authors
                .get(&user_id)
                .map(|author| author.name.clone())
                .unwrap_or_default(),
            author_based_in: authors
                .get(&user_id)
                .and_then(|author| author.based_in.clone()),
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

    fn tokyo() -> Place {
        Place {
            country: "JP".into(),
            city: "Tokyo".into(),
        }
    }

    fn traveler() -> CreatePostRequest {
        CreatePostRequest {
            role: PostRole::Traveler,
            places: vec![tokyo()],
            title: "Food buddy in Tokyo".into(),
            body: "Looking for someone to eat with".into(),
        }
    }

    #[test]
    fn accepts_a_traveler_post() {
        assert!(traveler().validate().is_ok());
    }

    #[test]
    fn drops_the_places_of_a_local_post() {
        let request = CreatePostRequest {
            role: PostRole::Local,
            ..traveler()
        };
        assert!(request.validate().unwrap().places.is_empty());
    }

    #[test]
    fn trims_the_title_and_body() {
        let request = CreatePostRequest {
            title: "  Hi  ".into(),
            body: "\n Hello \n".into(),
            ..traveler()
        };
        let post = request.validate().unwrap();
        assert_eq!((post.title.as_str(), post.body.as_str()), ("Hi", "Hello"));
    }

    #[test]
    fn rejects_a_blank_title() {
        let request = CreatePostRequest {
            title: "   ".into(),
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn rejects_a_title_of_two_lines() {
        let request = CreatePostRequest {
            title: "Food buddy\nin Tokyo".into(),
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn counts_characters_not_bytes() {
        let request = CreatePostRequest {
            title: "가".repeat(MAX_TITLE),
            body: "가".repeat(MAX_BODY),
            ..traveler()
        };
        assert!(request.validate().is_ok());
    }

    #[test]
    fn rejects_a_too_long_body() {
        let request = CreatePostRequest {
            body: "a".repeat(MAX_BODY + 1),
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn rejects_a_traveler_post_without_places() {
        let request = CreatePostRequest {
            places: Vec::new(),
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn rejects_too_many_places() {
        let request = CreatePostRequest {
            places: vec![tokyo(); MAX_PLACES + 1],
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn trims_the_places() {
        let request = CreatePostRequest {
            places: vec![Place {
                country: " JP ".into(),
                city: " Tokyo ".into(),
            }],
            ..traveler()
        };
        assert_eq!(request.validate().unwrap().places, vec![tokyo()]);
    }

    #[test]
    fn keeps_the_same_place_once() {
        let request = CreatePostRequest {
            places: vec![tokyo(), tokyo()],
            ..traveler()
        };
        assert_eq!(request.validate().unwrap().places, vec![tokyo()]);
    }

    #[test]
    fn rejects_a_city_that_isnt_listed() {
        let request = CreatePostRequest {
            places: vec![Place {
                country: "JP".into(),
                city: "Atlantis".into(),
            }],
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn rejects_a_country_that_isnt_a_code() {
        let request = CreatePostRequest {
            places: vec![Place {
                country: "Japan".into(),
                city: "Tokyo".into(),
            }],
            ..traveler()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn reads_the_places_json_arrayagg_builds() {
        let places = parse_places(
            r#"[{"city": "Tokyo", "country": "JP"}, {"city": "Kyoto", "country": "JP"}]"#,
        );
        assert_eq!(
            places,
            vec![
                tokyo(),
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

    #[test]
    fn reads_a_local_post_without_places() {
        let request: CreatePostRequest =
            serde_json::from_str(r#"{"role":"local","title":"t","body":"b"}"#).unwrap();
        assert!(request.places.is_empty());
    }
}
