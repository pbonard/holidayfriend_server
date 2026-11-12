use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use mysql_async::{Pool, TxOpts};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cities,
    db::{
        post::{self, NewPost, PostRole},
        profile::{self, ProfileBasedIn},
    },
    place::Place,
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
struct ValidPost {
    role: PostRole,
    /// the cities a traveler picked. empty for a local, whose city is the one their profile is based in
    places: Vec<Place>,
    title: String,
    body: String,
}

impl CreatePostRequest {
    fn validate(self) -> Result<ValidPost, &'static str> {
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

        Ok(ValidPost {
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

/// `POST /post`
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn create(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<CreatePostRequest>,
) -> Result<(StatusCode, Json<CreatePostResponse>), (StatusCode, &'static str)> {
    let mut valid = request
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let mut conn = pool.get_conn().await.map_err(failed)?;
    // every post has a city, so a city's posts are both its travelers' and its locals'.
    // a local's is copied, so their posts keep the city they were written in when they move
    if valid.role == PostRole::Local {
        let based_in = profile::select_based_in(&mut conn, user_id)
            .await
            .map_err(failed)?
            // onboarding saves the profile before anything else can be done
            .ok_or((StatusCode::BAD_REQUEST, "Set up your profile first"))?;
        valid.places = vec![based_in];
    }

    // v7 is time ordered, so new posts land at the end of the primary key instead of all over it
    let post_id = Uuid::now_v7();
    // the post and its places together or not at all
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    post::insert(
        &mut tx,
        post_id,
        user_id,
        &NewPost {
            role: valid.role,
            title: &valid.title,
            body: &valid.body,
        },
    )
    .await
    .map_err(failed)?;
    post::insert_places(&mut tx, post_id, &valid.places)
        .await
        .map_err(failed)?;
    tx.commit().await.map_err(failed)?;

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

/// `GET /post/list`: a city's posts, newest first, locals' and travelers' both. the web app's tabs filter them itself
#[derive(Debug, Deserialize)]
pub struct ListPostsQuery {
    /// ISO 3166-1 alpha-2
    country: String,
    city: String,
    /// the last post id of the page before, the posts older than it come next. the first page leaves it out.
    /// a post id and not a page number, so posts written while scrolling don't shift the pages
    before: Option<Uuid>,
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
    author_based_in: Option<ProfileBasedIn>,
    /// every city of the post, the one being explored too. a local's post has just theirs
    places: Vec<Place>,
}

/// `GET /post/list`. two round trips: the posts with their cities, then their authors
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

    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to list posts");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load posts")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let posts = post::select_city_posts(
        &mut conn,
        country,
        city,
        query.before,
        FEED_PAGE_SIZE,
        FEED_BODY_PREVIEW,
    )
    .await
    .map_err(failed)?;
    if posts.is_empty() {
        return Ok(Json(Vec::new()));
    }

    // the authors' profiles are sharded by their own id, so they're read apart instead of joined
    let authors =
        profile::select_brief_profiles(&mut conn, posts.iter().map(|post| &post.user_id).collect())
            .await
            .map_err(failed)?;

    Ok(Json(
        posts
            .into_iter()
            .map(|post| {
                let author = authors.get(&post.user_id);
                PostFeedItem {
                    id: post.id.to_string(),
                    role: post.role,
                    title: post.title,
                    body: post.body,
                    author_name: author.map(|author| author.name.clone()).unwrap_or_default(),
                    author_based_in: author.and_then(|author| author.based_in.clone()),
                    places: post.places,
                }
            })
            .collect(),
    ))
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
    fn reads_a_local_post_without_places() {
        let request: CreatePostRequest =
            serde_json::from_str(r#"{"role":"local","title":"t","body":"b"}"#).unwrap();
        assert!(request.places.is_empty());
    }
}
