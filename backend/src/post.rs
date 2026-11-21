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
    id::{IdStruct, UserId},
    kafka::{
        self,
        producer::{self, Producer},
    },
    moderation_client::ModerationClient,
    place::Place,
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

/// `POST /post`
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn create(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<CreatePostRequest>,
) -> Result<(StatusCode, Json<IdStruct>), (StatusCode, &'static str)> {
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
        &NewPost {
            id: post_id,
            user_id,
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

    Ok((StatusCode::CREATED, Json(IdStruct { id: post_id })))
}

fn failed(err: mysql_async::Error) -> (StatusCode, &'static str) {
    tracing::error!(error = %err, "fail to create post");
    (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create post")
}

/// `UpdatePostRequest` in the web app: the post to change, and all of it again as the create form sends it
#[derive(Debug, Deserialize)]
pub struct UpdatePostRequest {
    id: Uuid,
    #[serde(flatten)]
    post: CreatePostRequest,
}

/// `PUT /post`: the author changes their post, any of it but who wrote it, checked as a new one is. a traveler's
/// cities are replaced by the ones sent. a post that becomes local drops the cities its author picked for the one
/// their profile is based in, as a new local post has. a local post that stays local keeps the city it was written
/// in, as it does when the author moves. the post and its places are on the post's shard, so it's one shard's
/// transaction
pub async fn update(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<UpdatePostRequest>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to update post");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update the post",
        )
    };
    let id = request.id;
    let mut valid = request
        .post
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let mut conn = pool.get_conn().await.map_err(failed)?;
    // a local's city is the one their profile is based in, as in create. read before the transaction: the profile is
    // on its own shard, and whether the post was local already is only known inside it
    if valid.role == PostRole::Local {
        let based_in = profile::select_based_in(&mut conn, user_id)
            .await
            .map_err(failed)?
            .ok_or((StatusCode::BAD_REQUEST, "Set up your profile first"))?;
        valid.places = vec![based_in];
    }
    // a failed check returns before commit, and dropping the transaction rolls it back
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    let (author_id, old_role) = post::select_author_for_update(&mut tx, id)
        .await
        .map_err(failed)?
        .ok_or((StatusCode::NOT_FOUND, "Post not found"))?;
    if author_id != user_id {
        return Err((StatusCode::FORBIDDEN, "You can only edit your own post"));
    }
    post::update(&mut tx, id, valid.role, &valid.title, &valid.body)
        .await
        .map_err(failed)?;
    // a local post that stays local keeps the city it was written in
    if old_role != PostRole::Local || valid.role != PostRole::Local {
        // only what changed: the cities it no longer has go, the new ones come
        post::delete_places_except(&mut tx, id, &valid.places)
            .await
            .map_err(failed)?;
        post::insert_missing_places(&mut tx, id, &valid.places)
            .await
            .map_err(failed)?;
    }
    tx.commit().await.map_err(failed)?;

    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /post?id=`: the author deletes their post for good, its cities with it, in one transaction on the
/// post's shard. invites found on it stay, they're to the person and not the post. the post_not_interested rows
/// naming it stay too: they're on the shards of the people who hid it, and an id that's gone hides nothing
pub async fn delete(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Query(IdStruct { id }): Query<IdStruct>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to delete post");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to delete the post",
        )
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    // the author check is the delete's own WHERE, so there's nothing to read first
    let deleted = post::delete(&mut tx, id, user_id).await.map_err(failed)?;
    if !deleted {
        // none, or someone else's: the same answer, the post stays either way. dropping the transaction rolls it back
        return Err((StatusCode::NOT_FOUND, "Post not found"));
    }
    post::delete_places(&mut tx, id).await.map_err(failed)?;
    tx.commit().await.map_err(failed)?;

    Ok(StatusCode::NO_CONTENT)
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
    id: Uuid,
    role: String,
    title: String,
    /// the whole body, or its first FEED_BODY_PREVIEW characters when it's longer
    body: String,
    /// who to send an invite to from the card
    author_id: Uuid,
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
        profile::select_brief_profiles(&mut conn, posts.iter().map(|post| post.user_id).collect())
            .await
            .map_err(failed)?;

    Ok(Json(
        posts
            .into_iter()
            .map(|post| {
                let author = authors.get(&post.user_id);
                PostFeedItem {
                    id: post.id,
                    role: post.role,
                    title: post.title,
                    body: post.body,
                    author_id: post.user_id,
                    author_name: author.map(|author| author.name.clone()).unwrap_or_default(),
                    author_based_in: author.and_then(|author| author.based_in.clone()),
                    places: post.places,
                }
            })
            .collect(),
    ))
}

/// `GET /post/user?userId=`: whose posts, and the next page of them
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListUserPostsQuery {
    user_id: Uuid,
    /// the last post id of the page before, as in the city's feed. the first page leaves it out
    before: Option<Uuid>,
}

/// `UserPostItem` in the web app: a post as a line of its author's profile. no author: the profile above the list
/// is theirs, so a lookup of them for every page would only repeat what the page already has
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPostItem {
    id: Uuid,
    role: String,
    title: String,
    /// the whole body, or its first FEED_BODY_PREVIEW characters when it's longer
    body: String,
    /// every city of the post, the first is the one its page opens from
    places: Vec<Place>,
}

/// `GET /post/user?userId=`: a person's posts for their profile page, newest first and a page at a time as a
/// city's feed. one round trip: the posts with their cities
pub async fn list_by_user(
    _: UserId,
    State(pool): State<Pool>,
    Query(query): Query<ListUserPostsQuery>,
) -> Result<Json<Vec<UserPostItem>>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to list user posts");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load posts")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let posts = post::select_user_posts(
        &mut conn,
        query.user_id,
        query.before,
        FEED_PAGE_SIZE,
        FEED_BODY_PREVIEW,
    )
    .await
    .map_err(failed)?;

    Ok(Json(
        posts
            .into_iter()
            .map(|post| UserPostItem {
                id: post.id,
                role: post.role,
                title: post.title,
                body: post.body,
                places: post.places,
            })
            .collect(),
    ))
}

/// `PostDetail` in the web app.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostDetail {
    id: Uuid,
    role: String,
    title: String,
    /// the whole body
    body: String,
    /// who "Say hi" sends the invite to
    author_id: Uuid,
    author_name: String,
    /// as much as the author shows, as in the feed
    author_based_in: Option<ProfileBasedIn>,
    places: Vec<Place>,
}

/// `GET /post?id=`. two round trips, as the feed: the post with its cities, then its author
pub async fn get(
    _: UserId,
    State(pool): State<Pool>,
    Query(IdStruct { id }): Query<IdStruct>,
) -> Result<Json<PostDetail>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to get post");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to load the post")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let post = post::select_post(&mut conn, id)
        .await
        .map_err(failed)?
        .ok_or((StatusCode::NOT_FOUND, "Post not found"))?;
    let authors = profile::select_brief_profiles(&mut conn, vec![post.user_id])
        .await
        .map_err(failed)?;
    let author = authors.get(&post.user_id);

    Ok(Json(PostDetail {
        id: post.id,
        role: post.role,
        title: post.title,
        body: post.body,
        author_id: post.user_id,
        author_name: author.map(|author| author.name.clone()).unwrap_or_default(),
        author_based_in: author.and_then(|author| author.based_in.clone()),
        places: post.places,
    }))
}

/// `POST /post/not-interested`: the person asking hides the post from their feed, from the more button of its
/// card. no check that the post exists: a missing one hides nothing, and checking would read the post's shard too
pub async fn not_interested(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(IdStruct { id }): Json<IdStruct>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to mark post not interested");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to hide the post")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    post::insert_not_interested(&mut conn, user_id, id)
        .await
        .map_err(failed)?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /post/not-interested`: the ids of every post the person asking hid, for the web app to leave out of the
/// feeds. apart from `GET /post/list`, so a city's feed stays the same for everyone
pub async fn list_not_interested(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
) -> Result<Json<Vec<Uuid>>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to list not interested posts");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to load hidden posts",
        )
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let post_ids = post::select_not_interested_post_ids(&mut conn, user_id)
        .await
        .map_err(failed)?;
    Ok(Json(post_ids))
}

/// `POST /post/report`: the person asking reports someone's post, from the more button of its card or its page.
/// answered once the broker has it. no check that the post exists or who wrote it: the consumer reads the post,
/// a missing one is dropped there (kafka/consumer.rs)
pub async fn report(
    UserId(user_id): UserId,
    State(producer): State<Producer>,
    Json(IdStruct { id }): Json<IdStruct>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    producer::send(
        &producer,
        kafka::TOPIC,
        id.as_bytes(),
        None,
        vec![(
            kafka::TASK_TYPE_HEADER.to_string(),
            kafka::REPORT_POST_TASK.as_bytes().to_vec(),
        )],
    )
    .await
    .map_err(|err| {
        tracing::error!(error = %err, post_id = %id, "fail to report post");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to report the post",
        )
    })?;
    tracing::info!(post_id = %id, reporter_id = %user_id, "post reported");
    Ok(StatusCode::NO_CONTENT)
}

/// A reported post, the report consumer's REPORT_POST_TASK, as howwasread-server's onlineconversation evaluates a
/// reported conversation. asks the model about the post and saves what it said. a violation deletes the post, its
/// cities with it; a post that's fine is marked so the next report of it is skipped, until it's edited. the model's
/// call is outside the transaction, it takes seconds: the post is checked to be unchanged when it's written down
/// instead. an error is why it failed, the report goes to the retry job with it
pub async fn manage_report(
    pool: &Pool,
    moderation_client: &ModerationClient,
    post_id: Uuid,
) -> Result<(), String> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, post_id = %post_id, "fail to evaluate reported post");
        err.to_string()
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let Some(target) = post::select_report_target(&mut conn, post_id)
        .await
        .map_err(failed)?
    else {
        // deleted by its author, or by an earlier report
        return Ok(());
    };
    if target.is_evaluated {
        return Ok(());
    }

    let verdict = moderation_client
        .evaluate(&target.title, &target.body)
        .await?;

    // a failed step returns before commit, and dropping the transaction rolls it back
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    let marked = post::mark_evaluated_if_unchanged(&mut tx, post_id, &target.updated_at)
        .await
        .map_err(failed)?;
    if !marked {
        tracing::info!(
            post_id = %post_id,
            violation = verdict.violation,
            "post changed during report evaluation"
        );
        if !verdict.violation {
            return Err(
                "post contents was okay, but contents changed during report evaluation".into(),
            );
        }
        // so when it "was" a violation, the post is deleted even when it was edited while the model read it
    }
    post::insert_verdict(
        &mut tx,
        post_id,
        &target,
        moderation_client.model(),
        &verdict,
    )
    .await
    .map_err(failed)?;
    if verdict.violation {
        tracing::info!(
            post_id = %post_id,
            category = verdict.category,
            "delete violating post"
        );
        // by its author, as they would delete it. none when they deleted it meanwhile, its cities go either way
        post::delete(&mut tx, post_id, target.user_id)
            .await
            .map_err(failed)?;
        post::delete_places(&mut tx, post_id)
            .await
            .map_err(failed)?;
    }
    tx.commit().await.map_err(failed)?;
    Ok(())
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

    // the web app reads ids as the hyphenated strings, uuid's serde writes them that way
    #[test]
    fn writes_ids_as_uuid_strings() {
        let json = serde_json::to_string(&IdStruct {
            id: Uuid::from_u128(1),
        })
        .unwrap();
        assert_eq!(json, r#"{"id":"00000000-0000-0000-0000-000000000001"}"#);
    }

    #[test]
    fn reads_a_local_post_without_places() {
        let request: CreatePostRequest =
            serde_json::from_str(r#"{"role":"local","title":"t","body":"b"}"#).unwrap();
        assert!(request.places.is_empty());
    }

    // the web app sends the id beside the create form's fields, not around them
    #[test]
    fn reads_an_update_as_the_id_and_the_create_fields() {
        let request: UpdatePostRequest = serde_json::from_str(
            r#"{"id":"00000000-0000-0000-0000-000000000001","role":"traveler",
                "places":[{"country":"JP","city":"Tokyo"}],"title":"t","body":"b"}"#,
        )
        .unwrap();
        assert_eq!(request.id, Uuid::from_u128(1));
        assert_eq!(request.post.validate().unwrap().places, vec![tokyo()]);
    }
}
