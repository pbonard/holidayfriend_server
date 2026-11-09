use axum::{
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use uuid::Uuid;

/// The member the gateway verified the access token of, it sets the header from the `sub` claim.
pub struct UserId(pub Uuid);

impl<S: Sync> FromRequestParts<S> for UserId {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .headers
            .get("X-User-Id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Uuid::parse_str(value).ok())
            .map(UserId)
            .ok_or((StatusCode::UNAUTHORIZED, "Missing or invalid X-User-Id"))
    }
}
