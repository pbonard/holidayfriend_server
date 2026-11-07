use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::{StatusCode, request::Parts},
};
use mysql_async::{Pool, params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_NAME: usize = 30;
const MAX_PLACE: usize = 100;
const MAX_ABOUT_ME: usize = 300;
const MAX_PROMPT: usize = 100;

/// The member the gateway verified the access token of, it sets the header from the `sub` claim.
pub struct UserId(Uuid);

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

/// The fields of `ProfileFormValue` in the web app, the ones left out count as empty.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SaveProfileRequest {
    name: String,
    based_in_country: String,
    based_in_city: String,
    about_me: String,
    grew_up_in: String,
    languages: String,
    dream_destination: String,
    obsession: String,
    local_spot: String,
    best_trip: String,
    birth_decade: String,
    fun_fact: String,
}

impl SaveProfileRequest {
    fn validate(&self) -> Result<(), &'static str> {
        check(
            &self.name,
            MAX_NAME,
            true,
            "Enter a name of up to 30 characters",
        )?;
        check(
            &self.based_in_country,
            MAX_PLACE,
            true,
            "Select your country",
        )?;
        check(&self.based_in_city, MAX_PLACE, true, "Select your city")?;
        check(&self.about_me, MAX_ABOUT_ME, false, "About me is too long")?;

        let prompts = [
            &self.grew_up_in,
            &self.languages,
            &self.dream_destination,
            &self.obsession,
            &self.local_spot,
            &self.best_trip,
            &self.birth_decade,
            &self.fun_fact,
        ];
        for prompt in prompts {
            check(prompt, MAX_PROMPT, false, "An answer is too long")?;
        }
        Ok(())
    }
}

fn check(
    value: &str,
    max: usize,
    required: bool,
    message: &'static str,
) -> Result<(), &'static str> {
    let value = value.trim();
    if (required && value.is_empty()) || value.chars().count() > max {
        return Err(message);
    }
    Ok(())
}

/// PUT, so pressing Done again after a failed response saves the same profile instead of erroring.
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn save(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(profile): Json<SaveProfileRequest>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    profile
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    upsert(&pool, user_id, &profile).await.map_err(|err| {
        eprintln!("fail to save profile of {user_id}: {err}");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save profile")
    })?;

    Ok(StatusCode::NO_CONTENT)
}

async fn upsert(
    pool: &Pool,
    user_id: Uuid,
    profile: &SaveProfileRequest,
) -> mysql_async::Result<()> {
    let mut conn = pool.get_conn().await?;
    conn.exec_drop(
        r"-- sql
    INSERT INTO profile (
        id, name, based_in_country, based_in_city, about_me, grew_up_in, languages,
        dream_destination, obsession, local_spot, best_trip, birth_decade, fun_fact
    ) VALUES (
        :user_id, :name, :based_in_country, :based_in_city, :about_me, :grew_up_in, :languages,
        :dream_destination, :obsession, :local_spot, :best_trip, :birth_decade, :fun_fact
    ) ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        based_in_country = VALUES(based_in_country),
        based_in_city = VALUES(based_in_city),
        about_me = VALUES(about_me),
        grew_up_in = VALUES(grew_up_in),
        languages = VALUES(languages),
        dream_destination = VALUES(dream_destination),
        obsession = VALUES(obsession),
        local_spot = VALUES(local_spot),
        best_trip = VALUES(best_trip),
        birth_decade = VALUES(birth_decade),
        fun_fact = VALUES(fun_fact)",
        params! {
            "user_id" => user_id.as_bytes().to_vec(),
            "name" => profile.name.trim(),
            "based_in_country" => profile.based_in_country.trim(),
            "based_in_city" => profile.based_in_city.trim(),
            "about_me" => profile.about_me.trim(),
            "grew_up_in" => profile.grew_up_in.trim(),
            "languages" => profile.languages.trim(),
            "dream_destination" => profile.dream_destination.trim(),
            "obsession" => profile.obsession.trim(),
            "local_spot" => profile.local_spot.trim(),
            "best_trip" => profile.best_trip.trim(),
            "birth_decade" => profile.birth_decade.trim(),
            "fun_fact" => profile.fun_fact.trim(),
        },
    )
    .await
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBriefProfileResponse {
    name: String,
    based_in_country: String,
    based_in_city: String,
}

pub async fn get_brief(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
) -> Result<Json<GetBriefProfileResponse>, (StatusCode, &'static str)> {
    let brief = select_brief(&pool, user_id).await.map_err(|err| {
        eprintln!("fail to get profile brief of {user_id}: {err}");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
    })?;

    brief
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "Profile not found"))
}

async fn select_brief(
    pool: &Pool,
    user_id: Uuid,
) -> mysql_async::Result<Option<GetBriefProfileResponse>> {
    let mut conn = pool.get_conn().await?;
    let row: Option<(String, String, String)> = conn
        .exec_first(
            r"-- sql
    SELECT name, based_in_country, based_in_city
    FROM profile
    WHERE id = :user_id",
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
            },
        )
        .await?;

    Ok(row.map(
        |(name, based_in_country, based_in_city)| GetBriefProfileResponse {
            name,
            based_in_country,
            based_in_city,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> SaveProfileRequest {
        SaveProfileRequest {
            name: "Jay".into(),
            based_in_country: "KR".into(),
            based_in_city: "Seoul".into(),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_the_required_fields_alone() {
        assert!(valid().validate().is_ok());
    }

    #[test]
    fn rejects_a_blank_name() {
        let profile = SaveProfileRequest {
            name: "   ".into(),
            ..valid()
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn rejects_a_missing_city() {
        let profile = SaveProfileRequest {
            based_in_city: String::new(),
            ..valid()
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn counts_characters_not_bytes() {
        let profile = SaveProfileRequest {
            name: "가".repeat(MAX_NAME),
            ..valid()
        };
        assert!(profile.validate().is_ok());
    }

    #[test]
    fn rejects_a_too_long_answer() {
        let profile = SaveProfileRequest {
            fun_fact: "a".repeat(MAX_PROMPT + 1),
            ..valid()
        };
        assert!(profile.validate().is_err());
    }
}
