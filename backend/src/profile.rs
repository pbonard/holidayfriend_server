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
        outbox,
        profile::{self, Profile, ProfileBasedIn, ShowBasedIn},
        verdict::{self, Kind, NewVerdict},
    },
    id::{IdStruct, UserId},
    kafka::{
        self,
        producer::{self, Producer},
    },
    moderation_client::ModerationClient,
};

const MAX_NAME: usize = 30;
const MAX_PLACE: usize = 100;
const MAX_ABOUT_ME: usize = 300;
const MAX_PROMPT: usize = 100;

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
    /// left out, the city and country are shown, as before there was a choice
    show_based_in: ShowBasedIn,
}

impl SaveProfileRequest {
    /// The profile to save, trimmed, when it's valid
    fn validate(&self) -> Result<Profile<'_>, &'static str> {
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
        // the pair the web app's pickers offer, not just anything entered
        if !cities::is_city(self.based_in_country.trim(), self.based_in_city.trim()) {
            return Err("Select your city from the list");
        }
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
        Ok(Profile {
            name: self.name.trim(),
            based_in_country: self.based_in_country.trim(),
            based_in_city: self.based_in_city.trim(),
            about_me: self.about_me.trim(),
            grew_up_in: self.grew_up_in.trim(),
            languages: self.languages.trim(),
            dream_destination: self.dream_destination.trim(),
            obsession: self.obsession.trim(),
            local_spot: self.local_spot.trim(),
            best_trip: self.best_trip.trim(),
            birth_decade: self.birth_decade.trim(),
            fun_fact: self.fun_fact.trim(),
            show_based_in: self.show_based_in,
        })
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

/// `PUT /profile`. PUT, so pressing Done again after a failed response saves the same profile instead of erroring.
/// Errors are plain text, the web app's axios interceptor shows them as they are.
pub async fn save(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
    Json(request): Json<SaveProfileRequest>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    let profile = request
        .validate()
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;

    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to save profile");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save profile")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    profile::upsert(&mut conn, user_id, &profile)
        .await
        .map_err(failed)?;

    Ok(StatusCode::NO_CONTENT)
}

/// `MyProfileBrief` in the web app: what a screen needs of the person asking.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MyProfileBriefResponse {
    /// the web app tells its own posts by it, e.g. no "Say hi" on them
    id: Uuid,
    name: String,
    based_in_country: String,
    based_in_city: String,
}

/// `GET /profile/brief/my`
pub async fn get_my_brief(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
) -> Result<Json<MyProfileBriefResponse>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to get profile brief");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let brief = profile::select_name_and_based_in(&mut conn, user_id)
        .await
        .map_err(failed)?;

    brief
        .map(|brief| {
            Json(MyProfileBriefResponse {
                id: user_id,
                name: brief.name,
                based_in_country: brief.based_in.country,
                based_in_city: brief.based_in.city,
            })
        })
        .ok_or((StatusCode::NOT_FOUND, "Profile not found"))
}

/// `Profile` in the web app: a person's profile page, as anyone sees it
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileResponse {
    /// who "Say hi" sends the invite to, and whose posts the page lists
    id: Uuid,
    name: String,
    /// as much as they show, none when they show neither
    based_in: Option<ProfileBasedIn>,
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

/// `GET /profile?id=`. the person's own page shows only what they show others too
pub async fn get(
    _: UserId,
    State(pool): State<Pool>,
    Query(IdStruct { id }): Query<IdStruct>,
) -> Result<Json<ProfileResponse>, (StatusCode, &'static str)> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, "fail to get profile");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let profile = profile::select_public_profile(&mut conn, id)
        .await
        .map_err(failed)?
        .ok_or((StatusCode::NOT_FOUND, "Profile not found"))?;

    Ok(Json(ProfileResponse {
        id,
        name: profile.name,
        based_in: profile.based_in,
        about_me: profile.about_me,
        grew_up_in: profile.grew_up_in,
        languages: profile.languages,
        dream_destination: profile.dream_destination,
        obsession: profile.obsession,
        local_spot: profile.local_spot,
        best_trip: profile.best_trip,
        birth_decade: profile.birth_decade,
        fun_fact: profile.fun_fact,
    }))
}

pub async fn report(
    UserId(user_id): UserId,
    State(producer): State<Producer>,
    Json(IdStruct { id }): Json<IdStruct>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    if id == user_id {
        return Err((StatusCode::BAD_REQUEST, "You can't report yourself"));
    }
    producer::send(
        &producer,
        kafka::TOPIC,
        id.as_bytes(),
        None,
        vec![(
            kafka::TASK_TYPE_HEADER.to_string(),
            kafka::REPORT_PROFILE_TASK.as_bytes().to_vec(),
        )],
    )
    .await
    .map_err(|err| {
        tracing::error!(error = %err, profile_id = %id, "fail to report profile");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to report the profile",
        )
    })?;
    tracing::info!(profile_id = %id, reporter_id = %user_id, "profile reported");
    Ok(StatusCode::NO_CONTENT)
}

pub async fn manage_report(
    pool: &Pool,
    moderation_client: &ModerationClient,
    profile_id: Uuid,
) -> Result<(), String> {
    let failed = |err: mysql_async::Error| {
        tracing::error!(error = %err, profile_id = %profile_id, "fail to evaluate reported profile");
        err.to_string()
    };
    let mut conn = pool.get_conn().await.map_err(failed)?;
    let Some(target) = profile::select_report_target(&mut conn, profile_id)
        .await
        .map_err(failed)?
    else {
        return Ok(());
    };
    if target.is_evaluated {
        return Ok(());
    }

    let fields: Vec<(&str, &str)> = target
        .fields
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    let verdict = moderation_client.evaluate(&fields).await?;

    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(failed)?;
    let marked = profile::mark_evaluated_if_unchanged(&mut tx, profile_id, &target.updated_at)
        .await
        .map_err(failed)?;
    if !marked {
        tracing::info!(
            profile_id = %profile_id,
            violation = verdict.violation,
            "profile changed during report evaluation"
        );
        if !verdict.violation {
            return Err(
                "profile contents was okay, but contents changed during report evaluation".into(),
            );
        }
    }
    verdict::insert(
        &mut tx,
        &NewVerdict {
            kind: Kind::Profile,
            target_id: profile_id,
            user_id: profile_id,
            content_updated_at: Some(target.updated_at.clone()),
            contents: target.contents(),
            model: moderation_client.model(),
            verdict: &verdict,
        },
    )
    .await
    .map_err(failed)?;
    if verdict.violation {
        tracing::info!(
            profile_id = %profile_id,
            category = verdict.category,
            "ban the owner of a violating profile"
        );
        outbox::publish_ban(&mut tx, profile_id)
            .await
            .map_err(failed)?;
    }
    tx.commit().await.map_err(failed)?;
    Ok(())
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
    fn trims_what_it_saves() {
        let request = SaveProfileRequest {
            name: "  Jay  ".into(),
            ..valid()
        };
        assert_eq!(request.validate().unwrap().name, "Jay");
    }

    fn show_based_in_of(json: &str) -> serde_json::Result<ShowBasedIn> {
        serde_json::from_str::<SaveProfileRequest>(json).map(|profile| profile.show_based_in)
    }

    #[test]
    fn reads_what_of_the_based_in_is_shown() {
        assert_eq!(
            show_based_in_of(r#"{"showBasedIn":"city"}"#).unwrap(),
            ShowBasedIn::City
        );
        assert_eq!(
            show_based_in_of(r#"{"showBasedIn":"country"}"#).unwrap(),
            ShowBasedIn::Country
        );
        assert_eq!(
            show_based_in_of(r#"{"showBasedIn":"none"}"#).unwrap(),
            ShowBasedIn::None
        );
    }

    #[test]
    fn shows_the_city_when_left_out() {
        assert_eq!(show_based_in_of("{}").unwrap(), ShowBasedIn::City);
    }

    #[test]
    fn rejects_an_unknown_show_based_in() {
        assert!(show_based_in_of(r#"{"showBasedIn":"everyone"}"#).is_err());
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
    fn rejects_a_city_that_isnt_listed() {
        let profile = SaveProfileRequest {
            based_in_city: "Atlantis".into(),
            ..valid()
        };
        assert!(profile.validate().is_err());
    }

    #[test]
    fn rejects_a_city_of_another_country() {
        let profile = SaveProfileRequest {
            based_in_city: "Tokyo".into(),
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
