use std::collections::HashMap;

use axum::{Json, extract::State, http::StatusCode};
use mysql_async::{Conn, Pool, params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{cities, place::Place, user::UserId};

const MAX_NAME: usize = 30;
const MAX_PLACE: usize = 100;
const MAX_ABOUT_ME: usize = 300;
const MAX_PROMPT: usize = 100;

/// How much of where they're based in a person shows on their posts. one value and not a toggle each,
/// so showing the city without the country can't be saved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShowBasedIn {
    /// the city and its country
    #[default]
    City,
    Country,
    None,
}

impl ShowBasedIn {
    pub fn as_str(self) -> &'static str {
        match self {
            ShowBasedIn::City => "city",
            ShowBasedIn::Country => "country",
            ShowBasedIn::None => "none",
        }
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
    /// left out, the city and country are shown, as before there was a choice
    show_based_in: ShowBasedIn,
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
        tracing::error!(error = %err, "fail to save profile");
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
        dream_destination, obsession, local_spot, best_trip, birth_decade, fun_fact, show_based_in
    ) VALUES (
        :user_id, :name, :based_in_country, :based_in_city, :about_me, :grew_up_in, :languages,
        :dream_destination, :obsession, :local_spot, :best_trip, :birth_decade, :fun_fact, :show_based_in
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
        fun_fact = VALUES(fun_fact),
        show_based_in = VALUES(show_based_in)",
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
            "show_based_in" => profile.show_based_in.as_str(),
        },
    )
    .await
}

/// `MyProfileBrief` in the web app: what a screen needs of the person asking.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MyProfileBriefResponse {
    name: String,
    based_in_country: String,
    based_in_city: String,
}

pub async fn get_my_brief(
    UserId(user_id): UserId,
    State(pool): State<Pool>,
) -> Result<Json<MyProfileBriefResponse>, (StatusCode, &'static str)> {
    let brief = select_brief(&pool, user_id).await.map_err(|err| {
        tracing::error!(error = %err, "fail to get profile brief");
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get profile")
    })?;

    brief
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "Profile not found"))
}

async fn select_brief(
    pool: &Pool,
    user_id: Uuid,
) -> mysql_async::Result<Option<MyProfileBriefResponse>> {
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
        |(name, based_in_country, based_in_city)| MyProfileBriefResponse {
            name,
            based_in_country,
            based_in_city,
        },
    ))
}

/// Whether a person has set up their profile, which onboarding does before anything else
pub async fn exists(conn: &mut Conn, user_id: Uuid) -> mysql_async::Result<bool> {
    let row: Option<u8> = conn
        .exec_first(
            r"-- sql
    SELECT 1
    FROM profile
    WHERE id = :user_id",
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.is_some())
}

/// Where a person is based in, to place a local's post in their city. what they show doesn't matter here:
/// a local's post is in their city either way
pub async fn select_based_in(conn: &mut Conn, user_id: Uuid) -> mysql_async::Result<Option<Place>> {
    let row: Option<(String, String)> = conn
        .exec_first(
            r"-- sql
    SELECT based_in_country, based_in_city
    FROM profile
    WHERE id = :user_id",
            params! {
                "user_id" => user_id.as_bytes().to_vec(),
            },
        )
        .await?;
    Ok(row.map(|(country, city)| Place { country, city }))
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileBasedIn {
    country: String,
    city: Option<String>,
}

impl ProfileBasedIn {
    /// the columns select_authors leaves NULL for what an author hides: no country is nothing shown
    fn from_columns(country: Option<String>, city: Option<String>) -> Option<Self> {
        country.map(|country| ProfileBasedIn { country, city })
    }
}

pub struct BriefProfile {
    pub name: String,
    /// as much as they show, none when they show neither
    pub based_in: Option<ProfileBasedIn>,
}

/// id, name, and the based in country and city, NULL for what's hidden
type BriefProfileRow = (Vec<u8>, String, Option<String>, Option<String>);

pub async fn select_brief_profiles(
    conn: &mut Conn,
    mut user_ids: Vec<&Vec<u8>>,
) -> mysql_async::Result<HashMap<Vec<u8>, BriefProfile>> {
    // one author of several posts is asked for once
    user_ids.sort();
    user_ids.dedup();
    if user_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<BriefProfileRow> = conn
        .exec(
            format!(
                r"-- sql
    SELECT
        id,
        name,
        CASE WHEN show_based_in IN ('city', 'country') THEN based_in_country END,
        CASE WHEN show_based_in = 'city' THEN based_in_city END
    FROM profile
    WHERE id IN ({})",
                vec!["?"; user_ids.len()].join(", ")
            ),
            user_ids,
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, country, city)| {
            let based_in = ProfileBasedIn::from_columns(country, city);
            (id, BriefProfile { name, based_in })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_shown_city_and_country() {
        let based_in =
            ProfileBasedIn::from_columns(Some("KR".into()), Some("Seoul".into())).unwrap();
        assert_eq!(
            (based_in.country.as_str(), based_in.city.as_deref()),
            ("KR", Some("Seoul"))
        );
    }

    #[test]
    fn reads_a_shown_country_alone() {
        let based_in = ProfileBasedIn::from_columns(Some("KR".into()), None).unwrap();
        assert_eq!((based_in.country.as_str(), based_in.city), ("KR", None));
    }

    #[test]
    fn reads_nothing_shown_as_none() {
        assert!(ProfileBasedIn::from_columns(None, None).is_none());
    }

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
