use std::collections::HashMap;

use mysql_async::{Conn, params, prelude::Queryable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::place::Place;

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
    fn as_str(self) -> &'static str {
        match self {
            ShowBasedIn::City => "city",
            ShowBasedIn::Country => "country",
            ShowBasedIn::None => "none",
        }
    }
}

/// As much of where a person is based in as they show
#[derive(Debug, Clone, Serialize)]
pub struct ProfileBasedIn {
    country: String,
    city: Option<String>,
}

impl ProfileBasedIn {
    /// the columns select_brief_profiles leaves NULL for what a person hides: no country is nothing shown
    fn from_columns(country: Option<String>, city: Option<String>) -> Option<Self> {
        country.map(|country| ProfileBasedIn { country, city })
    }
}

/// What another person sees of a profile: on a post's author, or an invite's sender
pub struct BriefProfile {
    pub name: String,
    /// as much as they show, none when they show neither
    pub based_in: Option<ProfileBasedIn>,
}

/// A profile as it's saved, already checked and trimmed
pub struct Profile<'a> {
    pub name: &'a str,
    pub based_in_country: &'a str,
    pub based_in_city: &'a str,
    pub about_me: &'a str,
    pub grew_up_in: &'a str,
    pub languages: &'a str,
    pub dream_destination: &'a str,
    pub obsession: &'a str,
    pub local_spot: &'a str,
    pub best_trip: &'a str,
    pub birth_decade: &'a str,
    pub fun_fact: &'a str,
    pub show_based_in: ShowBasedIn,
}

pub async fn upsert(
    conn: &mut Conn,
    user_id: Uuid,
    profile: &Profile<'_>,
) -> mysql_async::Result<()> {
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
            "name" => profile.name,
            "based_in_country" => profile.based_in_country,
            "based_in_city" => profile.based_in_city,
            "about_me" => profile.about_me,
            "grew_up_in" => profile.grew_up_in,
            "languages" => profile.languages,
            "dream_destination" => profile.dream_destination,
            "obsession" => profile.obsession,
            "local_spot" => profile.local_spot,
            "best_trip" => profile.best_trip,
            "birth_decade" => profile.birth_decade,
            "fun_fact" => profile.fun_fact,
            "show_based_in" => profile.show_based_in.as_str(),
        },
    )
    .await
}

/// A person's name and where they're based in, all of it, whatever they show
pub struct NameAndBasedIn {
    pub name: String,
    pub based_in: Place,
}

pub async fn select_name_and_based_in(
    conn: &mut Conn,
    user_id: Uuid,
) -> mysql_async::Result<Option<NameAndBasedIn>> {
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
    Ok(row.map(|(name, country, city)| NameAndBasedIn {
        name,
        based_in: Place { country, city },
    }))
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

/// id, name, and the based in country and city, NULL for what's hidden
type BriefProfileRow = (Vec<u8>, String, Option<String>, Option<String>);

/// The profiles of the people, by their id. one who's gone isn't there
pub async fn select_brief_profiles(
    conn: &mut Conn,
    mut user_ids: Vec<Uuid>,
) -> mysql_async::Result<HashMap<Uuid, BriefProfile>> {
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
            user_ids
                .iter()
                .map(|id| id.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, country, city)| {
            let based_in = ProfileBasedIn::from_columns(country, city);
            // BINARY(16), so always the 16 bytes of a uuid
            (
                Uuid::from_slice(&id).unwrap_or_default(),
                BriefProfile { name, based_in },
            )
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
}
