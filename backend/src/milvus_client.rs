//! The posts' vectors in milvus, over its REST API. vitess has the posts, this only says which ones are alike
use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::{embedding_client::DIMENSION, place::Place, post::MAX_PLACES, required_env};

const POST: &str = "post";

/// What a search is for: the posts like the vector, by the same kind of people
pub struct Related<'a> {
    pub vector: Vec<f32>,
    pub role: &'a str,
    pub country: &'a str,
    /// none for anywhere in the country
    pub city: Option<&'a str>,
}

/// What a search leaves out: the viewer's own posts, and the one seen with those already found
pub struct Excluded {
    pub viewer_id: Uuid,
    pub post_ids: Vec<Uuid>,
}

/// Milvus answers 200 to a failed request too, with a code that isn't 0
#[derive(Deserialize)]
struct Response {
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Value,
}

/// A post as it's indexed: its vector, and what a search filters by
#[derive(Serialize)]
pub struct IndexedPost<'a> {
    pub id: Uuid,
    pub user_id: Uuid,
    pub role: &'a str,
    countries: Vec<&'a str>,
    cities: Vec<String>,
    vector: Vec<f32>,
}

impl<'a> IndexedPost<'a> {
    pub fn new(
        id: Uuid,
        user_id: Uuid,
        role: &'a str,
        places: &'a [Place],
        vector: Vec<f32>,
    ) -> Self {
        let mut countries: Vec<&str> = places.iter().map(|place| place.country.as_str()).collect();
        countries.sort_unstable();
        countries.dedup();
        Self {
            id,
            user_id,
            role,
            countries,
            cities: places
                .iter()
                .map(|place| indexed_city(&place.country, &place.city))
                .collect(),
            vector,
        }
    }
}

/// A city as it's indexed, with the country: the same city name can be in two countries
fn indexed_city(country: &str, city: &str) -> String {
    format!("{country}:{city}")
}

#[derive(Clone)]
pub struct MilvusClient {
    http: reqwest::Client,
    url: String,
    /// set once the collection is known to be there, a failed check is tried again by the next call
    collection_ready: Arc<OnceCell<()>>,
}

impl MilvusClient {
    pub fn from_env() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("fail to build the milvus http client"),
            url: required_env("MILVUS_URL"),
            collection_ready: Arc::default(),
        }
    }

    /// Adds the post, or replaces the one of its id
    pub async fn upsert_post(&self, post: &IndexedPost<'_>) -> Result<(), String> {
        self.ensure_collection().await?;
        self.call(
            "entities/upsert",
            &json!({ "collectionName": POST, "data": [post] }),
        )
        .await?;
        Ok(())
    }

    /// Deleting one that isn't there is fine
    pub async fn delete_post(&self, id: Uuid) -> Result<(), String> {
        self.ensure_collection().await?;
        self.call(
            "entities/delete",
            &json!({ "collectionName": POST, "filter": format!("id in [\"{id}\"]") }),
        )
        .await?;
        Ok(())
    }

    /// The vector of the post, none when it isn't indexed yet
    pub async fn find_vector(&self, id: Uuid) -> Result<Option<Vec<f32>>, String> {
        self.ensure_collection().await?;
        let mut found: Vec<VectorRow> = self
            .call(
                "entities/query",
                &json!({
                    "collectionName": POST,
                    "filter": format!("id in [\"{id}\"]"),
                    "outputFields": ["vector"],
                }),
            )
            .await
            .and_then(rows_of)?;
        Ok(found.pop().map(|row| row.vector))
    }

    /// The ids of the posts most like the vector, the nearest first
    pub async fn search_related(
        &self,
        related_to: &Related<'_>,
        excluded: &Excluded,
        limit: usize,
    ) -> Result<Vec<Uuid>, String> {
        self.ensure_collection().await?;
        let found: Vec<IdRow> = self
            .call(
                "entities/search",
                &json!({
                    "collectionName": POST,
                    "data": [related_to.vector],
                    "annsField": "vector",
                    "filter": related_filter(related_to, excluded),
                    "limit": limit,
                    "outputFields": ["id"],
                }),
            )
            .await
            .and_then(rows_of)?;
        Ok(found.into_iter().map(|row| row.id).collect())
    }

    /// Makes the collection when there's none, at startup, or by the next call when that failed
    pub async fn ensure_collection(&self) -> Result<(), String> {
        self.collection_ready
            .get_or_try_init(|| async {
                let found = self
                    .call("collections/has", &json!({ "collectionName": POST }))
                    .await?;
                if found["has"].as_bool() != Some(true) {
                    self.call("collections/create", &collection()).await?;
                }
                Ok(())
            })
            .await
            .copied()
    }

    async fn call(&self, path: &str, body: &Value) -> Result<Value, String> {
        let response = self
            .http
            .post(format!("{}/v2/vectordb/{path}", self.url))
            .json(body)
            .send()
            .await
            .map_err(|err| format!("fail to call milvus {path}: {err}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "milvus {path} returned status {}",
                response.status()
            ));
        }
        let response: Response = response
            .json()
            .await
            .map_err(|err| format!("fail to read the milvus {path} response: {err}"))?;
        data_of(path, response)
    }
}

fn data_of(path: &str, response: Response) -> Result<Value, String> {
    if response.code != 0 {
        return Err(format!(
            "milvus {path} returned code {}: {}",
            response.code, response.message
        ));
    }
    Ok(response.data)
}

#[derive(Deserialize)]
struct VectorRow {
    vector: Vec<f32>,
}

#[derive(Deserialize)]
struct IdRow {
    id: Uuid,
}

fn rows_of<T: DeserializeOwned>(data: Value) -> Result<Vec<T>, String> {
    serde_json::from_value(data).map_err(|err| format!("fail to read the milvus rows: {err}"))
}

/// milvus writes strings and lists as json does
fn related_filter(related_to: &Related<'_>, excluded: &Excluded) -> String {
    let (field, value) = match related_to.city {
        Some(city) => ("cities", indexed_city(related_to.country, city)),
        None => ("countries", related_to.country.to_owned()),
    };
    format!(
        "id not in {} and user_id != \"{}\" and role == {} and ARRAY_CONTAINS({field}, {})",
        json!(excluded.post_ids),
        excluded.viewer_id,
        json!(related_to.role),
        json!(value),
    )
}

/// Made with its index, so milvus loads it right away
fn collection() -> Value {
    let varchar = |name: &str, max_length: usize| {
        json!({
            "fieldName": name,
            "dataType": "VarChar",
            "elementTypeParams": { "max_length": max_length.to_string() },
        })
    };
    let varchars = |name: &str, max_length: usize| {
        json!({
            "fieldName": name,
            "dataType": "Array",
            "elementDataType": "VarChar",
            "elementTypeParams": {
                "max_capacity": MAX_PLACES.to_string(),
                "max_length": max_length.to_string(),
            },
        })
    };
    let mut id = varchar("id", 36);
    id["isPrimary"] = json!(true);
    json!({
        "collectionName": POST,
        "schema": {
            "autoId": false,
            "enableDynamicField": false,
            "fields": [
                id,
                varchar("user_id", 36),
                varchar("role", 10),
                varchars("countries", 2),
                // bytes and not characters: a country, a colon and post_place's 100 characters of up to 4 bytes
                varchars("cities", 403),
                {
                    "fieldName": "vector",
                    "dataType": "FloatVector",
                    "elementTypeParams": { "dim": DIMENSION.to_string() },
                },
            ],
        },
        "indexParams": [{
            "fieldName": "vector",
            "indexName": "vector",
            "indexType": "AUTOINDEX",
            "metricType": "COSINE",
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn place(country: &str, city: &str) -> Place {
        Place {
            country: country.into(),
            city: city.into(),
        }
    }

    #[test]
    fn indexes_each_country_once_and_cities_with_theirs() {
        let places = [
            place("JP", "Tokyo"),
            place("KR", "Seoul"),
            place("JP", "Kyoto"),
        ];
        let post = IndexedPost::new(
            Uuid::nil(),
            Uuid::nil(),
            "traveler",
            &places,
            vec![0.5; DIMENSION],
        );
        assert_eq!(post.countries, ["JP", "KR"]);
        assert_eq!(post.cities, ["JP:Tokyo", "KR:Seoul", "JP:Kyoto"]);
    }

    #[test]
    fn the_collection_has_every_field_of_a_post() {
        let post = IndexedPost::new(Uuid::nil(), Uuid::nil(), "local", &[], vec![]);
        let post = serde_json::to_value(post).unwrap();
        let mut sent: Vec<&str> = post
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let collection = collection();
        let mut fields: Vec<&str> = collection["schema"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["fieldName"].as_str().unwrap())
            .collect();
        sent.sort_unstable();
        fields.sort_unstable();
        assert_eq!(sent, fields);
    }

    #[test]
    fn filters_related_posts_by_role_and_place_without_the_excluded() {
        let mut related_to = Related {
            vector: vec![],
            role: "traveler",
            country: "CN",
            city: Some("Xi'an"),
        };
        let excluded = Excluded {
            viewer_id: Uuid::from_u128(2),
            post_ids: vec![Uuid::from_u128(1), Uuid::from_u128(3)],
        };
        assert_eq!(
            related_filter(&related_to, &excluded),
            "id not in [\"00000000-0000-0000-0000-000000000001\",\"00000000-0000-0000-0000-000000000003\"] \
             and user_id != \"00000000-0000-0000-0000-000000000002\" and role == \"traveler\" \
             and ARRAY_CONTAINS(cities, \"CN:Xi'an\")"
        );
        related_to.city = None;
        assert!(
            related_filter(&related_to, &excluded).ends_with("ARRAY_CONTAINS(countries, \"CN\")")
        );
    }

    #[test]
    fn reads_the_rows_of_a_search_and_a_query() {
        let found: Vec<IdRow> = rows_of(json!([
            { "distance": 0.6, "id": "01a20614-6eb4-755e-bb45-efc549a58861" },
        ]))
        .unwrap();
        assert_eq!(
            found[0].id.to_string(),
            "01a20614-6eb4-755e-bb45-efc549a58861"
        );
        let found: Vec<VectorRow> =
            rows_of(json!([{ "id": "x", "vector": [0.5, -0.25] }])).unwrap();
        assert_eq!(found[0].vector, [0.5, -0.25]);
        assert!(rows_of::<IdRow>(json!([])).unwrap().is_empty());
    }

    #[test]
    fn a_code_other_than_0_is_an_error() {
        let response = |body: &str| data_of("x", serde_json::from_str(body).unwrap());
        assert_eq!(
            response(r#"{"code":0,"data":{"has":true}}"#).unwrap()["has"],
            json!(true)
        );
        assert!(
            response(r#"{"code":1100,"message":"bad"}"#)
                .unwrap_err()
                .contains("1100: bad")
        );
    }
}
