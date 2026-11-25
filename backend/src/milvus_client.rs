//! The posts' vectors in milvus, with its rust sdk over grpc. vitess has the posts, this only says which ones are alike
use std::time::Duration;

use milvus::v2::{
    error::Error,
    prelude::{
        ClientV2, CollectionSchema, ConnectConfig, ConsistencyLevel, CreateCollectionRequest,
        DataType, DeleteRequest, FieldSchema, HasCollectionRequest, Ids, IndexParam, IndexType,
        InsertRequest, MetricType, QueryRequest, RetryConfig, SearchRequest, SearchVectors,
        TelemetryConfig, UpsertRequest,
    },
};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::{embedding_client::DIMENSION, place::Place, post::MAX_PLACES};

const POST: &str = "post";
/// For connecting, and for a call with its retries. the sdk's own for a call is none: it would go on for minutes while milvus is away
const TIMEOUT: Duration = Duration::from_secs(10);

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
    client: ClientV2,
}

impl MilvusClient {
    /// Connects, and makes the collection when there's none. it connects again by itself when milvus goes away later
    pub async fn connect(url: &str) -> Result<Self, String> {
        let config = ConnectConfig::new()
            .uri(url)
            .connect_timeout(TIMEOUT)
            .rpc_timeout(TIMEOUT)
            .retry(
                RetryConfig::new()
                    .max_attempts(3)
                    .max_retry_timeout(TIMEOUT),
            )
            // a heartbeat every 10 seconds, with the calls' metrics
            .telemetry(TelemetryConfig::new().enabled(false));
        let client = ClientV2::new(&config).await.map_err(failed_to("connect"))?;

        let failed = failed_to("find the collection");
        let has = HasCollectionRequest::builder()
            .collection_name(POST)
            .build()
            .map_err(&failed)?;
        if !client.has_collection(has).await.map_err(&failed)?.exists() {
            client
                .create_collection(build_post_collection_request()?)
                .await
                .map_err(failed_to("create the collection"))?;
        }
        Ok(Self { client })
    }

    /// Adds the post, or replaces the one of its id
    pub async fn upsert_post(&self, post: &IndexedPost<'_>) -> Result<(), String> {
        let failed = failed_to("upsert a post");
        let request = build_upsert_request(post)?;
        self.client.upsert(request).await.map_err(failed)?;
        Ok(())
    }

    /// Deleting one that isn't there is fine
    pub async fn delete_post(&self, id: Uuid) -> Result<(), String> {
        let failed = failed_to("delete a post");
        let request = DeleteRequest::builder()
            .collection_name(POST)
            .ids(Ids::VarChar(vec![id.to_string()]))
            .build()
            .map_err(&failed)?;
        self.client.delete(request).await.map_err(failed)?;
        Ok(())
    }

    /// The vector of the post, none when it isn't indexed yet
    pub async fn find_vector(&self, id: Uuid) -> Result<Option<Vec<f32>>, String> {
        let failed = failed_to("find a vector");
        let request = QueryRequest::builder()
            .collection_name(POST)
            .ids(Ids::VarChar(vec![id.to_string()]))
            .output_fields(["vector"])
            .consistency_level(ConsistencyLevel::Bounded)
            .build()
            .map_err(&failed)?;
        let found = self.client.query(request).await.map_err(&failed)?;
        let Some(row) = found.results().rows().map_err(&failed)?.next() else {
            return Ok(None);
        };
        let vector = row.get_float_vector("vector").map_err(failed)?;
        Ok(Some(vector.to_vec()))
    }

    /// The ids of the posts most like the vector, the nearest first
    pub async fn search_related(
        &self,
        related_to: &Related<'_>,
        excluded: &Excluded,
        limit: usize,
    ) -> Result<Vec<Uuid>, String> {
        let failed = failed_to("search posts");
        let request = build_search_request(related_to, excluded, limit)?;
        let found = self.client.search(request).await.map_err(&failed)?;
        let mut ids = Vec::new();
        // one result for the one vector
        for result in found.results().iter() {
            for row in result.rows().map_err(&failed)? {
                let id = row.get_str("id").map_err(&failed)?;
                ids.push(
                    id.parse()
                        .map_err(|err| format!("milvus has a post of id {id}: {err}"))?,
                );
            }
        }
        Ok(ids)
    }
}

fn failed_to(what: &str) -> impl Fn(Error) -> String {
    move |err| format!("milvus: fail to {what}: {err}")
}

fn build_upsert_request(post: &IndexedPost<'_>) -> Result<UpsertRequest, String> {
    let failed = failed_to("make an upsert");
    let insert = InsertRequest::builder()
        .collection_name(POST)
        .rows([json!(post)])
        .build()
        .map_err(&failed)?;
    UpsertRequest::builder()
        .insert(insert)
        .build()
        .map_err(failed)
}

fn build_search_request(
    related_to: &Related<'_>,
    excluded: &Excluded,
    limit: usize,
) -> Result<SearchRequest, String> {
    SearchRequest::builder()
        .collection_name(POST)
        .vector_field("vector")
        .vectors(SearchVectors::Float(vec![related_to.vector.clone()]))
        .filter(related_filter(related_to, excluded))
        .limit(limit as i64)
        .consistency_level(ConsistencyLevel::Bounded)
        .build()
        .map_err(failed_to("make a search"))
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

fn build_post_schema() -> CollectionSchema {
    let varchar = |name: &str, max_length: u32| {
        FieldSchema::new()
            .name(name)
            .data_type(DataType::VarChar)
            .max_length(max_length)
    };
    let varchars = |name: &str, max_length: u32| {
        FieldSchema::new()
            .name(name)
            .data_type(DataType::Array)
            .element_type(DataType::VarChar)
            .max_capacity(MAX_PLACES as u32)
            .max_length(max_length)
    };
    CollectionSchema::new()
        .enable_dynamic_field(false)
        .add_field(varchar("id", 36).primary_key(true))
        .add_field(varchar("user_id", 36))
        .add_field(varchar("role", 10))
        .add_field(varchars("countries", 2))
        // bytes and not characters: a country, a colon and post_place's 100 characters of up to 4 bytes
        .add_field(varchars("cities", 403))
        .add_field(
            FieldSchema::new()
                .name("vector")
                .data_type(DataType::FloatVector)
                .dimension(DIMENSION as u32),
        )
}

/// Made with its index, so the sdk loads it right away
fn build_post_collection_request() -> Result<CreateCollectionRequest, String> {
    CreateCollectionRequest::builder()
        .collection_name(POST)
        .schema(build_post_schema())
        .index_param(
            IndexParam::new()
                .field_name("vector")
                .index_name("vector")
                .index_type(IndexType::AutoIndex)
                .metric_type(MetricType::Cosine),
        )
        .build()
        .map_err(failed_to("make the collection"))
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
        let post = json!(post);
        let mut sent: Vec<&str> = post
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let schema = build_post_schema();
        let mut fields: Vec<&str> = schema
            .get_fields()
            .iter()
            .map(|field| field.get_name())
            .collect();
        sent.sort_unstable();
        fields.sort_unstable();
        assert_eq!(sent, fields);
    }

    #[test]
    fn the_requests_are_valid() {
        let related_to = Related {
            vector: vec![0.5; DIMENSION],
            role: "local",
            country: "KR",
            city: None,
        };
        let excluded = Excluded {
            viewer_id: Uuid::nil(),
            post_ids: vec![Uuid::nil()],
        };
        let post = IndexedPost::new(Uuid::nil(), Uuid::nil(), "local", &[], vec![0.5; DIMENSION]);
        assert!(build_post_collection_request().is_ok());
        assert!(build_upsert_request(&post).is_ok());
        assert!(build_search_request(&related_to, &excluded, 10).is_ok());
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
}
