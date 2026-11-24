//! Turns text into a vector with ollama's embedding model: texts that mean alike are near each other
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::required_env;

/// The numbers of a vector, bge-m3's. the milvus collection is made with it
pub const DIMENSION: usize = 1024;

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

#[derive(Clone)]
pub struct EmbeddingClient {
    http: reqwest::Client,
    url: String,
    model: String,
}

impl EmbeddingClient {
    pub fn from_env() -> Self {
        let url = required_env("OLLAMA_URL");
        let timeout_seconds: u64 = required_env("OLLAMA_TIMEOUT_SECONDS")
            .parse()
            .expect("OLLAMA_TIMEOUT_SECONDS must be a number of seconds");
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(timeout_seconds))
                .build()
                .expect("fail to build the embedding http client"),
            url: format!("{url}/api/embed"),
            model: required_env("OLLAMA_EMBEDDING_MODEL"),
        }
    }

    pub async fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let response = self
            .http
            .post(&self.url)
            .json(&json!({ "model": self.model, "input": text }))
            .send()
            .await
            .map_err(|err| format!("fail to call ollama: {err}"))?;
        if !response.status().is_success() {
            return Err(format!("ollama returned status {}", response.status()));
        }
        let response: EmbedResponse = response
            .json()
            .await
            .map_err(|err| format!("fail to read the ollama embedding: {err}"))?;
        only_vector(response.embeddings)
    }
}

/// The vector of the one text asked for. another model's size is an error, milvus wouldn't take it
fn only_vector(mut embeddings: Vec<Vec<f32>>) -> Result<Vec<f32>, String> {
    match embeddings.pop() {
        Some(vector) if embeddings.is_empty() && vector.len() == DIMENSION => Ok(vector),
        vector => Err(format!(
            "ollama returned {} more embeddings and one of {:?} numbers",
            embeddings.len(),
            vector.map(|vector| vector.len())
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takes_one_vector_of_the_collections_size() {
        assert_eq!(
            only_vector(vec![vec![0.5; DIMENSION]]).unwrap().len(),
            DIMENSION
        );
    }

    #[test]
    fn rejects_none_many_or_another_size() {
        assert!(only_vector(vec![]).is_err());
        assert!(only_vector(vec![vec![0.5; DIMENSION], vec![0.5; DIMENSION]]).is_err());
        assert!(only_vector(vec![vec![0.5; 768]]).is_err());
    }
}
