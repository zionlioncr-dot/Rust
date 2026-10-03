use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct OllamaEmbeddingClient {
    client: Client,
    base_url: String,
    model: String,
}

#[derive(Debug, Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

impl OllamaEmbeddingClient {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("Failed to create Ollama HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
        })
    }

    pub async fn embed(&self, input: &str) -> Result<Vec<f32>> {
        let input = input.trim();

        if input.is_empty() {
            bail!("Cannot generate an embedding for an empty input");
        }

        let url = format!("{}/api/embed", self.base_url);

        let request = EmbedRequest {
            model: &self.model,
            input,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("Failed to send embedding request to {}", url))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("Failed to read Ollama embedding response")?;

        if !status.is_success() {
            bail!(
                "Ollama embedding request failed with HTTP {}: {}",
                status,
                body
            );
        }

        let parsed: EmbedResponse = serde_json::from_str(&body)
            .with_context(|| format!("Failed to parse Ollama embedding response: {}", body))?;

        let embedding = parsed
            .embeddings
            .into_iter()
            .next()
            .context("Ollama returned no embedding")?;

        if embedding.len() != 768 {
            bail!(
                "Unexpected embedding dimension: expected 768, got {}",
                embedding.len()
            );
        }

        Ok(embedding)
    }
}
