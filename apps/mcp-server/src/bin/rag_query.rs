use std::time::Duration;

use anyhow::{Context, Result};
use common::{config::AppConfig, database::create_pool};
use repository::RagRepository;
use reqwest::Client;
use serde::{Deserialize, Serialize};

const EMBEDDING_DIMENSION: usize = 768;

#[derive(Debug, Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

#[derive(Clone)]
struct OllamaEmbeddingClient {
    client: Client,
    base_url: String,
    model: String,
}

impl OllamaEmbeddingClient {
    fn new(base_url: impl Into<String>, model: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("Failed to create Ollama HTTP client")?;

        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
        })
    }

    async fn embed(&self, input: &str) -> Result<Vec<f32>> {
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
            .with_context(|| format!("Failed to call Ollama at {}", url))?;

        let status = response.status();

        let body = response
            .text()
            .await
            .context("Failed to read Ollama response")?;

        if !status.is_success() {
            anyhow::bail!("Ollama returned HTTP {}: {}", status, body);
        }

        let parsed: EmbedResponse =
            serde_json::from_str(&body).context("Failed to parse Ollama embedding response")?;

        let embedding = parsed
            .embeddings
            .into_iter()
            .next()
            .context("Ollama returned no embedding")?;

        if embedding.len() != EMBEDDING_DIMENSION {
            anyhow::bail!(
                "Expected {} dimensions, got {}",
                EMBEDDING_DIMENSION,
                embedding.len()
            );
        }

        Ok(embedding)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let query = std::env::args().skip(1).collect::<Vec<_>>().join(" ");

    if query.trim().is_empty() {
        anyhow::bail!("Usage: cargo run -p mcp-server --bin rag_query -- \"your question\"");
    }

    let config = AppConfig::load();

    println!("========================================");
    println!(" RAG QUERY");
    println!("========================================");
    println!("Query: {}", query);
    println!("Embedding model: {}", config.ollama_embedding_model);
    println!("Ollama: {}", config.ollama_base_url);

    let pool = create_pool(config.max_db_connections)
        .await
        .context("Failed to create PostgreSQL pool")?;

    let repository = RagRepository::new(pool);

    let embedding_client =
        OllamaEmbeddingClient::new(&config.ollama_base_url, &config.ollama_embedding_model)?;

    println!("Generating query embedding...");

    let embedding = embedding_client
        .embed(&query)
        .await
        .context("Failed to generate query embedding")?;

    println!("Embedding generated: {} dimensions", embedding.len());

    println!("Searching pgvector...");

    let results = repository
        .search_hybrid(&embedding, &query, 5)
        .await
        .context("Failed to execute vector search")?;

    println!();
    println!("========================================");
    println!(" TOP {} RESULTS", results.len());
    println!("========================================");

    for (index, result) in results.iter().enumerate() {
        println!();
        println!("#{} similarity={:.6}", index + 1, result.similarity);
        println!("source: {}", result.source);
        println!("metadata: {}", result.metadata);

        let preview = result.content.chars().take(500).collect::<String>();

        println!("content:");
        println!("{}", preview);

        if result.content.chars().count() > 500 {
            println!("...[truncated]");
        }
    }

    println!();
    println!("========================================");

    Ok(())
}
