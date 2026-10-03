use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use common::{config::AppConfig, database::create_pool};
use repository::{RagDocument, RagRepository};
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

const CHUNK_LINES: usize = 80;
const OVERLAP_LINES: usize = 15;
const EMBEDDING_DIMENSION: usize = 768;

#[derive(Debug, Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
}

#[derive(Debug, serde::Deserialize)]
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
        let input = input.trim();

        if input.is_empty() {
            bail!("Cannot generate an embedding for empty input");
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

        if embedding.len() != EMBEDDING_DIMENSION {
            bail!(
                "Unexpected embedding dimension: expected {}, got {}",
                EMBEDDING_DIMENSION,
                embedding.len()
            );
        }

        Ok(embedding)
    }
}

#[derive(Debug, Clone)]
struct CorpusFile {
    path: &'static str,
    component: &'static str,
    layer: &'static str,
    topic: &'static str,
}

const CORPUS: &[CorpusFile] = &[
    CorpusFile {
        path: "README.md",
        component: "platform",
        layer: "documentation",
        topic: "architecture",
    },
    CorpusFile {
        path: "apps/audit-service/src/service/audit_service.rs",
        component: "audit-service",
        layer: "service",
        topic: "audit-created",
    },
    CorpusFile {
        path: "apps/audit-service/src/builders/event_envelope_builder.rs",
        component: "audit-service",
        layer: "builder",
        topic: "event-envelope",
    },
    CorpusFile {
        path: "apps/audit-service/src/builders/outbox_builder.rs",
        component: "audit-service",
        layer: "builder",
        topic: "audit-created",
    },
    CorpusFile {
        path: "apps/outbox-worker/src/worker/outbox_worker.rs",
        component: "outbox-worker",
        layer: "worker",
        topic: "outbox",
    },
    CorpusFile {
        path: "apps/outbox-worker/src/publisher/kafka_publisher.rs",
        component: "outbox-worker",
        layer: "publisher",
        topic: "kafka",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/consumer/audit_consumer.rs",
        component: "audit-consumer",
        layer: "consumer",
        topic: "kafka",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/consumer/worker.rs",
        component: "audit-consumer",
        layer: "worker",
        topic: "dispatch",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/dispatcher/event_dispatcher.rs",
        component: "audit-consumer",
        layer: "dispatcher",
        topic: "events",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/handler/audit_handler.rs",
        component: "audit-consumer",
        layer: "handler",
        topic: "audit-created",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/service/audit_processing_service.rs",
        component: "audit-consumer",
        layer: "service",
        topic: "audit-processing",
    },
    CorpusFile {
        path: "apps/audit-consumer/src/service/idempotency_service.rs",
        component: "audit-consumer",
        layer: "service",
        topic: "idempotency",
    },
    CorpusFile {
        path: "libs/domain/src/events/audit_created.rs",
        component: "domain",
        layer: "event",
        topic: "audit-created",
    },
    CorpusFile {
        path: "libs/domain/src/events/event_envelope.rs",
        component: "domain",
        layer: "event",
        topic: "event-envelope",
    },
    CorpusFile {
        path: "libs/domain/src/events/event_metadata.rs",
        component: "domain",
        layer: "event",
        topic: "event-metadata",
    },
    CorpusFile {
        path: "libs/domain/src/events/event_version.rs",
        component: "domain",
        layer: "event",
        topic: "event-version",
    },
    CorpusFile {
        path: "libs/repository/src/postgres/outbox_repository.rs",
        component: "repository",
        layer: "postgres",
        topic: "outbox",
    },
    CorpusFile {
        path: "libs/repository/src/postgres/audit_repository.rs",
        component: "repository",
        layer: "postgres",
        topic: "audit",
    },
    CorpusFile {
        path: "libs/kafka/src/producer.rs",
        component: "kafka",
        layer: "producer",
        topic: "avro",
    },
    CorpusFile {
        path: "libs/kafka/src/consumer.rs",
        component: "kafka",
        layer: "consumer",
        topic: "avro",
    },
    CorpusFile {
        path: "libs/kafka/src/schema_registry.rs",
        component: "kafka",
        layer: "schema-registry",
        topic: "avro",
    },
    CorpusFile {
        path: "migrations/0001_create_audit_events.sql",
        component: "database",
        layer: "migration",
        topic: "audit-events",
    },
    CorpusFile {
        path: "migrations/0002_create_outbox_events.sql",
        component: "database",
        layer: "migration",
        topic: "outbox",
    },
    CorpusFile {
        path: "migrations/0004_processed_events.sql",
        component: "database",
        layer: "migration",
        topic: "idempotency",
    },
    CorpusFile {
        path: "migrations/0008_create_rag_documents.sql",
        component: "database",
        layer: "migration",
        topic: "rag",
    },
    CorpusFile {
        path: "k8s/audit-service/audit-service.yaml",
        component: "kubernetes",
        layer: "deployment",
        topic: "audit-service",
    },
    CorpusFile {
        path: "k8s/outbox-worker/outbox-worker.yaml",
        component: "kubernetes",
        layer: "deployment",
        topic: "outbox-worker",
    },
    CorpusFile {
        path: "k8s/audit-consumer/audit-consumer.yaml",
        component: "kubernetes",
        layer: "deployment",
        topic: "audit-consumer",
    },
    CorpusFile {
        path: "k8s/ollama/ollama.yaml",
        component: "kubernetes",
        layer: "deployment",
        topic: "ollama",
    },
];

fn repository_root() -> Result<PathBuf> {
    env::current_dir().context("Failed to determine current working directory")
}

fn read_file(root: &Path, corpus_file: &CorpusFile) -> Result<String> {
    let path = root.join(corpus_file.path);

    fs::read_to_string(&path)
        .with_context(|| format!("Failed to read corpus file {}", corpus_file.path))
}

fn chunk_lines(content: &str) -> Vec<String> {
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < lines.len() {
        let end = (start + CHUNK_LINES).min(lines.len());

        let chunk = lines[start..end].join("\n");

        if !chunk.trim().is_empty() {
            chunks.push(chunk);
        }

        if end == lines.len() {
            break;
        }

        start = end.saturating_sub(OVERLAP_LINES);
    }

    chunks
}

fn deterministic_id(source: &str, chunk_index: usize) -> Uuid {
    let key = format!("rag://{}#{}", source, chunk_index);

    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes())
}

async fn ingest_file(
    root: &Path,
    rag_repository: &RagRepository,
    embedding_client: &OllamaEmbeddingClient,
    corpus_file: &CorpusFile,
) -> Result<usize> {
    let content = read_file(root, corpus_file)?;

    let chunks = chunk_lines(&content);

    println!("SOURCE {} -> {} chunks", corpus_file.path, chunks.len());

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        println!("  embedding chunk {}/{}", chunk_index + 1, chunks.len());

        let embedding = embedding_client.embed(chunk).await.with_context(|| {
            format!("Failed to embed {} chunk {}", corpus_file.path, chunk_index)
        })?;

        let id = deterministic_id(corpus_file.path, chunk_index);

        let metadata = json!({
            "path": corpus_file.path,
            "component": corpus_file.component,
            "layer": corpus_file.layer,
            "topic": corpus_file.topic,
            "chunk_index": chunk_index,
            "chunk_lines": CHUNK_LINES,
            "overlap_lines": OVERLAP_LINES
        });

        let document = RagDocument {
            id,
            source: corpus_file.path.to_string(),
            content: chunk.clone(),
            metadata,
            embedding,
        };

        rag_repository
            .insert_document(&document)
            .await
            .with_context(|| {
                format!("Failed to store {} chunk {}", corpus_file.path, chunk_index)
            })?;
    }

    Ok(chunks.len())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    println!("========================================");
    println!(" RAG INGEST");
    println!("========================================");

    let config = AppConfig::load();

    println!("DATABASE_URL={}", config.database_url);

    println!("OLLAMA_BASE_URL={}", config.ollama_base_url);

    println!("OLLAMA_EMBEDDING_MODEL={}", config.ollama_embedding_model);

    let root = repository_root()?;

    println!("Repository root: {}", root.display());

    let pool = create_pool(config.max_db_connections)
        .await
        .context("Failed to create PostgreSQL pool")?;

    println!("PostgreSQL connection established");

    let rag_repository = RagRepository::new(pool);

    let embedding_client =
        OllamaEmbeddingClient::new(&config.ollama_base_url, &config.ollama_embedding_model)?;

    println!("Embedding model ready: {}", config.ollama_embedding_model);

    let mut total_chunks = 0usize;

    for corpus_file in CORPUS {
        let count = ingest_file(&root, &rag_repository, &embedding_client, corpus_file).await?;

        total_chunks += count;
    }

    let stored = rag_repository
        .count()
        .await
        .context("Failed to count RAG documents")?;

    println!("========================================");
    println!(" RAG INGEST COMPLETE");
    println!("========================================");
    println!("Chunks processed: {}", total_chunks);
    println!("Documents stored: {}", stored);

    Ok(())
}
