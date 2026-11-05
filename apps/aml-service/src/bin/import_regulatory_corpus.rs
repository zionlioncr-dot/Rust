use anyhow::{bail, ensure, Context, Result};
use common::database::create_pool;
use repository_lib::{RagDocument, RagRepository};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};
use uuid::Uuid;

const EMBEDDING_DIMENSION: usize = 768;

#[derive(Deserialize)]
struct ManifestRecord {
    id: Uuid,
    source: String,
    content: String,
    metadata: Value,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let manifest = env::var("REGULATORY_MANIFEST")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("tools/regulatory-corpus/manifest/regulatory-corpus.jsonl")
        });

    let base_url = env::var("OLLAMA_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:11435".to_string())
        .trim_end_matches('/')
        .to_string();

    let model = env::var("OLLAMA_EMBEDDING_MODEL")
        .unwrap_or_else(|_| "nomic-embed-text:latest".to_string());

    let expected_count: usize = env::var("REGULATORY_EXPECTED_COUNT")
        .unwrap_or_else(|_| "129".to_string())
        .parse()
        .context("REGULATORY_EXPECTED_COUNT inválido")?;

    ensure!(manifest.is_file(), "No existe el manifiesto: {}", manifest.display());

    let file = File::open(&manifest)
        .await
        .with_context(|| format!("No se pudo abrir {}", manifest.display()))?;

    let mut reader = BufReader::new(file).lines();
    let mut records = Vec::new();
    let mut ids = HashSet::new();
    let mut line_number = 0usize;

    while let Some(line) = reader.next_line().await? {
        line_number += 1;

        if line.trim().is_empty() {
            continue;
        }

        let record: ManifestRecord = serde_json::from_str(&line)
            .with_context(|| format!("JSON inválido en línea {line_number}"))?;

        ensure!(!record.source.trim().is_empty(), "Fuente vacía en línea {line_number}");
        ensure!(!record.content.trim().is_empty(), "Contenido vacío en línea {line_number}");
        ensure!(
            ids.insert(record.id),
            "UUID duplicado en el manifiesto: {}",
            record.id
        );

        records.push(record);
    }

    ensure!(
        records.len() == expected_count,
        "Se esperaban {expected_count} registros, encontrados {}",
        records.len()
    );

    println!("Manifiesto validado: {} registros", records.len());
    println!("Modelo de embeddings: {model}");
    println!("Endpoint Ollama: {base_url}/api/embed");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;

    let pool = create_pool(3)
        .await
        .context("No se pudo conectar a PostgreSQL")?;

    sqlx::query("SELECT 1 FROM rag_documents LIMIT 1")
        .execute(&pool)
        .await
        .context("No se pudo comprobar la tabla rag_documents")?;

    let repository = RagRepository::new(pool.clone());
    let mut inserted = 0usize;

    for record in records.iter() {
        let response = client
            .post(format!("{base_url}/api/embed"))
            .json(&serde_json::json!({
                "model": model,
                "input": record.content
            }))
            .send()
            .await
            .with_context(|| format!("Error de Ollama para UUID {}", record.id))?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            bail!(
                "Ollama respondió HTTP {status} para UUID {}: {}",
                record.id,
                body.chars().take(500).collect::<String>()
            );
        }

        let parsed: EmbedResponse = serde_json::from_str(&body)
            .with_context(|| format!("Respuesta de embedding inválida para {}", record.id))?;

        ensure!(
            parsed.embeddings.len() == 1,
            "Se esperaba un embedding para {}, recibidos {}",
            record.id,
            parsed.embeddings.len()
        );

        let embedding = parsed.embeddings.into_iter().next().unwrap();

        ensure!(
            embedding.len() == EMBEDDING_DIMENSION,
            "UUID {}: dimensión esperada {}, recibida {}",
            record.id,
            EMBEDDING_DIMENSION,
            embedding.len()
        );

        ensure!(
            embedding.iter().all(|v| v.is_finite()),
            "Embedding con valores no finitos para {}",
            record.id
        );

        let document = RagDocument {
            id: record.id,
            source: record.source.clone(),
            content: record.content.clone(),
            metadata: record.metadata.clone(),
            embedding,
        };

        repository
            .insert_document(&document)
            .await
            .with_context(|| format!("Error insertando UUID {}", record.id))?;

        inserted += 1;

        if inserted % 10 == 0 || inserted == records.len() {
            println!("Progreso: {inserted}/{}", records.len());
        }

        // Evita saturar Ollama con solicitudes consecutivas.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let final_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rag_documents")
        .fetch_one(&pool)
        .await?;

    let matching_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM rag_documents
        WHERE id = ANY($1)
        "#,
    )
    .bind(records.iter().map(|r| r.id).collect::<Vec<Uuid>>())
    .fetch_one(&pool)
    .await?;

    println!("Importación terminada.");
    println!("Registros procesados: {inserted}");
    println!("Registros del manifiesto encontrados en PostgreSQL: {matching_count}");
    println!("Total de documentos en rag_documents: {final_count}");

    ensure!(
        matching_count == expected_count as i64,
        "La validación final no encontró todos los documentos importados"
    );

    Ok(())
}
