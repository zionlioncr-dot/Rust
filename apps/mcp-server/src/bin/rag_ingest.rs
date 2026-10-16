use std::{collections::HashSet, fs, time::Duration};

use anyhow::{Context, Result, bail};
use common::{config::AppConfig, database::create_pool};
use repository::{RagDocument, RagRepository};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

const MANIFEST_PATH: &str = "tools/regulatory-corpus/manifest/regulatory-corpus.jsonl";
const EMBEDDING_DIMENSION: usize = 768;
const EXPECTED_TOTAL: usize = 129;

#[derive(Debug, Deserialize)]
struct CorpusRecord {
    id: Uuid,
    source: String,
    content: String,
    metadata: Value,
}

#[derive(Debug, serde::Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a str,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

struct EmbeddingClient {
    client: Client,
    base_url: String,
    model: String,
}

impl EmbeddingClient {
    fn new(base_url: &str, model: &str) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .context("No se pudo crear el cliente HTTP de Ollama")?,
            base_url: base_url.trim_end_matches('/').to_string(),
            model: model.to_string(),
        })
    }

    async fn embed(&self, content: &str) -> Result<Vec<f32>> {
        if content.trim().is_empty() {
            bail!("No se puede generar un embedding para contenido vacío");
        }

        let response = self
            .client
            .post(format!("{}/api/embed", self.base_url))
            .json(&EmbedRequest {
                model: &self.model,
                input: content,
            })
            .send()
            .await
            .context("Error al solicitar el embedding a Ollama")?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            bail!("Ollama respondió HTTP {}: {}", status, body);
        }

        let parsed: EmbedResponse =
            serde_json::from_str(&body).context("Respuesta de embedding inválida")?;

        if parsed.embeddings.len() != 1 {
            bail!(
                "Se esperaba un embedding y Ollama devolvió {}",
                parsed.embeddings.len()
            );
        }

        let embedding = parsed.embeddings.into_iter().next().unwrap();

        if embedding.len() != EMBEDDING_DIMENSION {
            bail!(
                "Dimensión incorrecta: esperada {}, recibida {}",
                EMBEDDING_DIMENSION,
                embedding.len()
            );
        }

        if embedding.iter().any(|v| !v.is_finite()) {
            bail!("Ollama devolvió valores no finitos en el embedding");
        }

        Ok(embedding)
    }
}

fn read_and_validate_manifest() -> Result<Vec<CorpusRecord>> {
    let contents = fs::read_to_string(MANIFEST_PATH)
        .with_context(|| format!("No se pudo leer {}", MANIFEST_PATH))?;

    let mut records = Vec::new();
    let mut ids = HashSet::new();
    let mut counts = std::collections::BTreeMap::<String, usize>::new();

    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let record: CorpusRecord = serde_json::from_str(line)
            .with_context(|| format!("JSON inválido en línea {}", index + 1))?;

        let metadata = &record.metadata;
        let code = metadata
            .get("regulation_code")
            .and_then(Value::as_str)
            .context("Falta metadata.regulation_code")?;

        if metadata.get("document_type").and_then(Value::as_str) != Some("regulation") {
            bail!("Registro {} no identificado como regulación", index + 1);
        }

        if metadata.get("jurisdiction").and_then(Value::as_str) != Some("CR") {
            bail!("Jurisdicción incorrecta en el registro {}", index + 1);
        }

        if metadata.get("verified").and_then(Value::as_bool) != Some(false) {
            bail!(
                "El registro {} debe permanecer con verified=false",
                index + 1
            );
        }

        if !metadata.get("effective_from").map_or(false, Value::is_null)
            || !metadata.get("effective_to").map_or(false, Value::is_null)
        {
            bail!(
                "El registro {} tiene fechas de vigencia no nulas; requiere revisión",
                index + 1
            );
        }

        let source_url = metadata
            .get("source_url")
            .and_then(Value::as_str)
            .context("Falta metadata.source_url")?;

        if !source_url.starts_with("https://") || record.source != source_url {
            bail!(
                "URL de fuente inválida o inconsistente en el registro {}",
                index + 1
            );
        }

        if record.content.trim().is_empty() {
            bail!("Contenido vacío en el registro {}", index + 1);
        }

        if !ids.insert(record.id) {
            bail!("UUID duplicado en el manifiesto: {}", record.id);
        }

        *counts.entry(code.to_string()).or_default() += 1;
        records.push(record);
    }

    if records.len() != EXPECTED_TOTAL {
        bail!(
            "Se esperaban {} fragmentos; el manifiesto contiene {}",
            EXPECTED_TOTAL,
            records.len()
        );
    }

    let expected = [
        ("CONASSIF 12-21", 61usize),
        ("SUGEF 11-18", 25usize),
        ("SUGEF 13-19", 43usize),
    ];

    for (code, count) in expected {
        let actual = counts.get(code).copied().unwrap_or(0);
        if actual != count {
            bail!(
                "Conteo incorrecto para {}: esperado {}, recibido {}",
                code,
                count,
                actual
            );
        }
    }

    if counts.len() != expected.len() {
        bail!("El manifiesto contiene regulaciones no previstas");
    }

    println!("Manifiesto validado: {} fragmentos", records.len());
    for (code, count) in counts {
        println!("  {}: {}", code, count);
    }

    Ok(records)
}

async fn remove_technical_documents(pool: &PgPool) -> Result<u64> {
    // El indexador técnico anterior almacenaba metadata.path.
    // Se eliminan solo documentos con esa marca que no sean regulaciones.
    let result = sqlx::query(
        r#"
        DELETE FROM rag_documents
        WHERE metadata ? 'path'
          AND metadata->>'document_type' IS DISTINCT FROM 'regulation'
        "#,
    )
    .execute(pool)
    .await
    .context("No se pudieron eliminar los documentos técnicos anteriores")?;

    Ok(result.rows_affected())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    println!("=== INGESTA DEL CORPUS REGULATORIO DE COSTA RICA ===");

    // Validar todo el manifiesto antes de contactar PostgreSQL u Ollama.
    let records = read_and_validate_manifest()?;
    let config = AppConfig::load();

    println!("Modelo de embeddings: {}", config.ollama_embedding_model);
    println!("Ollama: {}", config.ollama_base_url);

    let pool = create_pool(config.max_db_connections)
        .await
        .context("No se pudo conectar a PostgreSQL")?;

    let repository = RagRepository::new(pool.clone());
    let embedding_client =
        EmbeddingClient::new(&config.ollama_base_url, &config.ollama_embedding_model)?;

    let mut inserted = 0usize;

    // No borrar documentación anterior hasta que todos los documentos
    // regulatorios hayan sido generados y guardados correctamente.
    for (index, record) in records.iter().enumerate() {
        println!(
            "[{}/{}] {} artículo {} — {}",
            index + 1,
            records.len(),
            record.metadata["regulation_code"],
            record.metadata["article"],
            record.metadata["title"]
        );

        let embedding = embedding_client
            .embed(&record.content)
            .await
            .with_context(|| format!("Falló el embedding del documento {}", record.id))?;

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
            .with_context(|| format!("Falló el upsert del documento {}", record.id))?;

        inserted += 1;
    }

    // Verificación mínima antes de la limpieza.
    for record in &records {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM rag_documents WHERE id = $1)")
                .bind(record.id)
                .fetch_one(&pool)
                .await?;

        if !exists {
            bail!(
                "Falta el documento {} en PostgreSQL; no se limpiará el corpus técnico",
                record.id
            );
        }
    }

    let deleted = remove_technical_documents(&pool).await?;

    println!("=== INGESTA COMPLETADA ===");
    println!("Documentos regulatorios procesados: {}", inserted);
    println!("Documentos técnicos eliminados: {}", deleted);
    println!("Verificación legal: pendiente");
    println!("verified=true: ninguno");
    println!("Fechas de vigencia establecidas: ninguna");

    pool.close().await;
    Ok(())
}
