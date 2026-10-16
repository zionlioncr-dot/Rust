use crate::domain::AmlAlert;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use repository_lib::RagRepository;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

const EMBEDDING_DIMENSION: usize = 768;

#[derive(Clone)]
pub struct RegulatoryRag {
    repository: RagRepository,
    client: Client,
    base_url: String,
    model: String,
    enabled: bool,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

impl RegulatoryRag {
    pub fn new(
        repository: RagRepository,
        base_url: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("No se pudo crear el cliente HTTP de Ollama para AML")?;

        let enabled = std::env::var("AML_RAG_ENABLED")
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        Ok(Self {
            repository,
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
            enabled,
        })
    }

    /// Añade evidencia normativa vigente en la fecha de la transacción.
    /// La recuperación no decide por sí sola si existe incumplimiento.
    pub async fn enrich_alert(
        &self,
        alert: &mut AmlAlert,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        if !self.enabled {
            set_status(&mut alert.details, "disabled");
            return Ok(());
        }

        let query = format!(
            "Costa Rica prevención de legitimación de capitales y financiamiento \
             al terrorismo. Regla: {}. Descripción: {}. Datos observados: {}",
            alert.rule_code, alert.description, alert.details
        );

        let embedding = self.embed(&query).await?;
        let evidence = self
            .repository
            .search_regulatory(&embedding, "CR", occurred_at.date_naive(), 3)
            .await?;

        let items = evidence
            .into_iter()
            .map(|item| {
                let metadata = item.metadata;
                json!({
                    "document_id": item.id,
                    "regulation_code": metadata.get("regulation_code"),
                    "article": metadata.get("article"),
                    "title": metadata.get("title"),
                    "document_version": metadata.get("document_version"),
                    "effective_from": metadata.get("effective_from"),
                    "effective_to": metadata.get("effective_to"),
                    "source_url": metadata.get("source_url"),
                    "jurisdiction": metadata.get("jurisdiction"),
                    "similarity": item.similarity,
                    "excerpt": item.content
                })
            })
            .collect::<Vec<Value>>();

        if let Some(details) = alert.details.as_object_mut() {
            details.insert(
                "regulatory_evidence".to_string(),
                Value::Array(items.clone()),
            );
            details.insert(
                "regulatory_evidence_status".to_string(),
                Value::String(if items.is_empty() {
                    "no_verified_current_evidence".to_string()
                } else {
                    "retrieved".to_string()
                }),
            );
        }

        Ok(())
    }

    pub fn mark_unavailable(&self, alert: &mut AmlAlert) {
        set_status(&mut alert.details, "retrieval_unavailable");
    }

    async fn embed(&self, input: &str) -> Result<Vec<f32>> {
        let response = self
            .client
            .post(format!("{}/api/embed", self.base_url))
            .json(&json!({
                "model": self.model,
                "input": input
            }))
            .send()
            .await
            .context("No se pudo consultar Ollama para generar el embedding AML")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("No se pudo leer la respuesta de embeddings de Ollama")?;

        if !status.is_success() {
            bail!(
                "Ollama respondió HTTP {} al generar embedding AML: {}",
                status,
                body
            );
        }

        let parsed: EmbedResponse =
            serde_json::from_str(&body).context("Respuesta de embeddings de Ollama inválida")?;

        let embedding = parsed
            .embeddings
            .into_iter()
            .next()
            .context("Ollama devolvió una lista de embeddings vacía")?;

        if embedding.len() != EMBEDDING_DIMENSION {
            bail!(
                "Dimensión de embedding inesperada: se esperaban {}, se recibieron {}",
                EMBEDDING_DIMENSION,
                embedding.len()
            );
        }

        Ok(embedding)
    }
}

fn set_status(details: &mut Value, status: &str) {
    if let Some(object) = details.as_object_mut() {
        object.insert(
            "regulatory_evidence_status".to_string(),
            Value::String(status.to_string()),
        );
        object
            .entry("regulatory_evidence".to_string())
            .or_insert_with(|| Value::Array(Vec::new()));
    }
}
