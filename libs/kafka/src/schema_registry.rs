use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SchemaRegistryClient {
    base_url: String,
    client: Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaByIdResponse {
    pub schema: String,
}

impl SchemaRegistryClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    pub async fn get_latest_schema(&self, subject: &str) -> Result<SchemaResponse> {
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await
            .with_context(|| format!("Failed to connect to Schema Registry: {}", self.base_url))?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();

            anyhow::bail!("Schema Registry returned {}: {}", status, body);
        }

        let schema = response
            .json::<SchemaResponse>()
            .await
            .context("Failed to parse Schema Registry response")?;

        Ok(schema)
    }

    pub async fn get_schema_by_id(&self, schema_id: i32) -> Result<String> {
        let url = format!("{}/schemas/ids/{}", self.base_url, schema_id);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .send()
            .await
            .with_context(|| format!("Failed to connect to Schema Registry: {}", self.base_url))?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();

            anyhow::bail!(
                "Schema Registry returned {} for schema ID {}: {}",
                status,
                schema_id,
                body
            );
        }

        let schema = response
            .json::<SchemaByIdResponse>()
            .await
            .context("Failed to parse Schema Registry schema-by-id response")?;

        Ok(schema.schema)
    }
}
