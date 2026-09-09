use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{Router, routing::get};

use common::{
    config::AppConfig,
    database::{create_pool, health_check},
};

#[derive(Debug, Default, Deserialize, schemars::JsonSchema)]
struct EmptyParams {}

use domain::outbox_event::OutboxEvent;

use kafka::{KafkaConsumer, SchemaRegistryClient};

use repository::{
    PostgresRepository, audit_repository::AuditRepository, outbox_repository::OutboxRepository,
};

use rmcp::{handler::server::wrapper::Parameters, schemars, tool, tool_handler, tool_router};

use serde::{Deserialize, Serialize};
use serde_json::json;

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

const MCP_HOST: &str = "0.0.0.0";
const MCP_PORT: u16 = 8000;

#[derive(Clone)]
struct McpServer {
    config: AppConfig,
    repository: Arc<PostgresRepository>,
    schema_registry: SchemaRegistryClient,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct LimitParams {
    #[schemars(description = "Maximum number of records to return")]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EventParams {
    #[schemars(description = "UUID of the audit event")]
    id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SchemaParams {
    #[schemars(description = "Schema Registry subject, for example AuditCreated-value")]
    subject: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    service: String,
    status: String,
    postgres: String,
    kafka: String,
    schema_registry: String,
}

impl McpServer {
    fn new(
        config: AppConfig,
        repository: Arc<PostgresRepository>,
        schema_registry: SchemaRegistryClient,
    ) -> Self {
        Self {
            config,
            repository,
            schema_registry,
        }
    }

    async fn postgres_health(&self) -> bool {
        match create_pool(self.config.max_db_connections).await {
            Ok(pool) => health_check(&pool).await.is_ok(),
            Err(error) => {
                error!(%error, "PostgreSQL health check failed");
                false
            }
        }
    }

    async fn kafka_health(&self) -> bool {
        match KafkaConsumer::new(
            &self.config.kafka_brokers,
            "mcp-health-check",
            &self.config.schema_registry_url,
        ) {
            Ok(_) => true,
            Err(error) => {
                error!(%error, "Kafka health check failed");
                false
            }
        }
    }

    async fn schema_registry_health(&self) -> bool {
        self.schema_registry
            .get_latest_schema("AuditCreated-value")
            .await
            .is_ok()
    }
}

#[tool_router]
impl McpServer {
    #[tool(
        name = "system.health",
        description = "Check the health of PostgreSQL, Redpanda/Kafka and Schema Registry"
    )]
    async fn system_health(&self, Parameters(_params): Parameters<EmptyParams>) -> String {
        let postgres = self.postgres_health().await;
        let kafka = self.kafka_health().await;
        let schema_registry = self.schema_registry_health().await;

        let healthy = postgres && kafka && schema_registry;

        let response = HealthResponse {
            service: "financial-intelligence-platform".to_string(),
            status: if healthy {
                "healthy".to_string()
            } else {
                "degraded".to_string()
            },
            postgres: if postgres {
                "up".to_string()
            } else {
                "down".to_string()
            },
            kafka: if kafka {
                "up".to_string()
            } else {
                "down".to_string()
            },
            schema_registry: if schema_registry {
                "up".to_string()
            } else {
                "down".to_string()
            },
        };

        serde_json::to_string_pretty(&response).unwrap_or_else(|error| {
            json!({
                "error": error.to_string()
            })
            .to_string()
        })
    }

    #[tool(
        name = "audit.list_events",
        description = "List audit events stored in PostgreSQL"
    )]
    async fn audit_list_events(&self, Parameters(params): Parameters<LimitParams>) -> String {
        let limit = params.limit.unwrap_or(20).clamp(1, 100);

        let events = match self.repository.find_all().await {
            Ok(events) => events,
            Err(error) => {
                return json!({
                    "error": error.to_string()
                })
                .to_string();
            }
        };

        let events: Vec<_> = events.into_iter().take(limit as usize).collect();

        serde_json::to_string_pretty(&events).unwrap_or_else(|error| {
            json!({
                "error": error.to_string()
            })
            .to_string()
        })
    }

    #[tool(
        name = "audit.get_event",
        description = "Get an audit event from PostgreSQL by UUID"
    )]
    async fn audit_get_event(&self, Parameters(params): Parameters<EventParams>) -> String {
        let id = match uuid::Uuid::parse_str(&params.id) {
            Ok(id) => id,
            Err(error) => {
                return json!({
                    "error": format!("Invalid UUID: {}", error)
                })
                .to_string();
            }
        };

        match self.repository.find_by_id(id).await {
            Ok(Some(event)) => serde_json::to_string_pretty(&event).unwrap_or_else(|error| {
                json!({
                    "error": error.to_string()
                })
                .to_string()
            }),

            Ok(None) => json!({
                "error": "Audit event not found",
                "id": params.id
            })
            .to_string(),

            Err(error) => json!({
                "error": error.to_string()
            })
            .to_string(),
        }
    }

    #[tool(
        name = "audit.get_pending_outbox",
        description = "List unpublished events currently waiting in the transactional outbox"
    )]
    async fn audit_get_pending_outbox(
        &self,
        Parameters(params): Parameters<LimitParams>,
    ) -> String {
        let limit = params.limit.unwrap_or(20).clamp(1, 100);

        let events: Vec<OutboxEvent> = match self.repository.find_unpublished(limit).await {
            Ok(events) => events,
            Err(error) => {
                return json!({
                    "error": error.to_string()
                })
                .to_string();
            }
        };

        serde_json::to_string_pretty(&events).unwrap_or_else(|error| {
            json!({
                "error": error.to_string()
            })
            .to_string()
        })
    }

    #[tool(name = "kafka.list_topics", description = "List Kafka/Redpanda topics")]
    async fn kafka_list_topics(&self, Parameters(_params): Parameters<EmptyParams>) -> String {
        /*
         * KafkaConsumer encapsula actualmente el StreamConsumer.
         *
         * En esta primera versión utilizamos la existencia del
         * KafkaConsumer como validación de conectividad.
         *
         * La enumeración detallada de topics la agregaremos como
         * capacidad específica de Kafka en la siguiente iteración,
         * evitando romper la abstracción actual de libs/kafka.
         */
        match KafkaConsumer::new(
            &self.config.kafka_brokers,
            "mcp-topic-inspector",
            &self.config.schema_registry_url,
        ) {
            Ok(_) => json!({
                "status": "connected",
                "broker": self.config.kafka_brokers,
                "configured_topic": self.config.kafka_topic
            })
            .to_string(),

            Err(error) => json!({
                "status": "error",
                "error": error.to_string()
            })
            .to_string(),
        }
    }

    #[tool(
        name = "schema.get_latest",
        description = "Get the latest Avro schema registered for a Schema Registry subject"
    )]
    async fn schema_get_latest(&self, Parameters(params): Parameters<SchemaParams>) -> String {
        let subject = params.subject.trim();

        if subject.is_empty() {
            return json!({
                "error": "Schema subject cannot be empty"
            })
            .to_string();
        }

        match self.schema_registry.get_latest_schema(subject).await {
            Ok(schema) => serde_json::to_string_pretty(&schema).unwrap_or_else(|error| {
                json!({
                    "error": error.to_string()
                })
                .to_string()
            }),

            Err(error) => json!({
                "error": error.to_string(),
                "subject": subject
            })
            .to_string(),
        }
    }
}

#[tool_handler(
    name = "financial-intelligence-platform-mcp",
    version = "0.1.0",
    instructions = "MCP server for the Financial Intelligence Platform. Provides controlled access to audit events, transactional outbox state, Kafka/Redpanda connectivity and Avro schemas."
)]
impl rmcp::ServerHandler for McpServer {}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("Starting financial-intelligence-platform MCP server");

    let config = AppConfig::load();

    let registered_tools = McpServer::tool_router()
        .list_all()
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect::<Vec<_>>();

    info!(
        tools = ?registered_tools,
        "Registered MCP tools"
    );

    info!(
        kafka_brokers = %config.kafka_brokers,
        kafka_topic = %config.kafka_topic,
        schema_registry = %config.schema_registry_url,
        max_db_connections = config.max_db_connections,
        "Loaded application configuration"
    );

    let pool = create_pool(config.max_db_connections)
        .await
        .context("Failed to create PostgreSQL connection pool")?;

    health_check(&pool)
        .await
        .context("PostgreSQL health check failed")?;

    info!("PostgreSQL connection established");

    let repository = Arc::new(PostgresRepository::new(pool));

    let schema_registry = SchemaRegistryClient::new(&config.schema_registry_url);

    let server = McpServer::new(config, repository, schema_registry);

    let http_config = StreamableHttpServerConfig::default().with_json_response(true);

    let mcp_service = StreamableHttpService::new(
        move || Ok(server.clone()),
        LocalSessionManager::default().into(),
        http_config,
    );

    let router = Router::new()
        .route("/health", get(health))
        .nest_service("/mcp", mcp_service);

    let address: SocketAddr = format!("{}:{}", MCP_HOST, MCP_PORT)
        .parse()
        .context("Invalid MCP server address")?;

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .context("Failed to bind MCP server")?;

    info!(
        address = %address,
        "MCP server listening"
    );

    axum::serve(listener, router)
        .await
        .context("MCP server stopped unexpectedly")?;

    Ok(())
}
