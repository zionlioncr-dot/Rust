use std::env;

/// Configuración compartida de toda la plataforma.
///
/// Todas las aplicaciones (API, Workers, Consumers, MCP)
/// deben utilizar esta estructura para acceder a las variables
/// de entorno.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,

    pub kafka_brokers: String,

    pub kafka_topic: String,

    pub max_db_connections: u32,

    pub server_port: u16,

    pub polling_interval: u64,

    pub schema_registry_url: String,

    pub ollama_base_url: String,

    pub ollama_embedding_model: String,
}

impl AppConfig {
    pub fn load() -> Self {
        // Carga el .env de la raíz del workspace si existe.
        //
        // No hacemos panic si no existe porque en Docker/Kubernetes
        // las variables normalmente serán proporcionadas directamente
        // por el entorno del proceso.
        let _ = dotenvy::dotenv();

        let schema_registry_url = env::var("SCHEMA_REGISTRY_URL")
            .unwrap_or_else(|_| "http://localhost:18081".to_string());

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let kafka_brokers =
            env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

        println!("AppConfig kafka_brokers = {}", kafka_brokers);

        let kafka_topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "audit-events".to_string());

        let max_db_connections = env::var("MAX_DB_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let server_port = env::var("PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3000);

        let polling_interval = env::var("POLLING_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        let ollama_base_url =
            env::var("OLLAMA_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:11435".to_string());

        let ollama_embedding_model =
            env::var("OLLAMA_EMBEDDING_MODEL").unwrap_or_else(|_| "nomic-embed-text".to_string());

        Self {
            database_url,
            kafka_brokers,
            kafka_topic,
            schema_registry_url,
            max_db_connections,
            server_port,
            polling_interval,
            ollama_base_url,
            ollama_embedding_model,
        }
    }
}
