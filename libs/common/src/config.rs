use std::env;

/// Configuración compartida de toda la plataforma.
///
/// Todas las aplicaciones (API, Workers, Consumers)
/// deben utilizar esta estructura para acceder
/// a las variables de entorno.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,

    pub kafka_brokers: String,

    pub kafka_topic: String,

    pub max_db_connections: u32,

    pub server_port: u16,

    pub polling_interval: u64,
}

impl AppConfig {
    pub fn load() -> Self {
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

        Self {
            database_url,
            kafka_brokers,
            kafka_topic,
            max_db_connections,
            server_port,
            polling_interval,
        }
    }
}
