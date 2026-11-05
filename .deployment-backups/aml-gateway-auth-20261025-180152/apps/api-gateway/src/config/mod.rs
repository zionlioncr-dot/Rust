use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub server_addr: String,

    pub audit_service_url: String,

    pub anti_fraud_service_url: String,

    pub jwt_secret: String,

    pub jwt_issuer: String,

    pub jwt_audience: String,

    pub max_db_connections: u32,

    pub redis_url: String,
    pub bank_ip_allowlist_positive_ttl_secs: u64,
    pub bank_ip_allowlist_negative_ttl_secs: u64,
}

impl Config {
    pub fn load() -> Self {
        let _ = dotenvy::dotenv();

        Self {
            server_addr: env::var("SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:8088".to_string()),

            audit_service_url: env::var("AUDIT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:3000".to_string()),

            anti_fraud_service_url: env::var("ANTI_FRAUD_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:3002".to_string()),

            jwt_secret: env::var("JWT_SECRET")
                .unwrap_or_else(|_| "development-secret-change-me".to_string()),

            jwt_issuer: env::var("JWT_ISSUER")
                .unwrap_or_else(|_| "financial-intelligence-platform".to_string()),

            jwt_audience: env::var("JWT_AUDIENCE").unwrap_or_else(|_| "financial-api".to_string()),

            max_db_connections: env::var("MAX_DB_CONNECTIONS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(10),

            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379/".to_string()),

            bank_ip_allowlist_positive_ttl_secs: env::var("BANK_IP_ALLOWLIST_POSITIVE_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(60),

            bank_ip_allowlist_negative_ttl_secs: env::var("BANK_IP_ALLOWLIST_NEGATIVE_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(15),
        }
    }
}
