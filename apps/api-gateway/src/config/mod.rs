use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub server_addr: String,
    pub audit_service_url: String,
    pub jwt_secret: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
}

impl Config {
    pub fn load() -> Self {
        Self {
            server_addr: env::var("SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:8088".to_string()),

            audit_service_url: env::var("AUDIT_SERVICE_URL")
                .unwrap_or_else(|_| "http://localhost:3000".to_string()),

            jwt_secret: env::var("JWT_SECRET")
                .unwrap_or_else(|_| "development-secret-change-me".to_string()),

            jwt_issuer: env::var("JWT_ISSUER")
                .unwrap_or_else(|_| "financial-intelligence-platform".to_string()),

            jwt_audience: env::var("JWT_AUDIENCE").unwrap_or_else(|_| "financial-api".to_string()),
        }
    }
}
