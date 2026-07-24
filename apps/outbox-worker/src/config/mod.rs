use std::env;

pub struct Config {
    pub polling_interval: u64,
    pub kafka_topic: String,
}

impl Config {
    pub fn load() -> Self {
        Self {
            polling_interval: env::var("POLLING_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),

            kafka_topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "audit-events".to_string()),
        }
    }
}
