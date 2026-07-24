use std::env;

pub struct Config {
    pub topic: String,
}

impl Config {
    pub fn load() -> Self {
        Self {
            topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "audit-events".to_string()),
        }
    }
}
