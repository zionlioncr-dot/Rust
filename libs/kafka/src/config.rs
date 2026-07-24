use std::env;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
}

impl KafkaConfig {
    pub fn load() -> Self {
        Self {
            brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string()),
        }
    }
}
