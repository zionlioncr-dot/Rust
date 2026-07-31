use anyhow::Result;

use common::config::AppConfig;

use kafka::KafkaProducer;

pub struct EventPublisher {
    producer: KafkaProducer,
}

impl EventPublisher {
    pub fn new() -> Result<Self> {
        let config = AppConfig::load();

        Ok(Self {
            producer: KafkaProducer::new(&config.kafka_brokers)?,
        })
    }

    pub async fn publish(&self, topic: &str, key: Option<&str>, payload: &str) -> Result<()> {
        self.producer.publish(topic, key, payload).await
    }
}
