use anyhow::Result;

use common::config::AppConfig;

use kafka::KafkaProducer;

pub struct KafkaPublisher {
    producer: KafkaProducer,
}

impl KafkaPublisher {
    pub fn new(config: &AppConfig) -> Result<Self> {
        Ok(Self {
            producer: KafkaProducer::new(&config.kafka_brokers)?,
        })
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> Result<()> {
        self.producer.publish(topic, None, payload).await
    }
}
