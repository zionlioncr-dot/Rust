use anyhow::Result;

use common::config::AppConfig;

use kafka::KafkaProducer;

pub struct KafkaPublisher {
    producer: KafkaProducer,
}

impl KafkaPublisher {
    pub fn new(config: &AppConfig) -> Result<Self> {
        Ok(Self {
            producer: KafkaProducer::new(&config.kafka_brokers, &config.schema_registry_url)?,
        })
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> Result<()> {
        self.producer.publish(topic, None, payload).await
    }

    pub async fn publish_with_schema(
        &self,
        topic: &str,
        subject: &str,
        payload: &str,
        traceparent: Option<&str>,
    ) -> Result<()> {
        self.producer
            .publish_with_schema(topic, None, subject, payload, traceparent)
            .await
    }
}
