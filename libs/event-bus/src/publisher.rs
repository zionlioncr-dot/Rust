use anyhow::Result;

use kafka::KafkaProducer;

use crate::EventEnvelope;

pub struct EventPublisher {
    producer: KafkaProducer,
}

impl EventPublisher {
    pub fn new() -> Result<Self> {
        Ok(Self {
            producer: KafkaProducer::new()?,
        })
    }

    pub async fn publish(&self, topic: &str, event: &EventEnvelope) -> Result<()> {
        let payload = serde_json::to_string(event)?;

        self.producer
            .publish(topic, Some(&event.id.to_string()), &payload)
            .await
    }
}
