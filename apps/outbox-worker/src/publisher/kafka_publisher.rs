use anyhow::Result;

use kafka::KafkaProducer;

pub struct KafkaPublisher {
    producer: KafkaProducer,
}

impl KafkaPublisher {
    pub fn new() -> Result<Self> {
        Ok(Self {
            producer: KafkaProducer::new()?,
        })
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> Result<()> {
        self.producer.publish(topic, None, payload).await
    }
}
