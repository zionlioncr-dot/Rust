use std::time::Duration;

use anyhow::{anyhow, Result};

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

use tracing::{error, info};

use crate::config::KafkaConfig;

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new() -> Result<Self> {
        let config = KafkaConfig::load();

        info!(

            brokers = %config.brokers,

            "Initializing Kafka producer"

        );

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .create()?;

        Ok(Self { producer })
    }

    pub async fn publish(&self, topic: &str, key: Option<&str>, payload: &str) -> Result<()> {
        let record = match key {
            Some(key) => FutureRecord::to(topic).key(key).payload(payload),

            None => FutureRecord::to(topic).payload(payload),
        };

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(err, message)| {
                error!(

                    topic = topic,

                    error = %err,

                    message = ?message,

                    "Kafka publish failed"

                );

                anyhow!(err)
            })?;

        info!(

            topic = topic,

            key = ?key,

            "Kafka message published"

        );

        Ok(())
    }
}
