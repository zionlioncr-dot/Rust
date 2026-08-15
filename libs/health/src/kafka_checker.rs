use std::time::Duration;

use anyhow::{anyhow, Result};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig,
};

use crate::checker::HealthCheck;

pub struct KafkaChecker {
    brokers: String,
}

impl KafkaChecker {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
        }
    }
}

#[async_trait::async_trait]
impl HealthCheck for KafkaChecker {
    fn name(&self) -> &'static str {
        "kafka"
    }

    async fn check(&self) -> Result<()> {
        let brokers = self.brokers.clone();

        tokio::task::spawn_blocking(move || -> Result<()> {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("group.id", "health-check")
                .set("socket.timeout.ms", "2000")
                .create()
                .map_err(|err| anyhow!("Kafka client creation failed: {err}"))?;

            consumer
                .fetch_metadata(None, Duration::from_secs(2))
                .map_err(|err| anyhow!("Kafka health check failed: {err}"))?;

            Ok(())
        })
        .await??;

        Ok(())
    }
}
