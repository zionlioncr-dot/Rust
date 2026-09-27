use anyhow::Result;

use common::config::AppConfig;
use kafka::KafkaConsumer;

pub struct EventSubscriber {
    consumer: KafkaConsumer,
}

impl EventSubscriber {
    pub fn new(group: &str) -> Result<Self> {
        let config = AppConfig::load();

        Ok(Self {
            consumer: KafkaConsumer::new(
                &config.kafka_brokers,
                group,
                &config.schema_registry_url,
            )?,
        })
    }

    pub async fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer.subscribe(&[topic]).await
    }

    pub async fn listen<F, Fut>(&self, handler: F) -> Result<()>
    where
        F: FnMut(kafka::KafkaEvent) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.consumer.listen(handler).await
    }
}
