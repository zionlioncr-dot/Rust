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
            consumer: KafkaConsumer::new(&config.kafka_brokers, group)?,
        })
    }

    pub fn consumer(&self) -> &KafkaConsumer {
        &self.consumer
    }
}
