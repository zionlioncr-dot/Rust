use anyhow::Result;

use futures_util::StreamExt;

use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Message,
    ClientConfig,
};

use crate::{config::KafkaConfig, event::KafkaEvent};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
}

impl KafkaConsumer {
    pub fn new(group: &str) -> Result<Self> {
        let config = KafkaConfig::load();

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

        Ok(Self { consumer })
    }

    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer.subscribe(&[topic])?;

        Ok(())
    }

    pub async fn listen<F, Fut>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(KafkaEvent) -> Fut,

        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut stream = self.consumer.stream();

        while let Some(message) = stream.next().await {
            let message = message?;

            let payload = match message.payload_view::<str>() {
                Some(Ok(value)) => value.to_string(),

                Some(Err(_)) => String::new(),

                None => String::new(),
            };

            let key = match message.key_view::<str>() {
                Some(Ok(value)) => Some(value.to_string()),

                Some(Err(_)) => None,

                None => None,
            };

            handler(KafkaEvent { key, payload }).await?;
        }

        Ok(())
    }
}
