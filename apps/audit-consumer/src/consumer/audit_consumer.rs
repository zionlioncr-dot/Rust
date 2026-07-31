use std::sync::Arc;

use anyhow::Result;

use common::config::AppConfig;

use domain::events::event_envelope::EventEnvelope;

use kafka::KafkaConsumer;

use crate::{
    container::application_container::ApplicationContainer,
    dispatcher::event_dispatcher::EventDispatcher,
};

pub struct AuditConsumer {
    consumer: KafkaConsumer,
    config: AppConfig,
    dispatcher: Arc<EventDispatcher>,
}

impl AuditConsumer {
    pub async fn new() -> Result<Self> {
        let config = AppConfig::load();

        let container = ApplicationContainer::build().await?;

        Ok(Self {
            consumer: KafkaConsumer::new(&config.kafka_brokers, "audit-group")?,
            config,
            dispatcher: container.dispatcher(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.consumer.subscribe(&self.config.kafka_topic)?;

        let dispatcher = self.dispatcher.clone();

        self.consumer
            .listen(move |event| {
                let dispatcher = dispatcher.clone();

                async move {
                    let envelope = serde_json::from_str::<EventEnvelope>(&event.payload)?;

                    dispatcher.dispatch(envelope).await?;

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }
}
