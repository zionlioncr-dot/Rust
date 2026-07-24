use anyhow::Result;

use domain::events::event_envelope::EventEnvelope;

use kafka::KafkaConsumer;

use crate::{config::Config, dispatcher::event_dispatcher::EventDispatcher};

pub struct AuditConsumer {
    consumer: KafkaConsumer,
    config: Config,
    dispatcher: EventDispatcher,
}

impl AuditConsumer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            consumer: KafkaConsumer::new("audit-group")?,

            config: Config::load(),

            dispatcher: EventDispatcher::new(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.consumer.subscribe(&self.config.topic)?;

        let dispatcher = &self.dispatcher;

        self.consumer
            .listen(|event| async move {
                let envelope = serde_json::from_str::<EventEnvelope>(&event.payload)?;

                dispatcher.dispatch(envelope).await?;

                Ok(())
            })
            .await?;

        Ok(())
    }
}
