use std::sync::Arc;

use anyhow::Result;

use common::config::AppConfig;

use domain::events::event_envelope::EventEnvelope;

use kafka::KafkaConsumer;

use metrics::consumer_metrics;

use crate::{
    container::application_container::ApplicationContainer,
    dispatcher::event_dispatcher::EventDispatcher,
    router::event_version_router::EventVersionRouter,
    schema::event_schema_validator::EventSchemaValidator,
};

pub struct AuditConsumer {
    consumer: KafkaConsumer,

    config: AppConfig,

    dispatcher: Arc<EventDispatcher>,

    validator: Arc<EventSchemaValidator>,

    router: Arc<EventVersionRouter>,
}

impl AuditConsumer {
    pub async fn new() -> Result<Self> {
        let config = AppConfig::load();

        let schema_registry_url = std::env::var("SCHEMA_REGISTRY_URL")
            .unwrap_or_else(|_| "http://localhost:18081".to_string());

        let container = ApplicationContainer::build().await?;

        Ok(Self {
            consumer: KafkaConsumer::new(
                &config.kafka_brokers,
                "audit-group",
                &schema_registry_url,
            )?,

            config,

            dispatcher: container.dispatcher(),

            validator: Arc::new(EventSchemaValidator::new()),

            router: Arc::new(EventVersionRouter::new()),
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.consumer.subscribe(&self.config.kafka_topic)?;

        let dispatcher = self.dispatcher.clone();

        let validator = self.validator.clone();

        let router = self.router.clone();

        let consumer = &self.consumer;

        consumer
            .listen(move |event| {
                let dispatcher = dispatcher.clone();

                let validator = validator.clone();

                let router = router.clone();

                async move {
                    let json_value = consumer.decode_avro(&event.payload).await?;

                    let envelope = serde_json::from_value::<EventEnvelope>(json_value)?;

                    validator.validate(&envelope)?;

                    router.route(&envelope)?;

                    consumer_metrics::consumed();

                    dispatcher.dispatch(envelope).await?;

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }
}
