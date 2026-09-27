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
        tracing::info!("AUDIT_CONSUMER_INIT: loading AppConfig");

        let config = AppConfig::load();

        tracing::info!("AUDIT_CONSUMER_INIT: AppConfig loaded");

        let schema_registry_url = std::env::var("SCHEMA_REGISTRY_URL")
            .unwrap_or_else(|_| "http://localhost:18081".to_string());

        tracing::info!(
            schema_registry_url = %schema_registry_url,
            "AUDIT_CONSUMER_INIT: Schema Registry URL resolved"
        );

        tracing::info!("AUDIT_CONSUMER_INIT: building ApplicationContainer");

        let container = ApplicationContainer::build().await?;

        tracing::info!("AUDIT_CONSUMER_INIT: ApplicationContainer built");

        tracing::info!("AUDIT_CONSUMER_INIT: creating KafkaConsumer");

        let consumer =
            KafkaConsumer::new(&config.kafka_brokers, "audit-group", &schema_registry_url)?;

        tracing::info!("AUDIT_CONSUMER_INIT: KafkaConsumer created");

        Ok(Self {
            consumer,
            config,
            dispatcher: container.dispatcher(),
            validator: Arc::new(EventSchemaValidator::new()),
            router: Arc::new(EventVersionRouter::new()),
        })
    }

    pub async fn run(&self) -> Result<()> {
        tracing::info!(
            kafka_brokers = %self.config.kafka_brokers,
            kafka_topic = %self.config.kafka_topic,
            "AUDIT_CONSUMER_RUN: subscribing to Kafka topic"
        );

        self.consumer
            .subscribe(&[self.config.kafka_topic.as_str()])
            .await?;

        tracing::info!(
            kafka_topic = %self.config.kafka_topic,
            "AUDIT_CONSUMER_RUN: Kafka subscription established"
        );

        let dispatcher = self.dispatcher.clone();
        let validator = self.validator.clone();
        let router = self.router.clone();
        let consumer = &self.consumer;

        tracing::info!("AUDIT_CONSUMER_RUN: starting Kafka event listener");

        consumer
            .listen(move |event| {
                let dispatcher = dispatcher.clone();
                let validator = validator.clone();
                let router = router.clone();

                async move {
                    tracing::info!(
                        traceparent = ?event.traceparent,
                        "AUDIT_CONSUMER_EVENT: received Kafka event"
                    );

                    let trace_context = telemetry::tracing::extract_traceparent_context(
                        event.traceparent.as_deref(),
                    );

                    tracing::info!(
                        traceparent = ?event.traceparent,
                        "AUDIT_CONSUMER_EVENT: trace context extracted"
                    );

                    let json_value = consumer.decode_avro(&event.payload).await?;

                    tracing::info!("AUDIT_CONSUMER_EVENT: Avro payload decoded");

                    let envelope = serde_json::from_value::<EventEnvelope>(json_value)?;

                    tracing::info!(
                        event_type = %envelope.event_type,
                        event_id = %envelope.metadata.event_id,
                        trace_id = %envelope.metadata.trace_id,
                        traceparent = ?envelope.metadata.traceparent,
                        "AUDIT_CONSUMER_EVENT: EventEnvelope deserialized"
                    );

                    validator.validate(&envelope)?;

                    tracing::info!(
                        event_type = %envelope.event_type,
                        "AUDIT_CONSUMER_EVENT: event validated"
                    );

                    router.route(&envelope)?;

                    tracing::info!(
                        event_type = %envelope.event_type,
                        "AUDIT_CONSUMER_EVENT: event routed"
                    );

                    consumer_metrics::consumed();

                    telemetry::tracing::run_with_context(
                        trace_context,
                        dispatcher.dispatch(envelope),
                    )
                    .await?;

                    tracing::info!("AUDIT_CONSUMER_EVENT: event dispatched successfully");

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }
}
