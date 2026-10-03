use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info, info_span, Instrument};

use common::{config::AppConfig, database::create_pool};

use repository::{outbox_repository::OutboxRepository, PostgresRepository};

use metrics::outbox_metrics;

use crate::publisher::kafka_publisher::KafkaPublisher;

const BATCH_SIZE: i64 = 100;

pub struct OutboxWorker {
    config: AppConfig,
    repository: Arc<dyn OutboxRepository>,
    publisher: KafkaPublisher,
}

impl OutboxWorker {
    pub async fn new() -> Result<Self> {
        let config = AppConfig::load();

        let pool = create_pool(config.max_db_connections).await?;

        let repository = Arc::new(PostgresRepository::new(pool));

        let publisher = KafkaPublisher::new(&config)?;

        Ok(Self {
            config,
            repository,
            publisher,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Outbox Worker started");

        loop {
            let events = self.repository.find_unpublished(BATCH_SIZE).await?;

            if !events.is_empty() {
                info!(pending_events = events.len(), "Fetched unpublished events");
            }

            for event in events {
                let payload = serde_json::to_string(&event.payload)?;

                let subject = format!("{}-value", event.event_type);

                let traceparent = event
                    .payload
                    .get("metadata")
                    .and_then(|metadata| metadata.get("traceparent"))
                    .and_then(serde_json::Value::as_str);

                let trace_context = telemetry::tracing::extract_traceparent_context(traceparent);

                let publisher = &self.publisher;
                let repository = &self.repository;
                let topic = &self.config.kafka_topic;

                let event_id = event.id;
                let event_type = event.event_type.clone();

                let result = telemetry::tracing::run_with_context(trace_context, async move {
                    let span = info_span!(
                        "outbox.publish",
                        event_id = %event_id,
                        event_type = %event_type,
                        subject = %subject,
                        traceparent = ?traceparent,
                    );

                    async move {
                        let result = publisher
                            .publish_with_schema(topic, &subject, &payload, traceparent)
                            .await;

                        match result {
                            Ok(_) => {
                                repository.mark_as_published(event_id).await?;

                                outbox_metrics::published();

                                info!(
                                    event_id = %event_id,
                                    event_type = %event_type,
                                    subject = %subject,
                                    traceparent = ?traceparent,
                                    "Event published successfully with Avro schema"
                                );

                                Ok::<(), anyhow::Error>(())
                            }

                            Err(err) => {
                                outbox_metrics::failed();

                                error!(
                                    event_id = %event_id,
                                    event_type = %event_type,
                                    subject = %subject,
                                    traceparent = ?traceparent,
                                    error = %err,
                                    "Failed to publish event with Avro schema"
                                );

                                Err(err)
                            }
                        }
                    }
                    .instrument(span)
                    .await
                })
                .await;

                if let Err(err) = result {
                    error!(
                        event_id = %event_id,
                        error = %err,
                        "Outbox event processing failed"
                    );
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(self.config.polling_interval)).await;
        }
    }
}
