use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info};

use common::{config::AppConfig, database::create_pool};

use repository::{outbox_repository::OutboxRepository, PostgresRepository};

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

            info!(pending_events = events.len(), "Fetched unpublished events");

            for event in events {
                let payload = serde_json::to_string(&event.payload)?;

                match self
                    .publisher
                    .publish(&self.config.kafka_topic, &payload)
                    .await
                {
                    Ok(_) => {
                        self.repository.mark_as_published(event.id).await?;

                        info!(
                            event_id = %event.id,
                            event_type = %event.event_type,
                            "Event published successfully"
                        );
                    }

                    Err(err) => {
                        error!(
                            event_id = %event.id,
                            event_type = %event.event_type,
                            error = %err,
                            "Failed to publish event"
                        );
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(self.config.polling_interval)).await;
        }
    }
}
