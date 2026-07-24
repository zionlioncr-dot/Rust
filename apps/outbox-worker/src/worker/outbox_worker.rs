use std::sync::Arc;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info};

use repository::{outbox_repository::OutboxRepository, PostgresRepository};

use crate::{config::Config, publisher::kafka_publisher::KafkaPublisher};

const BATCH_SIZE: i64 = 100;

pub struct OutboxWorker {
    config: Config,

    repository: Arc<dyn OutboxRepository>,

    publisher: KafkaPublisher,
}

impl OutboxWorker {
    pub async fn new() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;

        let repository = Arc::new(PostgresRepository::new(pool));

        Ok(Self {
            config: Config::load(),

            repository,

            publisher: KafkaPublisher::new()?,
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
                            "Event published successfully"
                        );
                    }

                    Err(e) => {
                        error!(
                            event_id = %event.id,
                            error = %e,
                            "Failed to publish event"
                        );

                        continue;
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(self.config.polling_interval)).await;
        }
    }
}
