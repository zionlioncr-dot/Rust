use std::sync::Arc;

use anyhow::Result;

use common::{
    config::AppConfig,
    database::create_pool,
};

use repository::{
    outbox_repository::OutboxRepository,
    PostgresRepository,
};

use crate::{
    container::dependencies::Dependencies,
    publisher::kafka_publisher::KafkaPublisher,
};

pub struct ApplicationContainer {
    config: AppConfig,
    dependencies: Dependencies,
}

impl ApplicationContainer {
    pub async fn build() -> Result<Self> {
        let config = AppConfig::load();

        let pool = create_pool(config.max_db_connections).await?;

        let postgres = Arc::new(PostgresRepository::new(pool));

        let repository: Arc<dyn OutboxRepository> =
            postgres.clone();

        let publisher =
            Arc::new(KafkaPublisher::new(&config)?);

        Ok(Self {
            config,
            dependencies: Dependencies {
                repository,
                publisher,
            },
        })
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn dependencies(&self) -> &Dependencies {
        &self.dependencies
    }
}