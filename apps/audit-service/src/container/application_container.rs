use std::sync::Arc;

use anyhow::Result;

use common::{config::AppConfig, database::create_pool};

use repository::{
    PostgresRepository, audit_repository::AuditRepository, outbox_repository::OutboxRepository,
};

use crate::{
    container::dependencies::Dependencies, service::audit_service::AuditService, state::AppState,
};

pub struct ApplicationContainer {
    config: AppConfig,

    pool: sqlx::PgPool,

    dependencies: Dependencies,
}

impl ApplicationContainer {
    pub async fn build() -> Result<Self> {
        let config = AppConfig::load();

        let pool = create_pool(config.max_db_connections).await?;

        let postgres = Arc::new(PostgresRepository::new(pool.clone()));

        let audit_repository: Arc<dyn AuditRepository> = postgres.clone();

        let outbox_repository: Arc<dyn OutboxRepository> = postgres.clone();

        let audit_service = Arc::new(AuditService::new(
            audit_repository.clone(),
            outbox_repository.clone(),
        ));

        Ok(Self {
            config,

            pool,

            dependencies: Dependencies {
                audit_repository,
                outbox_repository,
                audit_service,
            },
        })
    }

    pub fn state(&self) -> AppState {
        AppState {
            pool: self.pool.clone(),

            audit_service: self.dependencies.audit_service.clone(),
        }
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn dependencies(&self) -> &Dependencies {
        &self.dependencies
    }
}
