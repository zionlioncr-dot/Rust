use std::sync::Arc;

use anyhow::Result;

use common::{config::AppConfig, database::create_pool};

use repository::{processed_event_repository::ProcessedEventRepository, PostgresRepository};

use crate::{
    container::dependencies::Dependencies,
    dispatcher::{event_dispatcher::EventDispatcher, handler_registry::HandlerRegistry},
    modules::audit_module,
    retry::{retry_executor::RetryExecutor, retry_policy::RetryPolicy},
    service::{
        audit_processing_service::AuditProcessingService, dead_letter_service::DeadLetterService,
        idempotency_service::IdempotencyService,
    },
};

pub struct ApplicationContainer {
    config: AppConfig,
    dependencies: Dependencies,
    dispatcher: Arc<EventDispatcher>,
}

impl ApplicationContainer {
    pub async fn build() -> Result<Self> {
        let config = AppConfig::load();

        let pool = create_pool(config.max_db_connections).await?;

        let postgres = Arc::new(PostgresRepository::new(pool));

        let processed_repository: Arc<dyn ProcessedEventRepository> = postgres.clone();

        let idempotency = Arc::new(IdempotencyService::new(processed_repository.clone()));

        let processing = Arc::new(AuditProcessingService::new(idempotency.clone()));

        let retry_executor = Arc::new(RetryExecutor::new(RetryPolicy::default()));

        let dead_letter_service = Arc::new(DeadLetterService::new(postgres.clone()));

        let mut registry = HandlerRegistry::new();

        audit_module::register(&mut registry, processing.clone());

        let dispatcher = Arc::new(EventDispatcher::new(
            registry,
            retry_executor.clone(),
            dead_letter_service.clone(),
        ));

        Ok(Self {
            config,
            dispatcher,
            dependencies: Dependencies {
                processed_repository,
                idempotency,
                processing,
                retry_executor,
                dead_letter_service,
            },
        })
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn dependencies(&self) -> &Dependencies {
        &self.dependencies
    }

    pub fn dispatcher(&self) -> Arc<EventDispatcher> {
        self.dispatcher.clone()
    }
}
