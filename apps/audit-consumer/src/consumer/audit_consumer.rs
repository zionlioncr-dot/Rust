use std::sync::Arc;

use anyhow::Result;

use sqlx::postgres::PgPoolOptions;

use domain::events::event_envelope::EventEnvelope;

use kafka::KafkaConsumer;

use repository::{processed_event_repository::ProcessedEventRepository, PostgresRepository};

use crate::{
    config::Config,
    dispatcher::{event_dispatcher::EventDispatcher, handler_registry::HandlerRegistry},
    modules::audit_module,
    service::{
        audit_processing_service::AuditProcessingService, idempotency_service::IdempotencyService,
    },
};

pub struct AuditConsumer {
    consumer: KafkaConsumer,
    config: Config,
    dispatcher: EventDispatcher,
}

impl AuditConsumer {
    pub async fn new() -> Result<Self> {
        let config = Config::load();

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;

        let postgres = Arc::new(PostgresRepository::new(pool));

        let processed_repository: Arc<dyn ProcessedEventRepository> = postgres.clone();

        let idempotency = Arc::new(IdempotencyService::new(processed_repository));

        let processing = Arc::new(AuditProcessingService::new(idempotency));

        let mut registry = HandlerRegistry::new();

        audit_module::register(&mut registry, processing);

        Ok(Self {
            consumer: KafkaConsumer::new("audit-group")?,
            config,
            dispatcher: EventDispatcher::new(registry),
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
