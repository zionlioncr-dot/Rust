use std::sync::Arc;

use anyhow::{anyhow, Result};
use tracing::{error, info};

use domain::events::event_envelope::EventEnvelope;

use crate::{
    dispatcher::handler_registry::HandlerRegistry, retry::retry_executor::RetryExecutor,
    service::dead_letter_service::DeadLetterService,
};

pub struct EventDispatcher {
    registry: HandlerRegistry,
    retry_executor: Arc<RetryExecutor>,
    dead_letter_service: Arc<DeadLetterService>,
}

impl EventDispatcher {
    pub fn new(
        registry: HandlerRegistry,
        retry_executor: Arc<RetryExecutor>,
        dead_letter_service: Arc<DeadLetterService>,
    ) -> Self {
        Self {
            registry,
            retry_executor,
            dead_letter_service,
        }
    }

    pub async fn dispatch(&self, envelope: EventEnvelope) -> Result<()> {
        let handler = self
            .registry
            .get(&envelope.event_type)
            .ok_or_else(|| anyhow!("No handler registered for {}", envelope.event_type))?;

        let event = envelope.clone();

        let result = self
            .retry_executor
            .execute(|| {
                let handler = handler.clone();
                let event = event.clone();

                async move { handler.handle(event).await }
            })
            .await;

        match result {
            Ok(_) => {
                info!(
                    event_type = %envelope.event_type,
                    event_id = %envelope.metadata.event_id,
                    "Event processed successfully"
                );

                Ok(())
            }

            Err(err) => {
                error!(
                    event_type = %envelope.event_type,
                    event_id = %envelope.metadata.event_id,
                    error = %err,
                    "Event processing failed"
                );

                self.dead_letter_service
                    .save(
                        envelope,
                        err.to_string(),
                        self.retry_executor.policy().max_attempts as i32,
                    )
                    .await?;

                Err(err)
            }
        }
    }
}
