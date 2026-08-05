use std::sync::Arc;

use anyhow::{bail, Result};

use tracing::{info, warn};

use domain::events::audit_created::AuditCreatedEvent;

use metrics::consumer_metrics;

use crate::service::idempotency_service::IdempotencyService;

pub struct AuditProcessingService {
    idempotency: Arc<IdempotencyService>,
}

impl AuditProcessingService {
    pub fn new(idempotency: Arc<IdempotencyService>) -> Self {
        Self { idempotency }
    }

    pub async fn process(
        &self,
        event: AuditCreatedEvent,
    ) -> Result<()> {

        //
        // Domain validation
        //

        if event.user.trim().is_empty() {
            consumer_metrics::failed();
            bail!("user cannot be empty");
        }

        if event.action.trim().is_empty() {
            consumer_metrics::failed();
            bail!("action cannot be empty");
        }

        //
        // Idempotency
        //

        if self.idempotency.already_processed(event.id).await? {

            warn!(
                event_id = %event.id,
                "Duplicate event ignored"
            );

            return Ok(());
        }

        //
        // Business logic
        //

        info!(
            event_id = %event.id,
            user = %event.user,
            action = %event.action,
            "Audit Event Processed"
        );

        self.idempotency
            .mark_processed(
                event.id,
                "audit-consumer",
                "audit-processing-service",
            )
            .await?;

        consumer_metrics::processed();

        Ok(())
    }
}