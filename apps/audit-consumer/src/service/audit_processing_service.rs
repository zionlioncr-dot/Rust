use std::sync::Arc;

use anyhow::Result;

use tracing::{info, warn};

use domain::events::audit_created::AuditCreatedEvent;

use metrics::recorder;

use crate::service::idempotency_service::IdempotencyService;

pub struct AuditProcessingService {
    idempotency: Arc<IdempotencyService>,
}

impl AuditProcessingService {
    pub fn new(idempotency: Arc<IdempotencyService>) -> Self {
        Self { idempotency }
    }

    pub async fn process(&self, event: AuditCreatedEvent) -> Result<()> {
        if self.idempotency.already_processed(event.id).await? {
            warn!(
                event_id = %event.id,
                "Duplicate event ignored"
            );

            return Ok(());
        }

        info!(
            event_id = %event.id,
            user = %event.user,
            action = %event.action,
            "Audit Event Processed"
        );

        self.idempotency
            .mark_processed(event.id, "audit-consumer", "audit-processing-service")
            .await?;

        recorder::processed();

        Ok(())
    }
}
