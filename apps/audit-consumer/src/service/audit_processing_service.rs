use std::sync::Arc;

use anyhow::Result;

use tracing::{info, warn};

use domain::events::audit_created::AuditCreatedEvent;

use crate::service::idempotency_service::IdempotencyService;

pub struct AuditProcessingService {
    idempotency: Arc<IdempotencyService>,
}

impl AuditProcessingService {
    pub fn new(idempotency: Arc<IdempotencyService>) -> Self {
        Self { idempotency }
    }

    pub async fn process(&self, event: AuditCreatedEvent) -> Result<()> {
        // Verificar si el evento ya fue procesado.
        if self.idempotency.already_processed(event.id).await? {
            warn!(
                event_id = %event.id,
                "Duplicate event ignored"
            );

            return Ok(());
        }

        // Lógica del negocio.
        info!(
            event_id = %event.id,
            user = %event.user,
            action = %event.action,
            "Audit Event Processed"
        );

        self.idempotency
            .mark_processed(event.id, "audit-consumer", "AuditHandler")
            .await?;

        Ok(())
    }
}
