use anyhow::Result;

use tracing::info;

use domain::events::audit_created::AuditCreatedEvent;

pub struct AuditProcessingService;

impl AuditProcessingService {
    pub fn new() -> Self {
        Self
    }

    pub async fn process(&self, event: AuditCreatedEvent) -> Result<()> {
        info!(
            event_id = %event.id,
            user = %event.user,
            action = %event.action,
            "Audit Event Processed"
        );

        Ok(())
    }
}
