use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use domain::audit_event::AuditEvent;

use repository::{audit_repository::AuditRepository, outbox_repository::OutboxRepository};

use crate::builders::outbox_builder::OutboxBuilder;

pub struct AuditService {
    audit_repository: Arc<dyn AuditRepository>,

    outbox_repository: Arc<dyn OutboxRepository>,
}

impl AuditService {
    pub fn new(
        audit_repository: Arc<dyn AuditRepository>,

        outbox_repository: Arc<dyn OutboxRepository>,
    ) -> Self {
        Self {
            audit_repository,

            outbox_repository,
        }
    }

    pub async fn create(&self, user: String, action: String) -> Result<AuditEvent> {
        let event = AuditEvent {
            id: Uuid::new_v4(),

            user,

            action,

            created_at: Utc::now(),
        };

        let saved_event = self.audit_repository.create(event).await?;

        metrics::metrics::record_audit(&saved_event.action);

        let outbox = OutboxBuilder::audit_created(&saved_event)?;

        self.outbox_repository.save(outbox).await?;

        Ok(saved_event)
    }

    pub async fn list(&self) -> Result<Vec<AuditEvent>> {
        self.audit_repository.find_all().await
    }
}
