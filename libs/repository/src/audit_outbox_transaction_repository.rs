use anyhow::Result;
use async_trait::async_trait;

use domain::{audit_event::AuditEvent, outbox_event::OutboxEvent};

#[async_trait]
pub trait AuditOutboxTransactionRepository: Send + Sync {
    async fn create_with_outbox(
        &self,
        event: AuditEvent,
        outbox_event: OutboxEvent,
    ) -> Result<AuditEvent>;
}
