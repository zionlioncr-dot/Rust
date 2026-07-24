use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use domain::audit_event::AuditEvent;

#[async_trait]
pub trait AuditRepository: Send + Sync {
    async fn create(&self, event: AuditEvent) -> Result<AuditEvent>;

    async fn find_by_id(&self, id: Uuid) -> Result<Option<AuditEvent>>;

    async fn find_all(&self) -> Result<Vec<AuditEvent>>;
}
