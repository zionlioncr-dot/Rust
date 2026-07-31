use std::sync::Arc;

use repository::{audit_repository::AuditRepository, outbox_repository::OutboxRepository};

use crate::service::audit_service::AuditService;

pub struct Dependencies {
    pub audit_repository: Arc<dyn AuditRepository>,
    pub outbox_repository: Arc<dyn OutboxRepository>,
    pub audit_service: Arc<AuditService>,
}
