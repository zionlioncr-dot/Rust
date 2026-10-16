use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use common::tenant::TenantContext;
use domain::audit_event::AuditEvent;
use repository::{
    audit_outbox_transaction_repository::AuditOutboxTransactionRepository,
    audit_repository::AuditRepository,
};

use crate::builders::outbox_builder::OutboxBuilder;

pub struct AuditService {
    audit_repository: Arc<dyn AuditRepository>,
    transaction_repository: Arc<dyn AuditOutboxTransactionRepository>,
}

impl AuditService {
    pub fn new(
        audit_repository: Arc<dyn AuditRepository>,
        transaction_repository: Arc<dyn AuditOutboxTransactionRepository>,
    ) -> Self {
        Self {
            audit_repository,
            transaction_repository,
        }
    }

    pub async fn create(
        &self,
        tenant_context: TenantContext,
        user: String,
        action: String,
        client_ip: Option<String>,
    ) -> Result<AuditEvent> {
        let event = AuditEvent {
            id: Uuid::new_v4(),
            tenant_id: tenant_context.tenant_id().to_string(),
            user,
            action,
            client_ip,
            created_at: Utc::now(),
        };

        // Construir el payload antes de abrir la transacción.
        let outbox_event = OutboxBuilder::audit_created(&event, &tenant_context)?;

        self.transaction_repository
            .create_with_outbox(event, outbox_event)
            .await
    }

    pub async fn list(&self, tenant_context: TenantContext) -> Result<Vec<AuditEvent>> {
        self.audit_repository
            .find_all(tenant_context.tenant_id())
            .await
    }
}
