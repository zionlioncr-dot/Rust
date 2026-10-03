use anyhow::Result;

use chrono::Utc;

use serde_json::to_value;

use uuid::Uuid;

use common::tenant::TenantContext;

use domain::{
    audit_event::AuditEvent,
    events::{audit_created::AuditCreatedEvent, event_types::AUDIT_CREATED},
    outbox_event::OutboxEvent,
};

use crate::builders::event_envelope_builder::EventEnvelopeBuilder;

pub struct OutboxBuilder;

impl OutboxBuilder {
    pub fn audit_created(
        audit: &AuditEvent,
        tenant_context: &TenantContext,
    ) -> Result<OutboxEvent> {
        let event = AuditCreatedEvent {
            id: audit.id,

            user: audit.user.clone(),

            action: audit.action.clone(),

            created_at: audit.created_at,
        };

        let envelope = EventEnvelopeBuilder::build(
            AUDIT_CREATED,
            "audit-service",
            Some(audit.id),
            tenant_context,
            &event,
        )?;

        Ok(OutboxEvent {
            id: Uuid::new_v4(),

            tenant_id: tenant_context.tenant_id().to_string(),

            aggregate_type: "Audit".to_string(),

            aggregate_id: audit.id,

            event_type: AUDIT_CREATED.to_string(),

            payload: to_value(envelope)?,

            created_at: Utc::now(),

            published: false,
        })
    }
}
