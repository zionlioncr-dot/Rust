use anyhow::Result;

use chrono::Utc;
use serde_json::to_value;
use uuid::Uuid;

use domain::{
    audit_event::AuditEvent, events::audit_created::AuditCreatedEvent, outbox_event::OutboxEvent,
};

pub struct OutboxBuilder;

impl OutboxBuilder {
    pub fn audit_created(audit: &AuditEvent) -> Result<OutboxEvent> {
        let payload = to_value(AuditCreatedEvent {
            id: audit.id,

            user: audit.user.clone(),

            action: audit.action.clone(),

            created_at: audit.created_at,
        })?;

        Ok(OutboxEvent {
            id: Uuid::new_v4(),

            aggregate_type: "Audit".to_string(),

            aggregate_id: audit.id,

            event_type: "AuditCreated".to_string(),

            payload,

            created_at: Utc::now(),

            published: false,
        })
    }
}
