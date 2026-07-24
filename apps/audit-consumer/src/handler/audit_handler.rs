use std::{future::Future, pin::Pin};

use anyhow::Result;

use domain::events::{audit_created::AuditCreatedEvent, event_envelope::EventEnvelope};

use crate::{
    handler::event_handler::EventHandler, service::audit_processing_service::AuditProcessingService,
};

pub struct AuditHandler {
    service: AuditProcessingService,
}

impl AuditHandler {
    pub fn new() -> Self {
        Self {
            service: AuditProcessingService::new(),
        }
    }
}

impl EventHandler for AuditHandler {
    fn event_type(&self) -> &'static str {
        "audit.created"
    }

    fn handle(
        &self,
        envelope: EventEnvelope,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let event: AuditCreatedEvent = serde_json::from_value(envelope.payload)?;

            self.service.process(event).await?;

            Ok(())
        })
    }
}
