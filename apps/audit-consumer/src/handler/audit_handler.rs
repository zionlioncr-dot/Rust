use std::{future::Future, pin::Pin, sync::Arc};

use anyhow::Result;

use domain::events::{
    audit_created::AuditCreatedEvent,
    event_envelope::EventEnvelope,
    event_types::AUDIT_CREATED,
};

use crate::{
    handler::event_handler::EventHandler,
    service::audit_processing_service::AuditProcessingService,
};

pub struct AuditHandler {
    service: Arc<AuditProcessingService>,
}

impl AuditHandler {
    pub fn new(service: Arc<AuditProcessingService>) -> Self {
        Self { service }
    }
}

impl EventHandler for AuditHandler {
    fn event_type(&self) -> &'static str {
        AUDIT_CREATED
    }

    fn handle(
        &self,
        envelope: EventEnvelope,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let event: AuditCreatedEvent =
                serde_json::from_value(envelope.payload)?;

            self.service.process(event).await?;

            Ok(())
        })
    }
}