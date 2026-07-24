use anyhow::{bail, Result};

use domain::events::event_envelope::EventEnvelope;

use crate::{dispatcher::handler_registry::HandlerRegistry, modules::audit_module};

pub struct EventDispatcher {
    registry: HandlerRegistry,
}

impl EventDispatcher {
    pub fn new() -> Self {
        let mut registry = HandlerRegistry::new();

        audit_module::register(&mut registry);

        Self { registry }
    }

    pub async fn dispatch(&self, envelope: EventEnvelope) -> Result<()> {
        if let Some(handler) = self.registry.get(&envelope.event_type) {
            handler.handle(envelope).await?;

            return Ok(());
        }

        bail!("No handler registered for {}", envelope.event_type)
    }
}
