use std::{future::Future, pin::Pin};

use anyhow::Result;

use domain::events::event_envelope::EventEnvelope;

pub trait EventHandler: Send + Sync {
    fn event_type(&self) -> &'static str;

    fn handle(
        &self,
        envelope: EventEnvelope,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}
