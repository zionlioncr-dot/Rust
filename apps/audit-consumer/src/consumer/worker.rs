use std::sync::Arc;

use anyhow::Result;

use domain::events::event_envelope::EventEnvelope;

use crate::dispatcher::event_dispatcher::EventDispatcher;

pub struct Worker {
    dispatcher: Arc<EventDispatcher>,
}

impl Worker {
    pub fn new(dispatcher: Arc<EventDispatcher>) -> Self {
        Self { dispatcher }
    }

    pub async fn process(&self, envelope: EventEnvelope) -> Result<()> {
        self.dispatcher.dispatch(envelope).await
    }
}
