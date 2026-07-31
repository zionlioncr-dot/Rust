use std::sync::Arc;

use anyhow::Result;

use tracing::{error, info};

use metrics::recorder;

use domain::events::event_envelope::EventEnvelope;

use domain::events::dead_letter_event::DeadLetterEvent;

use repository::dead_letter_repository::DeadLetterRepository;

pub struct DeadLetterService {
    repository: Arc<dyn DeadLetterRepository>,
}

impl DeadLetterService {
    pub fn new(repository: Arc<dyn DeadLetterRepository>) -> Self {
        Self { repository }
    }

    pub async fn save(
        &self,
        envelope: EventEnvelope,
        error_message: String,
        attempts: i32,
    ) -> Result<()> {
        error!(
            event_id = %envelope.metadata.event_id,
            attempts = attempts,
            error = %error_message,
            "Moving event to Dead Letter Queue"
        );

        let event = DeadLetterEvent::new(
            envelope.metadata.event_id,
            envelope.event_type.clone(),
            envelope.payload.clone(),
            error_message,
            attempts,
        );

        self.repository.save(event).await?;

        recorder::failed();
        recorder::dead_letter();

        info!(
            event_id = %envelope.metadata.event_id,
            "Dead Letter stored successfully"
        );

        Ok(())
    }
}
