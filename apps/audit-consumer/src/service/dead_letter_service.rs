use std::sync::Arc;

use anyhow::Result;

use chrono::Utc;

use uuid::Uuid;

use domain::events::{dead_letter_event::DeadLetterEvent, event_envelope::EventEnvelope};

use repository::dead_letter_repository::DeadLetterRepository;

pub struct DeadLetterService {
    repository: Arc<dyn DeadLetterRepository>,
}

impl DeadLetterService {
    pub fn new(repository: Arc<dyn DeadLetterRepository>) -> Self {
        Self { repository }
    }

    pub async fn save(&self, envelope: EventEnvelope, error: String, attempts: i32) -> Result<()> {
        let event = DeadLetterEvent {
            id: Uuid::new_v4(),

            event_id: envelope.metadata.event_id,

            event_type: envelope.event_type,

            payload: envelope.payload,

            error,

            attempts,

            created_at: Utc::now(),
        };

        self.repository.save(event).await
    }
}
