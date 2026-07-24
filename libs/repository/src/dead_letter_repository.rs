use anyhow::Result;

use domain::events::dead_letter_event::DeadLetterEvent;

#[async_trait::async_trait]
pub trait DeadLetterRepository: Send + Sync {
    async fn save(&self, event: DeadLetterEvent) -> Result<()>;
}
