use anyhow::Result;

use uuid::Uuid;

use domain::events::processed_event::ProcessedEvent;

#[async_trait::async_trait]
pub trait ProcessedEventRepository: Send + Sync {
    async fn exists(&self, event_id: Uuid) -> Result<bool>;

    async fn save(&self, event: ProcessedEvent) -> Result<()>;
}
