use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use domain::outbox_event::OutboxEvent;

#[async_trait]
pub trait OutboxRepository: Send + Sync {
    async fn save(&self, event: OutboxEvent) -> Result<()>;

    async fn find_unpublished(&self, limit: i64) -> Result<Vec<OutboxEvent>>;

    async fn mark_as_published(&self, id: Uuid) -> Result<()>;
}
