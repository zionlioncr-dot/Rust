use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use chrono::Utc;

use domain::events::processed_event::ProcessedEvent;

use repository::processed_event_repository::ProcessedEventRepository;

pub struct IdempotencyService {
    repository: Arc<dyn ProcessedEventRepository>,
}

impl IdempotencyService {
    pub fn new(repository: Arc<dyn ProcessedEventRepository>) -> Self {
        Self { repository }
    }

    /// Verifica si el evento ya fue procesado.
    pub async fn already_processed(&self, event_id: Uuid) -> Result<bool> {
        self.repository.exists(event_id).await
    }

    /// Marca un evento como procesado.
    pub async fn mark_processed(
        &self,
        event_id: Uuid,
        consumer: &str,
        handler: &str,
    ) -> Result<()> {
        let processed_event = ProcessedEvent {
            event_id,
            consumer: consumer.to_string(),
            handler: handler.to_string(),
            processed_at: Utc::now(),
        };

        self.repository.save(processed_event).await
    }

    /// Obtiene un evento procesado.
    pub async fn find(&self, event_id: Uuid) -> Result<Option<ProcessedEvent>> {
        self.repository.find_by_id(event_id).await
    }

    /// Elimina un evento procesado.
    pub async fn delete(&self, event_id: Uuid) -> Result<()> {
        self.repository.delete(event_id).await
    }

    /// Cuenta eventos procesados.
    pub async fn count(&self) -> Result<i64> {
        self.repository.count().await
    }

    /// Limpia la inbox.
    pub async fn clear(&self) -> Result<()> {
        self.repository.clear().await
    }
}
