use anyhow::Result;

use async_trait::async_trait;

use uuid::Uuid;

use domain::events::dead_letter_event::DeadLetterEvent;

/// Repositorio de Dead Letter Queue.
///
/// Almacena eventos que no pudieron procesarse
/// luego de agotar todos los reintentos.
#[async_trait]
pub trait DeadLetterRepository: Send + Sync {
    /// Guarda un evento en la DLQ.
    async fn save(&self, event: DeadLetterEvent) -> Result<()>;

    /// Busca un evento por su ID.
    async fn find_by_id(&self, id: Uuid) -> Result<Option<DeadLetterEvent>>;

    /// Obtiene todos los eventos.
    async fn find_all(&self) -> Result<Vec<DeadLetterEvent>>;

    /// Elimina un evento.
    async fn delete(&self, id: Uuid) -> Result<()>;

    /// Cuenta los eventos almacenados.
    async fn count(&self) -> Result<i64>;

    /// Limpia completamente la DLQ.
    async fn clear(&self) -> Result<()>;
}
