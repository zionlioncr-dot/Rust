use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use domain::events::processed_event::ProcessedEvent;

/// Repositorio responsable de la persistencia de eventos procesados.
///
/// Implementa el patrón Inbox para garantizar que un evento
/// no sea procesado más de una vez por un consumidor.
///
/// Cada implementación (PostgreSQL, Redis, DynamoDB, etc.)
/// deberá cumplir este contrato.
#[async_trait]
pub trait ProcessedEventRepository: Send + Sync {
    /// Indica si un evento ya fue procesado.
    ///
    /// Retorna:
    ///
    /// - true  -> el evento ya existe
    /// - false -> el evento todavía no fue procesado
    async fn exists(&self, event_id: Uuid) -> Result<bool>;

    /// Guarda un evento como procesado.
    ///
    /// Este método debe ejecutarse únicamente
    /// después de completar exitosamente
    /// toda la lógica del handler.
    async fn save(&self, event: ProcessedEvent) -> Result<()>;

    /// Elimina un registro de evento procesado.
    ///
    /// Útil para:
    ///
    /// - pruebas
    /// - reprocesamiento manual
    /// - herramientas administrativas
    async fn delete(&self, event_id: Uuid) -> Result<()>;

    /// Obtiene un evento previamente procesado.
    ///
    /// Retorna:
    ///
    /// Some(event) si existe.
    ///
    /// None si no existe.
    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ProcessedEvent>>;

    /// Cuenta la cantidad total de eventos procesados.
    ///
    /// Muy útil para:
    ///
    /// - métricas
    /// - pruebas
    /// - monitoreo
    async fn count(&self) -> Result<i64>;

    /// Elimina todos los registros.
    ///
    /// Normalmente sólo se utiliza
    /// durante pruebas automatizadas.
    async fn clear(&self) -> Result<()>;
}
