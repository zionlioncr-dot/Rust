use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Representa un evento que ya fue procesado por un consumidor.
///
/// Esta entidad implementa el patrón Inbox para garantizar
/// idempotencia en el procesamiento de eventos.
///
/// Un mismo evento puede ser procesado por distintos consumidores,
/// pero nunca dos veces por el mismo consumidor/handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ProcessedEvent {
    /// Identificador único del evento.
    pub event_id: Uuid,

    /// Nombre del consumidor que procesó el evento.
    ///
    /// Ejemplo:
    /// - audit-consumer
    /// - notification-consumer
    /// - analytics-consumer
    pub consumer: String,

    /// Handler responsable del procesamiento.
    ///
    /// Ejemplo:
    /// - AuditCreatedHandler
    /// - UserRegisteredHandler
    pub handler: String,

    /// Fecha y hora en que el evento fue procesado.
    pub processed_at: DateTime<Utc>,
}

impl ProcessedEvent {
    /// Crea una nueva instancia de ProcessedEvent.
    pub fn new(event_id: Uuid, consumer: impl Into<String>, handler: impl Into<String>) -> Self {
        Self {
            event_id,
            consumer: consumer.into(),
            handler: handler.into(),
            processed_at: Utc::now(),
        }
    }
}
