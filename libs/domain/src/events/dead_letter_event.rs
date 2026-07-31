use chrono::{DateTime, Utc};

use serde::{Deserialize, Serialize};

use serde_json::Value;

use uuid::Uuid;

/// Representa un evento que no pudo ser procesado
/// exitosamente incluso después de aplicar la política
/// de reintentos.
///
/// Estos eventos pueden analizarse posteriormente
/// o reprocesarse manualmente.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DeadLetterEvent {
    /// Identificador interno del registro DLQ.
    pub id: Uuid,

    /// Identificador del evento original.
    pub event_id: Uuid,

    /// Tipo del evento.
    pub event_type: String,

    /// Payload original.
    pub payload: Value,

    /// Error que provocó el fallo definitivo.
    pub error: String,

    /// Cantidad de intentos realizados.
    pub attempts: i32,

    /// Fecha de creación del registro.
    pub created_at: DateTime<Utc>,
}

impl DeadLetterEvent {
    pub fn new(
        event_id: Uuid,
        event_type: impl Into<String>,
        payload: Value,
        error: impl Into<String>,
        attempts: i32,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_id,
            event_type: event_type.into(),
            payload,
            error: error.into(),
            attempts,
            created_at: Utc::now(),
        }
    }
}
