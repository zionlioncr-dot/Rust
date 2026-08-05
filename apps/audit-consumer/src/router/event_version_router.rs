use anyhow::{anyhow, Result};

use domain::events::event_envelope::EventEnvelope;

/// Router encargado de decidir qué pipeline utilizar
/// dependiendo de la versión del evento.
///
/// En Sprint B1 solamente existe soporte para la
/// versión mayor 1.
///
/// Futuro:
///
/// v1 -> Dispatcher V1
/// v2 -> Dispatcher V2
/// v3 -> Dispatcher V3
///
#[derive(Default)]
pub struct EventVersionRouter;

impl EventVersionRouter {
    pub fn new() -> Self {
        Self
    }

    /// Verifica que la versión del evento esté soportada.
    pub fn route(&self, envelope: &EventEnvelope) -> Result<()> {
        match envelope.version.major {
            1 => Ok(()),

            version => Err(anyhow!("unsupported event version {}", version)),
        }
    }

    /// Indica si la versión está soportada.
    pub fn is_supported(&self, envelope: &EventEnvelope) -> bool {
        matches!(envelope.version.major, 1)
    }

    /// Devuelve la versión mayor.
    pub fn major_version(&self, envelope: &EventEnvelope) -> u16 {
        envelope.version.major
    }
}
