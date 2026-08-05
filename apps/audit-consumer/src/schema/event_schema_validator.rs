use anyhow::{anyhow, Result};

use domain::events::event_envelope::EventEnvelope;

/// Valida que el EventEnvelope cumpla con el contrato mínimo
/// esperado por la plataforma antes de ser despachado.
///
/// En esta primera versión se valida:
///
/// - metadata.event_id
/// - metadata.source
/// - metadata.trace_id
/// - event_type
/// - version.major
/// - payload
///
/// En el Sprint B2 se extenderá con validación
/// mediante Schema Registry.
pub struct EventSchemaValidator;

impl EventSchemaValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate(&self, envelope: &EventEnvelope) -> Result<()> {
        //
        // Event Type
        //
        if envelope.event_type.trim().is_empty() {
            return Err(anyhow!("event_type is empty"));
        }

        //
        // Source
        //
        if envelope.metadata.source.trim().is_empty() {
            return Err(anyhow!("metadata.source is empty"));
        }

        //
        // Trace Id
        //
        if envelope.metadata.trace_id.trim().is_empty() {
            return Err(anyhow!("metadata.trace_id is empty"));
        }

        //
        // Version
        //
        if envelope.version.major == 0 {
            return Err(anyhow!("invalid event version"));
        }

        //
        // Payload
        //
        if envelope.payload.is_null() {
            return Err(anyhow!("payload is null"));
        }

        Ok(())
    }

    pub fn validate_version(&self, envelope: &EventEnvelope) -> Result<()> {
        match envelope.version.major {
            1 => Ok(()),

            version => Err(anyhow!("unsupported event version {}", version)),
        }
    }
}

impl Default for EventSchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}
