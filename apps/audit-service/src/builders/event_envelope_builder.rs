use anyhow::Result;

use chrono::Utc;

use serde::Serialize;

use uuid::Uuid;

use domain::events::{
    event_envelope::EventEnvelope, event_metadata::EventMetadata, event_version::EventVersion,
};

pub struct EventEnvelopeBuilder;

impl EventEnvelopeBuilder {
    pub fn build<T>(
        event_type: &str,
        source: &str,
        correlation_id: Option<Uuid>,
        payload: &T,
    ) -> Result<EventEnvelope>
    where
        T: Serialize,
    {
        let (current_trace_id, traceparent) = telemetry::tracing::current_trace_context();

        let trace_id = current_trace_id.unwrap_or_else(|| Uuid::new_v4().simple().to_string());

        Ok(EventEnvelope {
            metadata: EventMetadata {
                event_id: Uuid::new_v4(),

                correlation_id: correlation_id.unwrap_or_else(Uuid::new_v4),

                trace_id,

                traceparent,

                source: source.to_string(),

                timestamp: Utc::now(),
            },

            version: EventVersion::default(),

            event_type: event_type.to_string(),

            payload: serde_json::to_value(payload)?,
        })
    }
}
