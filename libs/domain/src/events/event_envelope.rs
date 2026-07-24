use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{event_metadata::EventMetadata, event_version::EventVersion};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub metadata: EventMetadata,

    pub version: EventVersion,

    pub event_type: String,

    pub payload: Value,
}
