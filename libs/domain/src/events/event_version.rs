use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventVersion {
    pub major: u16,

    pub minor: u16,

    pub patch: u16,
}

impl Default for EventVersion {
    fn default() -> Self {
        Self {
            major: 1,

            minor: 0,

            patch: 0,
        }
    }
}
