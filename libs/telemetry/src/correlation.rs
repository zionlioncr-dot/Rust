use uuid::Uuid;

#[derive(Clone)]

pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}
