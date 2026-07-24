use uuid::Uuid;

#[derive(Clone)]

pub struct RequestId(pub String);

impl RequestId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}
