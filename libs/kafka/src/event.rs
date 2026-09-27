#[derive(Debug, Clone)]
pub struct KafkaEvent {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub traceparent: Option<String>,
}
