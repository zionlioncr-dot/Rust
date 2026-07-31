use std::sync::Arc;

use repository::outbox_repository::OutboxRepository;

use crate::publisher::kafka_publisher::KafkaPublisher;

pub struct Dependencies {
    pub repository: Arc<dyn OutboxRepository>,
    pub publisher: Arc<KafkaPublisher>,
}