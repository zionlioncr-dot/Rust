use std::sync::Arc;

use repository::processed_event_repository::ProcessedEventRepository;

use crate::service::{
    audit_processing_service::AuditProcessingService, dead_letter_service::DeadLetterService,
    idempotency_service::IdempotencyService,
};

use crate::retry::retry_executor::RetryExecutor;

pub struct Dependencies {
    pub processed_repository: Arc<dyn ProcessedEventRepository>,
    pub idempotency: Arc<IdempotencyService>,
    pub processing: Arc<AuditProcessingService>,
    pub retry_executor: Arc<RetryExecutor>,
    pub dead_letter_service: Arc<DeadLetterService>,
}
