pub mod audit_repository;
pub mod dead_letter_repository;
pub mod outbox_repository;
pub mod processed_event_repository;
pub mod rag;

pub mod postgres;

pub use audit_repository::AuditRepository;
pub use dead_letter_repository::DeadLetterRepository;
pub use outbox_repository::OutboxRepository;
pub use processed_event_repository::ProcessedEventRepository;
pub use rag::repository::{RagDocument, RagRepository, RagSearchResult};

pub use postgres::repository::PostgresRepository;
