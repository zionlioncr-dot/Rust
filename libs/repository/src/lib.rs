pub mod audit_repository;
pub mod bank_ip_allowlist_repository;
pub mod dead_letter_repository;
pub mod outbox_repository;
pub mod processed_event_repository;
pub mod rag;

pub mod postgres;

pub use audit_repository::AuditRepository;
pub use bank_ip_allowlist_repository::BankIpAllowlistRepository;
pub use dead_letter_repository::DeadLetterRepository;
pub use outbox_repository::OutboxRepository;
pub use processed_event_repository::ProcessedEventRepository;
pub use rag::repository::{RagDocument, RagRepository, RagSearchResult};

pub use postgres::bank_ip_allowlist_repository::PostgresBankIpAllowlistRepository;
pub use postgres::repository::PostgresRepository;
