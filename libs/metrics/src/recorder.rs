//! Compatibilidad temporal.
//
// Todo el código nuevo debe usar:
//
// metrics::audit_metrics
// metrics::consumer_metrics
// metrics::outbox_metrics
//
// Este archivo desaparecerá cuando termine la migración.

pub use crate::consumer_metrics::{
    consumed,
    consumed_total,
    dead_letter,
    dead_letter_total,
    failed,
    failed_total,
    processed,
    processed_total,
    retry,
    retry_total,
};