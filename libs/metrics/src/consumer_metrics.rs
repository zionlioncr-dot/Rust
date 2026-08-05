use std::sync::atomic::{AtomicU64, Ordering};

static CONSUMED: AtomicU64 = AtomicU64::new(0);
static PROCESSED: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static DLQ: AtomicU64 = AtomicU64::new(0);
static RETRY: AtomicU64 = AtomicU64::new(0);

pub fn consumed() {
    CONSUMED.fetch_add(1, Ordering::Relaxed);
}

pub fn processed() {
    PROCESSED.fetch_add(1, Ordering::Relaxed);
}

pub fn failed() {
    FAILED.fetch_add(1, Ordering::Relaxed);
}

pub fn retry() {
    RETRY.fetch_add(1, Ordering::Relaxed);
}

pub fn dead_letter() {
    DLQ.fetch_add(1, Ordering::Relaxed);
}

pub fn consumed_total() -> u64 {
    CONSUMED.load(Ordering::Relaxed)
}

pub fn processed_total() -> u64 {
    PROCESSED.load(Ordering::Relaxed)
}

pub fn failed_total() -> u64 {
    FAILED.load(Ordering::Relaxed)
}

pub fn retry_total() -> u64 {
    RETRY.load(Ordering::Relaxed)
}

pub fn dead_letter_total() -> u64 {
    DLQ.load(Ordering::Relaxed)
}