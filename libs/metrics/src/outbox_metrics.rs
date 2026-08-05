use std::sync::atomic::{AtomicU64, Ordering};

static PUBLISHED: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static RETRY: AtomicU64 = AtomicU64::new(0);

pub fn published() {
    PUBLISHED.fetch_add(1, Ordering::Relaxed);
}

pub fn failed() {
    FAILED.fetch_add(1, Ordering::Relaxed);
}

pub fn retry() {
    RETRY.fetch_add(1, Ordering::Relaxed);
}

pub fn published_total() -> u64 {
    PUBLISHED.load(Ordering::Relaxed)
}

pub fn failed_total() -> u64 {
    FAILED.load(Ordering::Relaxed)
}

pub fn retry_total() -> u64 {
    RETRY.load(Ordering::Relaxed)
}