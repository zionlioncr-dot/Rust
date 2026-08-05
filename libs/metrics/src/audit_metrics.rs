use std::sync::atomic::{AtomicU64, Ordering};

static REQUESTS: AtomicU64 = AtomicU64::new(0);
static AUDIT_CREATED: AtomicU64 = AtomicU64::new(0);

pub fn request() {
    REQUESTS.fetch_add(1, Ordering::Relaxed);
}

pub fn audit_created() {
    AUDIT_CREATED.fetch_add(1, Ordering::Relaxed);
}

pub fn request_total() -> u64 {
    REQUESTS.load(Ordering::Relaxed)
}

pub fn audit_created_total() -> u64 {
    AUDIT_CREATED.load(Ordering::Relaxed)
}