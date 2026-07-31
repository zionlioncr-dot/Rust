use once_cell::sync::Lazy;

use prometheus::{IntCounter, Registry};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static EVENTS_PROCESSED: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new("events_processed_total", "Total processed events").unwrap();

    REGISTRY.register(Box::new(counter.clone())).unwrap();

    counter
});

pub static EVENTS_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new("events_failed_total", "Total failed events").unwrap();

    REGISTRY.register(Box::new(counter.clone())).unwrap();

    counter
});

pub static RETRIES: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new("retry_total", "Retry attempts").unwrap();

    REGISTRY.register(Box::new(counter.clone())).unwrap();

    counter
});

pub static DEAD_LETTERS: Lazy<IntCounter> = Lazy::new(|| {
    let counter = IntCounter::new("dead_letter_total", "Dead letters").unwrap();

    REGISTRY.register(Box::new(counter.clone())).unwrap();

    counter
});
