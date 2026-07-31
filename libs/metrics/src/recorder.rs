use crate::metrics::{DEAD_LETTERS, EVENTS_FAILED, EVENTS_PROCESSED, RETRIES};

pub fn processed() {
    EVENTS_PROCESSED.inc();
}

pub fn failed() {
    EVENTS_FAILED.inc();
}

pub fn retry() {
    RETRIES.inc();
}

pub fn dead_letter() {
    DEAD_LETTERS.inc();
}
