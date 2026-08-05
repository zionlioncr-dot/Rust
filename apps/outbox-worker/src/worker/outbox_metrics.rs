use metrics::outbox_metrics;

pub fn published() {
    outbox_metrics::published();
}

pub fn failed() {
    outbox_metrics::failed();
}

pub fn retry() {
    outbox_metrics::retry();
}