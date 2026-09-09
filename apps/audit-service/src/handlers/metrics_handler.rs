use axum::response::IntoResponse;

use prometheus::{Encoder, TextEncoder};

pub async fn metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .expect("failed to encode Prometheus metrics");

    String::from_utf8(buffer).expect("Prometheus metrics are not valid UTF-8")
}
