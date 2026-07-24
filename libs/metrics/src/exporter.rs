use axum::response::IntoResponse;

use prometheus::{Encoder, TextEncoder};

pub async fn metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();

    let metrics = prometheus::gather();

    let mut buffer = Vec::new();

    encoder.encode(&metrics, &mut buffer).unwrap();

    String::from_utf8(buffer).unwrap()
}
