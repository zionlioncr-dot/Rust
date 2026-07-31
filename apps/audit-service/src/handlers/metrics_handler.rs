use axum::response::{IntoResponse, Response};

use http::{StatusCode, header::CONTENT_TYPE};

use prometheus::{Encoder, TextEncoder};

use metrics::metrics::REGISTRY;

pub async fn metrics() -> Response {
    let encoder = TextEncoder::new();

    let metric_families = REGISTRY.gather();

    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        StatusCode::OK,
        [(CONTENT_TYPE, encoder.format_type())],
        buffer,
    )
        .into_response()
}
