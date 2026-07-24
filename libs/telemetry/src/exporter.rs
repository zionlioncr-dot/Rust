use axum::response::{IntoResponse, Response};

use http::header::CONTENT_TYPE;

use prometheus::{Encoder, TextEncoder};

pub async fn metrics() -> Response {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        [(CONTENT_TYPE, encoder.format_type())],
        String::from_utf8(buffer).unwrap(),
    )
        .into_response()
}
