use axum::response::{IntoResponse, Response};

use http::header::CONTENT_TYPE;

use http::StatusCode;

use prometheus::{Encoder, TextEncoder};

pub async fn metrics() -> Response {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        StatusCode::OK,
        [(CONTENT_TYPE, encoder.format_type())],
        buffer,
    )
        .into_response()
}
