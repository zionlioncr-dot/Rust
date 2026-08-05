use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use metrics::audit_metrics;

pub async fn metrics() -> Response {
    let body = format!(
        "\
audit_requests_total {}\n\
audit_created_total {}\n\
",
        audit_metrics::request_total(),
        audit_metrics::audit_created_total(),
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}