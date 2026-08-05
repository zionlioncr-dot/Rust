use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use metrics::audit_metrics;

pub async fn metrics() -> Response {
    let body = format!(
        "\
audit_http_requests_total {}\n\
audit_created_total {}\n",
        audit_metrics::requests_total(),
        audit_metrics::audits_created_total(),
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}