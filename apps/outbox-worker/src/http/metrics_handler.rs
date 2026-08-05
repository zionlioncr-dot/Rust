use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use metrics::outbox_metrics;

pub async fn metrics() -> Response {
    let body = format!(
        "\
outbox_events_published_total {}\n\
outbox_events_failed_total {}\n\
outbox_events_retry_total {}\n",
        outbox_metrics::published_total(),
        outbox_metrics::failed_total(),
        outbox_metrics::retry_total(),
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}