use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use metrics::consumer_metrics;

pub async fn metrics() -> Response {

    let body = format!(
        "\
audit_events_consumed_total {}\n\
audit_events_processed_total {}\n\
audit_events_failed_total {}\n\
audit_events_dead_letter_total {}\n\
audit_events_retry_total {}\n",
        consumer_metrics::consumed_total(),
        consumer_metrics::processed_total(),
        consumer_metrics::failed_total(),
        consumer_metrics::dead_letter_total(),
        consumer_metrics::retry_total(),
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}