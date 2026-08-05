use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub async fn health() -> Response {
    (
        StatusCode::OK,
        r#"{"status":"UP"}"#,
    )
        .into_response()
}