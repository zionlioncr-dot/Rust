use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub async fn version() -> Response {
    (
        StatusCode::OK,
        r#"{"version":"0.1.0"}"#,
    )
        .into_response()
}