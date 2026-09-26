use axum::http::StatusCode;

pub async fn ready() -> (StatusCode, &'static str) {
    (StatusCode::OK, "READY")
}
