use axum::{response::IntoResponse, Json};

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ReadinessResponse {
    pub status: &'static str,
    pub ready: bool,
}

pub async fn ready() -> impl IntoResponse {
    Json(ReadinessResponse {
        status: "UP",
        ready: true,
    })
}
