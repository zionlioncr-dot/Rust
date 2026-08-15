use axum::{response::IntoResponse, Json};

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct LivenessResponse {
    pub status: &'static str,
    pub alive: bool,
}

pub async fn live() -> impl IntoResponse {
    Json(LivenessResponse {
        status: "UP",
        alive: true,
    })
}
