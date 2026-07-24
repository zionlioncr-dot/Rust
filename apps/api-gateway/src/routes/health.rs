use axum::Json;

use serde::Serialize;

#[derive(Serialize)]

pub struct HealthResponse {
    status: &'static str,

    service: &'static str,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "UP",

        service: "api-gateway",
    })
}
