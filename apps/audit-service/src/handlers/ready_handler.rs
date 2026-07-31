use axum::{Json, extract::State, http::StatusCode};

use serde_json::json;

use common::database::health_check;

use crate::state::AppState;

pub async fn ready(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    match health_check(&state.pool).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "READY"
            })),
        ),

        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "NOT_READY",
                "error": error.to_string()
            })),
        ),
    }
}
