use axum::Json;
use serde_json::json;

/// Liveness Probe.
///
/// Indica que el proceso está vivo.
pub async fn live() -> Json<serde_json::Value> {
    Json(json!({
        "status": "UP"
    }))
}
