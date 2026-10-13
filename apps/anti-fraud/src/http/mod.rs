use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use uuid::Uuid;

use crate::{
    domain::fraud_alert::FraudAlert, repository::fraud_alert_repository::FraudAlertRepository,
};

#[derive(Clone)]
pub struct AppState {
    pub alert_repository: Arc<dyn FraudAlertRepository>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/fraud/alerts", get(list_alerts))
        .route("/fraud/alerts/{id}", get(get_alert))
        .route("/health", get(health))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn list_alerts(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<FraudAlert>>, (StatusCode, Json<serde_json::Value>)> {
    let tenant_id = tenant_id_from_headers(&headers)?;

    let alerts = state
        .alert_repository
        .find_all(&tenant_id)
        .await
        .map_err(internal_error)?;

    Ok(Json(alerts))
}

async fn get_alert(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: axum::http::HeaderMap,
) -> Result<Json<FraudAlert>, (StatusCode, Json<serde_json::Value>)> {
    let tenant_id = tenant_id_from_headers(&headers)?;

    let alert = state
        .alert_repository
        .find_by_id(&tenant_id, id)
        .await
        .map_err(internal_error)?;

    match alert {
        Some(alert) => Ok(Json(alert)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "not_found",
                "message": "Fraud alert not found"
            })),
        )),
    }
}

fn tenant_id_from_headers(
    headers: &axum::http::HeaderMap,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let value = headers.get("x-tenant-id").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "unauthorized",
                "message": "Missing x-tenant-id header"
            })),
        )
    })?;

    let tenant_id = value.to_str().map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "unauthorized",
                "message": "Invalid x-tenant-id header"
            })),
        )
    })?;

    if tenant_id.trim().is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "unauthorized",
                "message": "Invalid tenant context"
            })),
        ));
    }

    Ok(tenant_id.trim().to_string())
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::error!(
        error = %error,
        "Fraud alerts repository operation failed"
    );

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": "internal_error",
            "message": "Failed to retrieve fraud alerts"
        })),
    )
}
