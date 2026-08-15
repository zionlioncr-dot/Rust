use axum::{
    extract::State,
    http::StatusCode,
    Json,
};

use domain::audit_event::AuditEvent;
use tracing::instrument;

use crate::state::AppState;

#[derive(serde::Deserialize)]
pub struct CreateAuditRequest {
    pub user: String,
    pub action: String,
}

#[instrument(
    name = "audit.create",
    skip(state, request),
    fields(
        audit.user = %request.user,
        audit.action = %request.action
    )
)]
pub async fn create_audit(
    State(state): State<AppState>,
    Json(request): Json<CreateAuditRequest>,
) -> Result<Json<AuditEvent>, StatusCode> {
    let event = state
        .audit_service
        .create(request.user, request.action)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(event))
}