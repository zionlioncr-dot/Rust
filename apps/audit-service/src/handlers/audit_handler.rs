use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};

use common::tenant::TenantContext;
use domain::audit_event::AuditEvent;
use telemetry::record_audit_created;

use crate::state::AppState;

#[derive(serde::Deserialize, Debug)]
pub struct CreateAuditRequest {
    pub user: String,
    pub action: String,
}

#[tracing::instrument(
    name = "audit.create",
    skip(state, request, headers),
    fields(
        audit.user = %request.user,
        audit.action = %request.action,
        tenant_id = tracing::field::Empty
    )
)]
pub async fn create_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateAuditRequest>,
) -> Result<Json<AuditEvent>, StatusCode> {
    let tenant_id = headers
        .get("x-tenant-id")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_str()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let tenant_context = TenantContext::new(tenant_id).map_err(|_| StatusCode::UNAUTHORIZED)?;

    tracing::Span::current().record(
        "tenant_id",
        tracing::field::display(tenant_context.tenant_id()),
    );

    let event = state
        .audit_service
        .create(tenant_context, request.user, request.action)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    record_audit_created();

    Ok(Json(event))
}

pub async fn list_audit(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<AuditEvent>>, StatusCode> {
    let tenant_id = headers
        .get("x-tenant-id")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .to_str()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let tenant_context = TenantContext::new(tenant_id).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let events = state
        .audit_service
        .list(tenant_context)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(events))
}
