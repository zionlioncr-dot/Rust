use axum::{extract::State, http::StatusCode, response::IntoResponse, Extension, Json};
use chrono::Utc;
use domain::{financial_transaction::FinancialTransaction, outbox_event::OutboxEvent};
use repository::{FinancialTransactionRepository, PostgresRepository};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use uuid::Uuid;

use crate::{middleware::auth::Claims, proxy::audit::ProxyState};

#[derive(Debug, Deserialize)]
pub struct CreateTransactionRequest {
    pub external_transaction_id: Option<String>,
    pub amount: String,
    pub currency: String,
    pub source_account_id: String,
    pub destination_account_id: String,
    pub counterparty: Option<String>,
    pub country_code: String,
    pub channel: String,
    pub client_ip: Option<String>,
}

pub async fn create_transaction(
    State(state): State<ProxyState>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<CreateTransactionRequest>,
) -> impl IntoResponse {
    let tenant_id = claims.tenant_id.trim();

    if tenant_id.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "JWT tenant_id is required"})),
        )
            .into_response();
    }

    let amount = match Decimal::from_str(input.amount.trim()) {
        Ok(value) if value > Decimal::ZERO => value,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "amount must be a valid positive decimal"
                })),
            )
                .into_response();
        }
    };

    let currency = input.currency.trim().to_ascii_uppercase();
    let country_code = input.country_code.trim().to_ascii_uppercase();
    let source_account_id = input.source_account_id.trim().to_string();
    let destination_account_id = input.destination_account_id.trim().to_string();
    let channel = input.channel.trim().to_string();

    if currency.len() != 3
        || country_code.is_empty()
        || source_account_id.is_empty()
        || destination_account_id.is_empty()
        || channel.is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "currency must have 3 letters; country_code, accounts and channel are required"
            })),
        ).into_response();
    }

    let now = Utc::now();
    let transaction_id = Uuid::new_v4();

    let transaction = FinancialTransaction {
        id: transaction_id,
        tenant_id: tenant_id.to_string(),
        external_transaction_id: input.external_transaction_id,
        amount,
        currency,
        source_account_id,
        destination_account_id,
        counterparty: input.counterparty,
        country_code,
        channel,
        client_ip: input.client_ip,
        occurred_at: now,
        created_at: now,
    };

    let outbox_event = OutboxEvent {
        id: Uuid::new_v4(),
        tenant_id: tenant_id.to_string(),
        aggregate_type: "financial_transaction".to_string(),
        aggregate_id: transaction_id,
        event_type: "FINANCIAL_TRANSACTION_CREATED".to_string(),
        payload: serde_json::json!({}),
        created_at: now,
        published: false,
    };

    let repository = PostgresRepository::new(state.pool.clone());

    match repository
        .create_with_outbox(transaction, outbox_event)
        .await
    {
        Ok(created) => (
            StatusCode::CREATED,
            Json(serde_json::json!({"transaction": created})),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, tenant_id = %tenant_id, "Failed to create financial transaction");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "failed to create financial transaction"
                })),
            )
                .into_response()
        }
    }
}
