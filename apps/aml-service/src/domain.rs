use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmlAlert {
    pub id: Uuid,
    pub tenant_id: String,
    pub event_id: String,
    pub transaction_id: Uuid,
    pub rule_code: String,
    pub severity: String,
    pub description: String,
    pub details: Value,
    pub created_at: DateTime<Utc>,
}

/// Resultado auditable del análisis regulatorio AML, independientemente
/// de que la evaluación produzca una alerta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmlAssessment {
    pub id: Uuid,
    pub tenant_id: String,
    pub event_id: String,
    pub transaction_id: Uuid,
    pub model: String,
    pub jurisdiction: String,
    pub suspicious: bool,
    pub severity: String,
    pub category: String,
    pub description: String,
    pub rationale: String,
    pub confidence: Option<f64>,
    pub evidence_refs: Value,
    pub regulatory_evidence: Value,
    pub limitations: Value,
    pub transaction_data: Value,
    pub created_at: DateTime<Utc>,
    pub assessment_status: String,
    /// Solo se establece cuando el modelo considera que hay sospecha.
    pub alert: Option<AmlAlert>,
}
