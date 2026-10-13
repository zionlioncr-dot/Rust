use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{fraud_decision::FraudEvidence, fraud_event::FraudEvent, risk_level::RiskLevel};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FraudAlert {
    pub id: Uuid,
    pub tenant_id: String,
    pub event_id: Uuid,
    pub audit_id: Uuid,
    pub username: String,
    pub risk_level: RiskLevel,
    pub triggered_rules: Vec<String>,
    pub evidence: Vec<FraudEvidence>,
    pub status: FraudAlertStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FraudAlertStatus {
    Open,
    Reviewed,
    Confirmed,
    Dismissed,
}

impl FraudAlertStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Reviewed => "REVIEWED",
            Self::Confirmed => "CONFIRMED",
            Self::Dismissed => "DISMISSED",
        }
    }
}

impl FraudAlert {
    pub fn new(
        event: &FraudEvent,
        risk_level: RiskLevel,
        triggered_rules: Vec<String>,
        evidence: Vec<FraudEvidence>,
    ) -> Self {
        let now = Utc::now();

        Self {
            id: Uuid::new_v4(),
            tenant_id: event.tenant_id.clone(),
            event_id: event.event_id,
            audit_id: event.audit_id,
            username: event.user.clone(),
            risk_level,
            triggered_rules,
            evidence,
            status: FraudAlertStatus::Open,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_open(&self) -> bool {
        self.status == FraudAlertStatus::Open
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fraud_event::FraudEvent;

    fn event() -> FraudEvent {
        FraudEvent::new(
            "tenant-test".to_string(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            "alice".to_string(),
            "PASSWORD_RESET".to_string(),
            Utc::now(),
        )
    }

    #[test]
    fn creates_open_alert_from_fraud_event() {
        let event = event();

        let alert = FraudAlert::new(
            &event,
            RiskLevel::High,
            vec!["sensitive_action".to_string()],
            vec![FraudEvidence::new(
                "sensitive_action",
                "sensitive action detected: PASSWORD_RESET",
            )],
        );

        assert_ne!(alert.id, Uuid::nil());
        assert_eq!(alert.tenant_id, "tenant-test");
        assert_eq!(alert.event_id, event.event_id);
        assert_eq!(alert.audit_id, event.audit_id);
        assert_eq!(alert.username, "alice");
        assert_eq!(alert.risk_level, RiskLevel::High);
        assert_eq!(alert.triggered_rules, vec!["sensitive_action".to_string()]);
        assert_eq!(alert.evidence.len(), 1);
        assert_eq!(alert.status, FraudAlertStatus::Open);
        assert!(alert.is_open());
        assert_eq!(alert.status.as_str(), "OPEN");
    }

    #[test]
    fn preserves_alert_tenant() {
        let event = FraudEvent::new(
            "tenant-a".to_string(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            "alice".to_string(),
            "PASSWORD_RESET".to_string(),
            Utc::now(),
        );

        let alert = FraudAlert::new(
            &event,
            RiskLevel::Critical,
            vec!["sensitive_action".to_string()],
            vec![],
        );

        assert_eq!(alert.tenant_id, "tenant-a");
    }

    #[test]
    fn alert_status_serialization_values_are_stable() {
        assert_eq!(FraudAlertStatus::Open.as_str(), "OPEN");
        assert_eq!(FraudAlertStatus::Reviewed.as_str(), "REVIEWED");
        assert_eq!(FraudAlertStatus::Confirmed.as_str(), "CONFIRMED");
        assert_eq!(FraudAlertStatus::Dismissed.as_str(), "DISMISSED");
    }
}
