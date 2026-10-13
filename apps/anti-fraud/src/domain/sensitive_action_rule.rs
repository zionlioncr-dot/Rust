use anyhow::Result;
use async_trait::async_trait;

use super::{
    fraud_decision::{FraudDecision, FraudEvidence},
    fraud_event::FraudEvent,
    fraud_rule::FraudRule,
    risk_level::RiskLevel,
};

pub struct SensitiveActionRule;

impl SensitiveActionRule {
    pub const RULE_NAME: &'static str = "sensitive_action";

    fn is_sensitive_action(action: &str) -> bool {
        matches!(
            action,
            "PASSWORD_RESET"
                | "CHANGE_PASSWORD"
                | "CHANGE_EMAIL"
                | "CHANGE_PHONE"
                | "CHANGE_SECURITY_SETTINGS"
        )
    }
}

#[async_trait]
impl FraudRule for SensitiveActionRule {
    fn name(&self) -> &'static str {
        Self::RULE_NAME
    }

    async fn evaluate(&self, event: &FraudEvent) -> Result<FraudDecision> {
        if !Self::is_sensitive_action(&event.action) {
            return Ok(FraudDecision::allow());
        }

        let reason = format!("sensitive action detected: {}", event.action);

        Ok(FraudDecision::trigger(
            RiskLevel::High,
            Self::RULE_NAME,
            FraudEvidence::new(Self::RULE_NAME, reason),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(action: &str) -> FraudEvent {
        FraudEvent::new(
            "tenant-test".to_string(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "alice".to_string(),
            action.to_string(),
            chrono::Utc::now(),
        )
    }

    #[tokio::test]
    async fn detects_sensitive_action() {
        let rule = SensitiveActionRule;

        let decision = rule.evaluate(&event("PASSWORD_RESET")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::High);
        assert_eq!(
            decision.triggered_rules,
            vec![SensitiveActionRule::RULE_NAME]
        );
        assert_eq!(decision.evidence.len(), 1);
        assert!(decision.is_suspicious());
    }

    #[tokio::test]
    async fn ignores_normal_action() {
        let rule = SensitiveActionRule;

        let decision = rule.evaluate(&event("VIEW_ACCOUNT")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Low);
        assert!(decision.triggered_rules.is_empty());
        assert!(decision.evidence.is_empty());
        assert!(!decision.is_suspicious());
    }

    #[tokio::test]
    async fn login_failed_is_handled_by_burst_rule() {
        let rule = SensitiveActionRule;

        let decision = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Low);
        assert!(decision.triggered_rules.is_empty());
        assert!(decision.evidence.is_empty());
    }

    #[tokio::test]
    async fn detects_all_sensitive_actions() {
        let rule = SensitiveActionRule;

        let actions = [
            "PASSWORD_RESET",
            "CHANGE_PASSWORD",
            "CHANGE_EMAIL",
            "CHANGE_PHONE",
            "CHANGE_SECURITY_SETTINGS",
        ];

        for action in actions {
            let decision = rule.evaluate(&event(action)).await.unwrap();

            assert_eq!(
                decision.risk_level,
                RiskLevel::High,
                "expected {action} to be detected"
            );
        }
    }
}
