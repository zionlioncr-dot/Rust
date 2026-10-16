use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::state::fraud_state_store::FraudStateStore;

use super::{
    fraud_decision::{FraudDecision, FraudEvidence},
    fraud_event::FraudEvent,
    fraud_rule::FraudRule,
    risk_level::RiskLevel,
};

pub struct FailedLoginBurstRule {
    state_store: Arc<dyn FraudStateStore>,
}

impl FailedLoginBurstRule {
    pub const RULE_NAME: &'static str = "failed_login_burst";

    pub fn new(state_store: Arc<dyn FraudStateStore>) -> Self {
        Self { state_store }
    }

    fn risk_level(attempt_count: i32) -> RiskLevel {
        match attempt_count {
            0..=2 => RiskLevel::Low,
            3..=4 => RiskLevel::Medium,
            5..=9 => RiskLevel::High,
            _ => RiskLevel::Critical,
        }
    }
}

#[async_trait]
impl FraudRule for FailedLoginBurstRule {
    fn name(&self) -> &'static str {
        Self::RULE_NAME
    }

    async fn evaluate(&self, event: &FraudEvent) -> Result<FraudDecision> {
        if event.action != "LOGIN_FAILED" {
            return Ok(FraudDecision::allow());
        }

        let attempt_count = self
            .state_store
            .record_failed_login(
                &event.tenant_id,
                &event.user,
                event.event_id,
                event.created_at,
            )
            .await?;

        let risk_level = Self::risk_level(attempt_count);

        if risk_level == RiskLevel::Low {
            return Ok(FraudDecision::allow());
        }

        let reason = format!(
            "failed login burst detected: {} attempts for user {} within the configured window",
            attempt_count, event.user
        );

        Ok(FraudDecision::trigger(
            risk_level,
            Self::RULE_NAME,
            FraudEvidence::new(Self::RULE_NAME, reason),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;
    use uuid::Uuid;

    struct FakeStateStore {
        counts: Mutex<Vec<i32>>,
    }

    impl FakeStateStore {
        fn new(counts: Vec<i32>) -> Self {
            Self {
                counts: Mutex::new(counts),
            }
        }
    }

    #[async_trait]
    impl FraudStateStore for FakeStateStore {
        async fn record_failed_login(
            &self,
            _tenant_id: &str,
            _username: &str,
            _event_id: Uuid,
            _occurred_at: DateTime<Utc>,
        ) -> Result<i32> {
            let mut counts = self.counts.lock().unwrap();

            if counts.is_empty() {
                return Ok(1);
            }

            Ok(counts.remove(0))
        }
    }

    fn event(action: &str) -> FraudEvent {
        FraudEvent::new(
            "tenant-test".to_string(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            "alice".to_string(),
            action.to_string(),
            Utc::now(),
        )
    }

    #[test]
    fn risk_thresholds_are_correct() {
        assert_eq!(FailedLoginBurstRule::risk_level(1), RiskLevel::Low);
        assert_eq!(FailedLoginBurstRule::risk_level(2), RiskLevel::Low);
        assert_eq!(FailedLoginBurstRule::risk_level(3), RiskLevel::Medium);
        assert_eq!(FailedLoginBurstRule::risk_level(4), RiskLevel::Medium);
        assert_eq!(FailedLoginBurstRule::risk_level(5), RiskLevel::High);
        assert_eq!(FailedLoginBurstRule::risk_level(9), RiskLevel::High);
        assert_eq!(FailedLoginBurstRule::risk_level(10), RiskLevel::Critical);
        assert_eq!(FailedLoginBurstRule::risk_level(100), RiskLevel::Critical);
    }

    #[tokio::test]
    async fn first_two_attempts_are_low() {
        let store = Arc::new(FakeStateStore::new(vec![1, 2]));
        let rule = FailedLoginBurstRule::new(store);

        let first = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();
        let second = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();

        assert_eq!(first.risk_level, RiskLevel::Low);
        assert_eq!(second.risk_level, RiskLevel::Low);
    }

    #[tokio::test]
    async fn third_attempt_is_medium() {
        let store = Arc::new(FakeStateStore::new(vec![3]));
        let rule = FailedLoginBurstRule::new(store);

        let decision = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Medium);
        assert_eq!(
            decision.triggered_rules,
            vec![FailedLoginBurstRule::RULE_NAME]
        );
        assert_eq!(decision.evidence.len(), 1);
    }

    #[tokio::test]
    async fn fifth_attempt_is_high() {
        let store = Arc::new(FakeStateStore::new(vec![5]));
        let rule = FailedLoginBurstRule::new(store);

        let decision = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::High);
    }

    #[tokio::test]
    async fn tenth_attempt_is_critical() {
        let store = Arc::new(FakeStateStore::new(vec![10]));
        let rule = FailedLoginBurstRule::new(store);

        let decision = rule.evaluate(&event("LOGIN_FAILED")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Critical);
    }

    #[tokio::test]
    async fn non_login_event_does_not_touch_state() {
        let store = Arc::new(FakeStateStore::new(Vec::new()));
        let rule = FailedLoginBurstRule::new(store);

        let decision = rule.evaluate(&event("VIEW_ACCOUNT")).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Low);
        assert!(decision.triggered_rules.is_empty());
    }
}
