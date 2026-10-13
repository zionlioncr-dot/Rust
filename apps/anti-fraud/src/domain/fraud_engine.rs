use anyhow::Result;

use super::{fraud_decision::FraudDecision, fraud_event::FraudEvent, fraud_rule::FraudRule};

pub struct FraudEngine {
    rules: Vec<Box<dyn FraudRule>>,
}

impl FraudEngine {
    pub fn new(rules: Vec<Box<dyn FraudRule>>) -> Self {
        Self { rules }
    }

    pub async fn evaluate(&self, event: &FraudEvent) -> Result<FraudDecision> {
        let mut decision = FraudDecision::allow();

        for rule in &self.rules {
            let rule_decision = rule.evaluate(event).await?;
            decision.merge(rule_decision);
        }

        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::{
        fraud_decision::{FraudDecision, FraudEvidence},
        risk_level::RiskLevel,
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use uuid::Uuid;

    struct AlwaysHighRule;

    #[async_trait]
    impl FraudRule for AlwaysHighRule {
        fn name(&self) -> &'static str {
            "always_high"
        }

        async fn evaluate(&self, _event: &FraudEvent) -> Result<FraudDecision> {
            Ok(FraudDecision::trigger(
                RiskLevel::High,
                self.name(),
                FraudEvidence::new(self.name(), "test rule triggered"),
            ))
        }
    }

    struct AlwaysMediumRule;

    #[async_trait]
    impl FraudRule for AlwaysMediumRule {
        fn name(&self) -> &'static str {
            "always_medium"
        }

        async fn evaluate(&self, _event: &FraudEvent) -> Result<FraudDecision> {
            Ok(FraudDecision::trigger(
                RiskLevel::Medium,
                self.name(),
                FraudEvidence::new(self.name(), "second test rule triggered"),
            ))
        }
    }

    fn event() -> FraudEvent {
        FraudEvent::new(
            "tenant-test".to_string(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            "alice".to_string(),
            "TEST".to_string(),
            Utc::now(),
        )
    }

    #[tokio::test]
    async fn returns_low_when_no_rules_trigger() {
        let engine = FraudEngine::new(Vec::new());

        let decision = engine.evaluate(&event()).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::Low);
        assert!(decision.triggered_rules.is_empty());
        assert!(decision.evidence.is_empty());
        assert!(!decision.is_suspicious());
    }

    #[tokio::test]
    async fn combines_multiple_rule_decisions() {
        let engine = FraudEngine::new(vec![Box::new(AlwaysMediumRule), Box::new(AlwaysHighRule)]);

        let decision = engine.evaluate(&event()).await.unwrap();

        assert_eq!(decision.risk_level, RiskLevel::High);
        assert_eq!(decision.triggered_rules.len(), 2);
        assert_eq!(decision.evidence.len(), 2);
        assert!(decision.is_suspicious());
    }
}
