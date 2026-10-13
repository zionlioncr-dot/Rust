use serde::{Deserialize, Serialize};

use super::risk_level::RiskLevel;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FraudEvidence {
    pub rule: String,
    pub reason: String,
}

impl FraudEvidence {
    pub fn new(rule: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            rule: rule.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FraudDecision {
    pub risk_level: RiskLevel,
    pub triggered_rules: Vec<String>,
    pub evidence: Vec<FraudEvidence>,
}

impl FraudDecision {
    pub fn allow() -> Self {
        Self {
            risk_level: RiskLevel::Low,
            triggered_rules: Vec::new(),
            evidence: Vec::new(),
        }
    }

    pub fn trigger(
        risk_level: RiskLevel,
        rule: impl Into<String>,
        evidence: FraudEvidence,
    ) -> Self {
        Self {
            risk_level,
            triggered_rules: vec![rule.into()],
            evidence: vec![evidence],
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.risk_level = self.risk_level.max(other.risk_level);

        self.triggered_rules.extend(other.triggered_rules);
        self.evidence.extend(other.evidence);
    }

    pub fn is_suspicious(&self) -> bool {
        self.risk_level >= RiskLevel::Medium
    }
}
