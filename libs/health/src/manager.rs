use std::sync::Arc;

use anyhow::Result;

use crate::{
    checker::HealthCheck,
    status::{CheckStatus, HealthStatus},
};

pub struct HealthManager {
    checks: Vec<Arc<dyn HealthCheck>>,
}

impl HealthManager {
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    pub fn register<T>(mut self, check: T) -> Self
    where
        T: HealthCheck + 'static,
    {
        self.checks.push(Arc::new(check));

        self
    }

    pub async fn check(&self) -> Result<HealthStatus> {
        let mut ready = true;

        let mut checks = Vec::new();

        for checker in &self.checks {
            match checker.check().await {
                Ok(_) => {
                    checks.push(CheckStatus {
                        name: checker.name().to_string(),
                        status: "UP".to_string(),
                    });
                }

                Err(_) => {
                    ready = false;

                    checks.push(CheckStatus {
                        name: checker.name().to_string(),
                        status: "DOWN".to_string(),
                    });
                }
            }
        }

        Ok(HealthStatus { ready, checks })
    }
}
