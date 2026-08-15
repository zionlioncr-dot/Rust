use anyhow::Result;

use async_trait::async_trait;

use sqlx::PgPool;

use crate::checker::HealthCheck;

pub struct PostgresChecker {
    pool: PgPool,
}

impl PostgresChecker {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl HealthCheck for PostgresChecker {
    async fn check(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;

        Ok(())
    }

    fn name(&self) -> &'static str {
        "postgres"
    }
}
