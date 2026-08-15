use anyhow::Result;

#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check(&self) -> Result<()>;

    fn name(&self) -> &'static str;
}
