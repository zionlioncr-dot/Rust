use anyhow::Result;

pub async fn start() -> Result<()> {
    http_server::start(3002).await
}