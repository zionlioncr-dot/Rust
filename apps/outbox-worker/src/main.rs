mod config;
mod publisher;
mod scheduler;
mod worker;

use anyhow::Result;

use worker::OutboxWorker;

use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    let worker = OutboxWorker::new().await?;

    worker.run().await?;

    Ok(())
}
