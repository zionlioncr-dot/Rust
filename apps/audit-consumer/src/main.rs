mod consumer;
mod container;
mod dispatcher;
mod handler;
mod modules;
mod retry;
mod service;

use anyhow::Result;

use consumer::audit_consumer::AuditConsumer;

use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    let consumer = AuditConsumer::new().await?;

    consumer.run().await?;

    Ok(())
}
