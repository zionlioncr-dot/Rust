mod config;
mod container;
mod http;
mod publisher;
mod worker;

use anyhow::Result;

use telemetry::tracing::init_tracing;

use worker::outbox_worker::OutboxWorker;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    tokio::spawn(async {
        if let Err(err) = http::http_server::start().await {
            tracing::error!("{:?}", err);
        }
    });

    let worker = OutboxWorker::new().await?;

    worker.run().await?;

    Ok(())
}