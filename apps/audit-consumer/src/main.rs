mod config;
mod consumer;
mod container;
mod dispatcher;
mod handler;
mod http;
mod modules;
mod retry;
mod router;
mod schema;
mod service;

use anyhow::Result;

use consumer::audit_consumer::AuditConsumer;

use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    //
    // HTTP Server
    //

    tokio::spawn(async {
        if let Err(e) = http::http_server::start().await {
            tracing::error!("{:?}", e);
        }
    });

    //
    // Kafka Consumer
    //

    let consumer = AuditConsumer::new().await?;

    consumer.run().await?;

    Ok(())
}