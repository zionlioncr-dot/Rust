mod config;
mod consumer;
mod domain;
mod http;
mod repository;
mod state;

use std::sync::Arc;

use anyhow::Result;
use common::database::create_pool;

use config::FraudConfig;
use consumer::fraud_consumer::FraudConsumer;
use http::AppState;
use repository::{
    fraud_alert_repository::FraudAlertRepository,
    postgres_fraud_alert_repository::PostgresFraudAlertRepository,
};
use state::{
    fraud_state_store::FraudStateStore, postgres_fraud_state_store::PostgresFraudStateStore,
};
use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    tracing::info!("ANTI_FRAUD: service starting");

    let config = FraudConfig::load();

    tracing::info!(
        max_db_connections = config.app.max_db_connections,
        "ANTI_FRAUD_INIT: creating PostgreSQL pool"
    );

    let pool = create_pool(config.app.max_db_connections).await?;

    tracing::info!("ANTI_FRAUD_INIT: PostgreSQL pool created");

    let alert_repository: Arc<dyn FraudAlertRepository> =
        Arc::new(PostgresFraudAlertRepository::new(pool.clone()));

    tracing::info!("ANTI_FRAUD_INIT: fraud alert repository created");

    let state_store: Arc<dyn FraudStateStore> = Arc::new(PostgresFraudStateStore::new(pool));

    tracing::info!("ANTI_FRAUD_INIT: fraud state store created");

    let http_state = AppState {
        alert_repository: alert_repository.clone(),
    };

    let consumer = FraudConsumer::new(config.clone(), alert_repository, state_store).await?;

    let app = http::router(http_state);

    let bind_address = format!("0.0.0.0:{}", config.port);

    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

    tracing::info!(
        address = %bind_address,
        "ANTI_FRAUD_HTTP: listening"
    );

    let http_task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(
                error = %error,
                "ANTI_FRAUD_HTTP: server stopped"
            );
        }
    });

    let consumer_task = tokio::spawn(async move {
        if let Err(error) = consumer.run().await {
            tracing::error!(
                error = %error,
                "ANTI_FRAUD_RUN: consumer stopped"
            );
        }
    });

    tokio::select! {
        _ = http_task => {
            tracing::error!("ANTI_FRAUD_HTTP: HTTP task exited");
        }

        _ = consumer_task => {
            tracing::error!("ANTI_FRAUD_RUN: consumer task exited");
        }
    }

    Ok(())
}
