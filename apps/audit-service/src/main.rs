mod builders;
mod handlers;
mod router;
mod service;
mod state;

use std::{net::SocketAddr, sync::Arc};

use sqlx::postgres::PgPoolOptions;

use repository::PostgresRepository;

use repository::{audit_repository::AuditRepository, outbox_repository::OutboxRepository};

use service::audit_service::AuditService;
use state::AppState;

use anyhow::Result;

use telemetry::tracing::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    telemetry::tracing::init_tracing()?;

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Cannot connect database");

    let postgres = Arc::new(PostgresRepository::new(pool));

    let audit_repository: Arc<dyn AuditRepository> = postgres.clone();

    let outbox_repository: Arc<dyn OutboxRepository> = postgres.clone();

    let audit_service = AuditService::new(audit_repository, outbox_repository);

    let state = AppState {
        audit_service: Arc::new(audit_service),
    };

    let app = router::create_router(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    println!("Listening {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    axum::serve(listener, app).await;

    Ok(())
}
