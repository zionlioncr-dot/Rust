mod builders;
mod container;
mod handlers;
mod router;
mod service;
mod state;

use anyhow::Result;

use bootstrap::BootstrapBuilder;

use container::application_container::ApplicationContainer;

use telemetry::tracing::{init_tracing, shutdown_tracing};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    init_tracing()?;

    let application = BootstrapBuilder::new().build()?;

    let container = ApplicationContainer::build().await?;

    let state = container.state();

    let port = application.config().server_port;

    let app = router::router(state);

    http_server::start_with_router(port, app).await?;

    shutdown_tracing();

    Ok(())
}
