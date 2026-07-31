mod application;
mod builders;
mod container;
mod handlers;
mod router;
mod service;
mod state;

use anyhow::Result;

use container::application_container::ApplicationContainer;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    telemetry::tracing::init_tracing()?;

    let container = ApplicationContainer::build().await?;

    application::run(container.state()).await?;

    Ok(())
}
