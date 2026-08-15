use anyhow::Result;

use tracing::info;

use crate::Application;

pub struct Runtime {
    application: Application,
}

impl Runtime {
    pub fn new(application: Application) -> Self {
        Self { application }
    }

    pub async fn start(self, port: u16) -> Result<()> {
        info!(port = port, "Starting application runtime");

        self.application.start_http(port).await?;

        Ok(())
    }
}
