use anyhow::Result;

use common::lifecycle::wait_for_shutdown;

use crate::worker::outbox_worker::OutboxWorker;

pub async fn run() -> Result<()> {
    let worker = OutboxWorker::new().await?;

    tokio::select! {
        result = worker.run() => {
            result?;
        }

        result = wait_for_shutdown() => {
            result?;
        }
    }

    Ok(())
}