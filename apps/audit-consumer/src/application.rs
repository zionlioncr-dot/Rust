use anyhow::Result;

use common::lifecycle::wait_for_shutdown;

use crate::consumer::audit_consumer::AuditConsumer;

pub async fn run() -> Result<()> {
    let consumer = AuditConsumer::new().await?;

    tokio::select! {
        result = consumer.run() => {
            result?;
        }

        result = wait_for_shutdown() => {
            result?;
        }
    }

    Ok(())
}