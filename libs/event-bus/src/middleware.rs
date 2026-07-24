use anyhow::Result;

use tracing::{error, info};

use crate::EventEnvelope;

pub async fn execute_with_logging<F, Fut>(event: EventEnvelope, handler: F) -> Result<()>
where
    F: FnOnce(EventEnvelope) -> Fut,

    Fut: std::future::Future<Output = Result<()>>,
{
    info!(

        event_id=%event.id,

        event_type=%event.event_type,

        "Processing event"

    );

    match handler(event.clone()).await {
        Ok(_) => {
            info!(

                event_id=%event.id,

                "Event processed"

            );

            Ok(())
        }

        Err(err) => {
            error!(

                event_id=%event.id,

                error=%err,

                "Event processing failed"

            );

            Err(err)
        }
    }
}
