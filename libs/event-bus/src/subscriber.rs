use anyhow::Result;

use domain::events::event_envelope::EventEnvelope;

use kafka::KafkaConsumer;

pub struct EventSubscriber {
    consumer: KafkaConsumer,
}

impl EventSubscriber {
    pub fn new(group: &str) -> Result<Self> {
        Ok(Self {
            consumer: KafkaConsumer::new(group)?,
        })
    }

    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer.subscribe(topic)
    }

    pub async fn listen<F, Fut>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(EventEnvelope) -> Fut,

        Fut: std::future::Future<Output = Result<()>>,
    {
        self.consumer
            .listen(|message| {
                let event: EventEnvelope = serde_json::from_str(&message.payload).unwrap();

                handler(event)
            })
            .await?;

        Ok(())
    }
}
