use anyhow::Result;

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        println!("==============================");
        println!("KafkaProducer brokers = {}", brokers);
        println!("==============================");

        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;

        Ok(Self { producer })
    }

    pub async fn publish(&self, topic: &str, key: Option<&str>, payload: &str) -> Result<()> {
        self.producer
            .send(
                FutureRecord::to(topic)
                    .payload(payload)
                    .key(key.unwrap_or("")),
                Timeout::Never,
            )
            .await
            .map_err(|(error, _)| anyhow::anyhow!(error))?;

        Ok(())
    }
}
