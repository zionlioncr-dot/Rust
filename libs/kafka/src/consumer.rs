use anyhow::Result;
use apache_avro::types::Value as AvroValue;
use futures_util::StreamExt;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Headers,
    Message,
};

use crate::{event::KafkaEvent, schema_registry::SchemaRegistryClient};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    schema_registry: SchemaRegistryClient,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, group: &str, schema_registry_url: &str) -> Result<Self> {
        let consumer: StreamConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

        Ok(Self {
            consumer,
            schema_registry: SchemaRegistryClient::new(schema_registry_url),
        })
    }

    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.consumer.subscribe(topics)?;

        Ok(())
    }

    pub async fn listen<F, Fut>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(KafkaEvent) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut stream = self.consumer.stream();

        while let Some(message) = stream.next().await {
            let message = message?;

            let payload = message
                .payload()
                .map(|value| value.to_vec())
                .unwrap_or_default();

            let key = match message.key_view::<str>() {
                Some(Ok(value)) => Some(value.to_string()),
                Some(Err(_)) => None,
                None => None,
            };

            let traceparent = message.headers().and_then(|headers| {
                headers.iter().find_map(|header| {
                    if header.key == "traceparent" {
                        header
                            .value
                            .and_then(|value| std::str::from_utf8(value).ok().map(str::to_owned))
                    } else {
                        None
                    }
                })
            });

            handler(KafkaEvent {
                key,
                payload,
                traceparent,
            })
            .await?;
        }

        Ok(())
    }

    pub async fn decode_avro(&self, payload: &[u8]) -> Result<serde_json::Value> {
        if payload.len() < 5 {
            anyhow::bail!("Invalid Confluent Avro payload: less than 5 bytes");
        }

        let magic_byte = payload[0];

        if magic_byte != 0 {
            anyhow::bail!("Invalid Confluent Avro magic byte: {}", magic_byte);
        }

        // Confluent wire format:
        //
        // Byte 0      = magic byte
        // Bytes 1..5  = schema ID, big-endian
        // Bytes 5..   = Avro binary payload
        let schema_id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);

        let schema_id_i32 = i32::try_from(schema_id)
            .map_err(|_| anyhow::anyhow!("Schema ID {} exceeds i32 range", schema_id))?;

        let schema_json = self.schema_registry.get_schema_by_id(schema_id_i32).await?;

        let schema = apache_avro::Schema::parse_str(&schema_json)?;

        let value = apache_avro::from_avro_datum(&schema, &mut &payload[5..], None)?;

        Ok(avro_value_to_json(&value))
    }
}

fn avro_value_to_json(value: &AvroValue) -> serde_json::Value {
    use serde_json::{Map, Number, Value};

    match value {
        AvroValue::Null => Value::Null,

        AvroValue::Boolean(value) => Value::Bool(*value),

        AvroValue::Int(value) => Value::Number(Number::from(*value)),

        AvroValue::Long(value) => Value::Number(Number::from(*value)),

        AvroValue::Float(value) => Number::from_f64(*value as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),

        AvroValue::Double(value) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),

        AvroValue::Bytes(bytes) => Value::Array(
            bytes
                .iter()
                .map(|byte| Value::Number(Number::from(*byte)))
                .collect(),
        ),

        AvroValue::String(value) => Value::String(value.clone()),

        AvroValue::Fixed(_, bytes) => Value::Array(
            bytes
                .iter()
                .map(|byte| Value::Number(Number::from(*byte)))
                .collect(),
        ),

        AvroValue::Enum(_, symbol) => Value::String(symbol.clone()),

        AvroValue::Array(values) => Value::Array(values.iter().map(avro_value_to_json).collect()),

        AvroValue::Map(values) => {
            let mut object = Map::new();

            for (key, value) in values {
                object.insert(key.clone(), avro_value_to_json(value));
            }

            Value::Object(object)
        }

        AvroValue::Record(fields) => {
            let mut object = Map::new();

            for (name, value) in fields {
                object.insert(name.clone(), avro_value_to_json(value));
            }

            Value::Object(object)
        }

        AvroValue::Union(_, value) => avro_value_to_json(value),

        AvroValue::Date(value) => Value::Number(Number::from(*value)),

        AvroValue::TimeMillis(value) => Value::Number(Number::from(*value)),

        AvroValue::TimeMicros(value) => Value::Number(Number::from(*value)),

        AvroValue::TimestampMillis(value) => Value::Number(Number::from(*value)),

        AvroValue::TimestampMicros(value) => Value::Number(Number::from(*value)),

        AvroValue::TimestampNanos(value) => Value::Number(Number::from(*value)),

        AvroValue::LocalTimestampMillis(value) => Value::Number(Number::from(*value)),

        AvroValue::LocalTimestampMicros(value) => Value::Number(Number::from(*value)),

        AvroValue::LocalTimestampNanos(value) => Value::Number(Number::from(*value)),

        AvroValue::Duration(duration) => Value::String(format!("{:?}", duration)),

        AvroValue::Decimal(decimal) => Value::String(format!("{:?}", decimal)),

        AvroValue::Uuid(uuid) => Value::String(uuid.to_string()),

        AvroValue::BigDecimal(value) => Value::String(format!("{:?}", value)),
    }
}
