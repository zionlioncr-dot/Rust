use anyhow::{Context, Result};

use apache_avro::{from_avro_datum, types::Value, Schema};

use futures_util::StreamExt;

use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::{Headers, Message},
    ClientConfig,
};

use serde_json::Value as JsonValue;

use crate::{event::KafkaEvent, schema_registry::SchemaRegistryClient};

pub struct KafkaConsumer {
    consumer: StreamConsumer,

    schema_registry: SchemaRegistryClient,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, group: &str, schema_registry_url: &str) -> Result<Self> {
        println!("==============================");

        println!("KafkaConsumer brokers = {}", brokers);

        println!("Group = {}", group);

        println!("Schema Registry = {}", schema_registry_url);

        println!("==============================");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

        let schema_registry = SchemaRegistryClient::new(schema_registry_url);

        Ok(Self {
            consumer,
            schema_registry,
        })
    }

    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer.subscribe(&[topic])?;

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
                            .and_then(|value| std::str::from_utf8(value).ok().map(String::from))
                    } else {
                        None
                    }
                })
            });

            tracing::debug!(
                traceparent = ?traceparent,
                "Kafka trace context extracted"
            );

            handler(KafkaEvent {
                key,
                payload,
                traceparent,
            })
            .await?;
        }

        Ok(())
    }

    pub async fn decode_avro(&self, payload: &[u8]) -> Result<JsonValue> {
        if payload.len() < 5 {
            anyhow::bail!(
                "Invalid Confluent Avro payload: expected at least 5 bytes, got {}",
                payload.len()
            );
        }

        let magic_byte = payload[0];

        if magic_byte != 0 {
            anyhow::bail!(
                "Invalid Confluent Avro magic byte: expected 0, got {}",
                magic_byte
            );
        }

        let schema_id = i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);

        tracing::debug!(schema_id, "Decoding Avro event using Schema Registry");

        let schema_json = self
            .schema_registry
            .get_schema_by_id(schema_id)
            .await
            .with_context(|| {
                format!(
                    "Failed to obtain schema with ID {} from Schema Registry",
                    schema_id
                )
            })?;

        let schema = Schema::parse_str(&schema_json).context("Failed to parse Avro schema")?;

        let mut reader = &payload[5..];

        let avro_value =
            from_avro_datum(&schema, &mut reader, None).context("Failed to decode Avro payload")?;

        let json_value =
            avro_to_json(&avro_value, &schema).context("Failed to convert Avro value to JSON")?;

        Ok(json_value)
    }
}

fn avro_to_json(value: &Value, schema: &Schema) -> Result<JsonValue> {
    match (value, schema) {
        (Value::Null, Schema::Null) => Ok(JsonValue::Null),

        (Value::Boolean(value), Schema::Boolean) => Ok(JsonValue::Bool(*value)),

        (Value::Int(value), Schema::Int) => Ok(JsonValue::Number((*value).into())),

        (Value::Long(value), Schema::Long) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::Float(value), Schema::Float) => {
            let number =
                serde_json::Number::from_f64(*value as f64).context("Invalid Avro float value")?;

            Ok(JsonValue::Number(number))
        }

        (Value::Double(value), Schema::Double) => {
            let number =
                serde_json::Number::from_f64(*value).context("Invalid Avro double value")?;

            Ok(JsonValue::Number(number))
        }

        (Value::String(value), Schema::String) => Ok(JsonValue::String(value.clone())),

        (Value::Bytes(value), Schema::Bytes) => {
            let array = value
                .iter()
                .map(|byte| JsonValue::Number(serde_json::Number::from(*byte)))
                .collect();

            Ok(JsonValue::Array(array))
        }

        (Value::Fixed(_, value), Schema::Fixed(_)) => {
            let array = value
                .iter()
                .map(|byte| JsonValue::Number(serde_json::Number::from(*byte)))
                .collect();

            Ok(JsonValue::Array(array))
        }

        (Value::Enum(index, symbol), Schema::Enum(_)) => {
            let _ = index;

            Ok(JsonValue::String(symbol.clone()))
        }

        (Value::Array(values), Schema::Array(array_schema)) => {
            let mut result = Vec::with_capacity(values.len());

            for value in values {
                result.push(avro_to_json(value, array_schema.items.as_ref())?);
            }

            Ok(JsonValue::Array(result))
        }

        (Value::Map(values), Schema::Map(map_schema)) => {
            let mut result = serde_json::Map::new();

            for (key, value) in values {
                result.insert(key.clone(), avro_to_json(value, map_schema.types.as_ref())?);
            }

            Ok(JsonValue::Object(result))
        }

        (Value::Record(fields), Schema::Record(record_schema)) => {
            let mut result = serde_json::Map::new();

            for (name, value) in fields {
                let field_schema = record_schema
                    .fields
                    .iter()
                    .find(|field| field.name == *name)
                    .map(|field| &field.schema)
                    .context(format!("Field '{}' not found in Avro schema", name))?;

                result.insert(name.clone(), avro_to_json(value, field_schema)?);
            }

            Ok(JsonValue::Object(result))
        }

        (Value::Union(_, value), Schema::Union(union_schema)) => {
            for branch in union_schema.variants() {
                if let Ok(json) = avro_to_json(value, branch) {
                    return Ok(json);
                }
            }

            anyhow::bail!("Could not convert Avro union value to JSON")
        }

        (Value::Date(value), Schema::Date) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::TimeMillis(value), Schema::TimeMillis) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::TimeMicros(value), Schema::TimeMicros) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::TimestampMillis(value), Schema::TimestampMillis) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::TimestampMicros(value), Schema::TimestampMicros) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::TimestampNanos(value), Schema::TimestampNanos) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::LocalTimestampMillis(value), Schema::LocalTimestampMillis) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::LocalTimestampMicros(value), Schema::LocalTimestampMicros) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::LocalTimestampNanos(value), Schema::LocalTimestampNanos) => {
            Ok(JsonValue::Number(serde_json::Number::from(*value)))
        }

        (Value::Decimal(value), Schema::Decimal(_)) => {
            Ok(JsonValue::String(format!("{:?}", value)))
        }

        _ => {
            anyhow::bail!(
                "Unsupported Avro value/schema combination: value={:?}, schema={:?}",
                value,
                schema
            )
        }
    }
}
