use anyhow::{Context, Result};
use apache_avro::{types::Value, Schema};
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use serde_json::Value as JsonValue;

use crate::schema_registry::SchemaRegistryClient;

pub struct KafkaProducer {
    producer: FutureProducer,
    schema_registry: SchemaRegistryClient,
}

impl KafkaProducer {
    pub fn new(brokers: &str, schema_registry_url: &str) -> Result<Self> {
        println!("==============================");
        println!("KafkaProducer brokers = {}", brokers);
        println!("Schema Registry = {}", schema_registry_url);
        println!("==============================");

        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;

        let schema_registry = SchemaRegistryClient::new(schema_registry_url);

        Ok(Self {
            producer,
            schema_registry,
        })
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

    pub async fn publish_with_schema(
        &self,
        topic: &str,
        key: Option<&str>,
        subject: &str,
        payload: &str,
        traceparent: Option<&str>,
    ) -> Result<()> {
        // ------------------------------------------------------------
        // 1. Parse JSON payload
        // ------------------------------------------------------------
        let json_value: JsonValue =
            serde_json::from_str(payload).context("Failed to parse payload as JSON")?;

        // ------------------------------------------------------------
        // 2. Resolve latest schema from Schema Registry
        // ------------------------------------------------------------
        let schema_response = self.schema_registry.get_latest_schema(subject).await?;

        println!(
            "Schema Registry schema resolved subject={} schema_version={} schema_id={}",
            subject, schema_response.version, schema_response.id
        );

        // ------------------------------------------------------------
        // 3. Convert schema JSON -> apache-avro Schema
        // ------------------------------------------------------------
        let schema = apache_avro::Schema::parse_str(&schema_response.schema)
            .context("Failed to parse Avro schema")?;

        // ------------------------------------------------------------
        // 4. Convert JSON -> Avro Value
        // ------------------------------------------------------------
        let avro_value = Self::json_to_avro(&schema, &json_value)?;

        // ------------------------------------------------------------
        // 5. Encode Avro datum
        // ------------------------------------------------------------
        let avro_payload = apache_avro::to_avro_datum(&schema, avro_value)
            .context("Failed to encode Avro datum")?;

        // ------------------------------------------------------------
        // 6. Schema ID
        // ------------------------------------------------------------
        let schema_id = schema_response.id;

        // ------------------------------------------------------------
        // 7. Build Confluent wire format
        //
        // Byte 0      = magic byte
        // Bytes 1..5  = schema ID, big-endian
        // Bytes 5..   = Avro binary payload
        // ------------------------------------------------------------
        let mut final_payload = Vec::with_capacity(5 + avro_payload.len());

        final_payload.push(0u8);

        final_payload.extend_from_slice(&schema_id.to_be_bytes());

        final_payload.extend_from_slice(&avro_payload);

        // ------------------------------------------------------------
        // 8. Build Kafka record
        // ------------------------------------------------------------
        let mut record = FutureRecord::to(topic)
            .payload(&final_payload)
            .key(key.unwrap_or(""));

        // ------------------------------------------------------------
        // 9. Propagate W3C traceparent
        // ------------------------------------------------------------
        if let Some(traceparent) = traceparent {
            let headers = OwnedHeaders::new().insert(Header {
                key: "traceparent",
                value: Some(traceparent),
            });

            record = record.headers(headers);
        }

        // ------------------------------------------------------------
        // 10. Publish to Kafka / Redpanda
        // ------------------------------------------------------------
        self.producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(error, _)| anyhow::anyhow!("Kafka publish failed: {}", error))?;

        Ok(())
    }

    // ====================================================================
    // JSON -> Avro Value
    // ====================================================================

    fn json_to_avro(schema: &Schema, json: &JsonValue) -> Result<Value> {
        use std::collections::HashMap;

        match schema {
            // ------------------------------------------------------------
            // Null
            // ------------------------------------------------------------
            Schema::Null => {
                if json.is_null() {
                    Ok(Value::Null)
                } else {
                    anyhow::bail!("Expected null, got {}", json)
                }
            }

            // ------------------------------------------------------------
            // Boolean
            // ------------------------------------------------------------
            Schema::Boolean => match json {
                JsonValue::Bool(value) => Ok(Value::Boolean(*value)),

                _ => anyhow::bail!("Expected boolean, got {}", json),
            },

            // ------------------------------------------------------------
            // String
            // ------------------------------------------------------------
            Schema::String => match json {
                JsonValue::String(value) => Ok(Value::String(value.clone())),

                _ => anyhow::bail!("Expected string, got {}", json),
            },

            // ------------------------------------------------------------
            // Int
            // ------------------------------------------------------------
            Schema::Int => match json {
                JsonValue::Number(number) => {
                    let value = number
                        .as_i64()
                        .context("Expected integer-compatible JSON number")?;

                    let value =
                        i32::try_from(value).context("JSON number is outside Avro Int range")?;

                    Ok(Value::Int(value))
                }

                _ => anyhow::bail!("Expected integer, got {}", json),
            },

            // ------------------------------------------------------------
            // Long
            // ------------------------------------------------------------
            Schema::Long => match json {
                JsonValue::Number(number) => {
                    let value = number
                        .as_i64()
                        .context("Expected integer-compatible JSON number")?;

                    Ok(Value::Long(value))
                }

                _ => anyhow::bail!("Expected long integer, got {}", json),
            },

            // ------------------------------------------------------------
            // Float
            // ------------------------------------------------------------
            Schema::Float => match json {
                JsonValue::Number(number) => {
                    let value = number.as_f64().context("Expected numeric JSON value")?;

                    Ok(Value::Float(value as f32))
                }

                _ => anyhow::bail!("Expected float, got {}", json),
            },

            // ------------------------------------------------------------
            // Double
            // ------------------------------------------------------------
            Schema::Double => match json {
                JsonValue::Number(number) => {
                    let value = number.as_f64().context("Expected numeric JSON value")?;

                    Ok(Value::Double(value))
                }

                _ => anyhow::bail!("Expected double, got {}", json),
            },

            // ------------------------------------------------------------
            // Bytes
            // ------------------------------------------------------------
            Schema::Bytes => match json {
                JsonValue::String(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),

                JsonValue::Array(values) => {
                    let mut bytes = Vec::with_capacity(values.len());

                    for value in values {
                        let number = value.as_u64().context("Expected byte value")?;

                        let byte = u8::try_from(number).context("Byte value outside u8 range")?;

                        bytes.push(byte);
                    }

                    Ok(Value::Bytes(bytes))
                }

                _ => anyhow::bail!("Expected string or byte array, got {}", json),
            },

            // ------------------------------------------------------------
            // Array
            // ------------------------------------------------------------
            Schema::Array(array_schema) => match json {
                JsonValue::Array(values) => {
                    let item_schema = &array_schema.items;

                    let mut items = Vec::with_capacity(values.len());

                    for value in values {
                        items.push(Self::json_to_avro(item_schema, value)?);
                    }

                    Ok(Value::Array(items))
                }

                _ => anyhow::bail!("Expected JSON array, got {}", json),
            },

            // ------------------------------------------------------------
            // Map
            // ------------------------------------------------------------
            Schema::Map(map_schema) => match json {
                JsonValue::Object(values) => {
                    let value_schema = map_schema.types.as_ref();

                    let mut entries = HashMap::with_capacity(values.len());

                    for (key, value) in values {
                        entries.insert(key.clone(), Self::json_to_avro(value_schema, value)?);
                    }

                    Ok(Value::Map(entries))
                }

                _ => anyhow::bail!("Expected JSON object for Avro map, got {}", json),
            },

            // ------------------------------------------------------------
            // Record
            // ------------------------------------------------------------
            Schema::Record(record_schema) => {
                let object = json
                    .as_object()
                    .context("Expected JSON object for Avro record")?;

                let mut record = Vec::new();

                for field in &record_schema.fields {
                    let value = object
                        .get(&field.name)
                        .with_context(|| format!("Missing required field '{}'", field.name))?;

                    let avro_value = Self::json_to_avro(&field.schema, value)?;

                    record.push((field.name.clone(), avro_value));
                }

                Ok(Value::Record(record))
            }

            // ------------------------------------------------------------
            // Enum
            // ------------------------------------------------------------
            Schema::Enum(enum_schema) => {
                let symbol = json.as_str().context("Expected enum symbol string")?;

                let symbols = &enum_schema.symbols;

                let index = symbols
                    .iter()
                    .position(|value| value == symbol)
                    .with_context(|| format!("Invalid enum symbol '{}'", symbol))?;

                Ok(Value::Enum(index as u32, symbol.to_string()))
            }

            // ------------------------------------------------------------
            // Fixed
            // ------------------------------------------------------------
            Schema::Fixed(fixed_schema) => {
                let string = json.as_str().context("Expected string for Avro fixed")?;

                let bytes = string.as_bytes().to_vec();

                let size = fixed_schema.size;

                if bytes.len() != size {
                    anyhow::bail!(
                        "Fixed value must contain exactly {} bytes, got {}",
                        size,
                        bytes.len()
                    );
                }

                Ok(Value::Fixed(size, bytes))
            }

            // ------------------------------------------------------------
            // Union
            // ------------------------------------------------------------
            Schema::Union(union_schema) => {
                for variant in union_schema.variants() {
                    if let Ok(value) = Self::json_to_avro(variant, json) {
                        return Ok(value);
                    }
                }

                anyhow::bail!("JSON value {} does not match any Avro union variant", json)
            }

            // ------------------------------------------------------------
            // Reference
            // ------------------------------------------------------------
            Schema::Ref { name } => {
                anyhow::bail!("Unresolved Avro schema reference: {}", name)
            }

            _ => {
                anyhow::bail!(
                    "Unsupported Avro schema type for JSON conversion: {:?}",
                    schema
                )
            }
        }
    }
}
