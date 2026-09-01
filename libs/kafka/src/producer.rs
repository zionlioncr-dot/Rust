use anyhow::{Context, Result};

use apache_avro::{
to_avro_datum,
types::Value,
Schema,
};

use rdkafka::{
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
pub fn new(
brokers: &str,
schema_registry_url: &str,
) -> Result<Self> {
println!("==============================");
println!(
"KafkaProducer brokers = {}",
brokers
);
println!(
"Schema Registry = {}",
schema_registry_url
);
println!("==============================");


    let producer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;

    let schema_registry =
        SchemaRegistryClient::new(schema_registry_url);

    Ok(Self {
        producer,
        schema_registry,
    })
}

pub async fn publish(
    &self,
    topic: &str,
    key: Option<&str>,
    payload: &str,
) -> Result<()> {
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
) -> Result<()> {
    let schema_response = self
        .schema_registry
        .get_latest_schema(subject)
        .await
        .context(
            "Failed to obtain schema from Schema Registry",
        )?;

    let schema = Schema::parse_str(
        &schema_response.schema,
    )
    .context("Failed to parse Avro schema")?;

    let json_value: JsonValue =
        serde_json::from_str(payload)
            .context("Failed to parse payload JSON")?;

    let avro_value = json_to_avro(
        &json_value,
        &schema,
    )
    .context(
        "Failed to convert JSON payload to Avro value",
    )?;

    if !avro_value.validate(&schema) {
        anyhow::bail!(
            "JSON payload does not match Avro schema for subject '{}'",
            subject
        );
    }

    /*
     * IMPORTANT:
     *
     * Do NOT use apache_avro::Writer here.
     *
     * Writer creates an Avro Object Container File (OCF),
     * which starts with the "Obj" header.
     *
     * Confluent Schema Registry wire format requires:
     *
     *   byte 0      = magic byte 0
     *   bytes 1..5  = schema ID, big endian
     *   bytes 5..   = Avro binary datum
     *
     * Therefore we use to_avro_datum().
     */

    let avro_payload = to_avro_datum(
    &schema,
    avro_value,
    )
    .context("Failed to serialize Avro payload")?;

    let schema_id = schema_response.id;

    let mut wire_payload = Vec::with_capacity(
        5 + avro_payload.len(),
    );

    // Confluent wire format.
    //
    // [0]       = magic byte
    // [1..5]    = schema ID, big endian
    // [5..]     = Avro binary datum

    wire_payload.push(0u8);

    wire_payload.extend_from_slice(
        &schema_id.to_be_bytes(),
    );

    wire_payload.extend_from_slice(
        &avro_payload,
    );

    tracing::debug!(
        subject = subject,
        schema_id = schema_id,
        payload_size = wire_payload.len(),
        "Publishing Avro message using Confluent wire format"
    );

    self.producer
        .send(
            FutureRecord::to(topic)
                .payload(&wire_payload)
                .key(key.unwrap_or("")),
            Timeout::Never,
        )
        .await
        .map_err(|(error, _)| {
            anyhow::anyhow!(error)
        })?;

    Ok(())
}


}

// ====================================================================
// JSON -> Avro Value
// ====================================================================

fn json_to_avro(
json: &JsonValue,
schema: &Schema,
) -> Result<Value> {
match schema {
// ------------------------------------------------------------
// Null
// ------------------------------------------------------------


    Schema::Null => {
        if json.is_null() {
            Ok(Value::Null)
        } else {
            anyhow::bail!(
                "Expected null, got {}",
                json
            )
        }
    }

    // ------------------------------------------------------------
    // Boolean
    // ------------------------------------------------------------

    Schema::Boolean => {
        match json {
            JsonValue::Bool(value) => {
                Ok(Value::Boolean(*value))
            }

            _ => anyhow::bail!(
                "Expected boolean, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // String
    // ------------------------------------------------------------

    Schema::String => {
        match json {
            JsonValue::String(value) => {
                Ok(Value::String(value.clone()))
            }

            _ => anyhow::bail!(
                "Expected string, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Int
    // ------------------------------------------------------------

    Schema::Int => {
        match json {
            JsonValue::Number(number) => {
                let value = number
                    .as_i64()
                    .context(
                        "Expected integer-compatible JSON number",
                    )?;

                let value =
                    i32::try_from(value)
                        .context(
                            "JSON number is outside Avro Int range",
                        )?;

                Ok(Value::Int(value))
            }

            _ => anyhow::bail!(
                "Expected integer, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Long
    // ------------------------------------------------------------

    Schema::Long => {
        match json {
            JsonValue::Number(number) => {
                let value = number
                    .as_i64()
                    .context(
                        "Expected integer-compatible JSON number",
                    )?;

                Ok(Value::Long(value))
            }

            _ => anyhow::bail!(
                "Expected long integer, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Float
    // ------------------------------------------------------------

    Schema::Float => {
        match json {
            JsonValue::Number(number) => {
                let value = number
                    .as_f64()
                    .context(
                        "Expected numeric JSON value",
                    )?;

                Ok(Value::Float(value as f32))
            }

            _ => anyhow::bail!(
                "Expected float, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Double
    // ------------------------------------------------------------

    Schema::Double => {
        match json {
            JsonValue::Number(number) => {
                let value = number
                    .as_f64()
                    .context(
                        "Expected numeric JSON value",
                    )?;

                Ok(Value::Double(value))
            }

            _ => anyhow::bail!(
                "Expected double, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Bytes
    // ------------------------------------------------------------

    Schema::Bytes => {
        match json {
            JsonValue::String(value) => {
                Ok(Value::Bytes(
                    value.as_bytes().to_vec(),
                ))
            }

            JsonValue::Array(values) => {
                let mut bytes =
                    Vec::with_capacity(values.len());

                for value in values {
                    let number = value
                        .as_u64()
                        .context(
                            "Expected byte-compatible integer",
                        )?;

                    let byte =
                        u8::try_from(number)
                            .context(
                                "JSON byte value is outside u8 range",
                            )?;

                    bytes.push(byte);
                }

                Ok(Value::Bytes(bytes))
            }

            _ => anyhow::bail!(
                "Expected string or array for Avro bytes, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Array
    // ------------------------------------------------------------

    Schema::Array(array_schema) => {
        match json {
            JsonValue::Array(values) => {
                let mut result =
                    Vec::with_capacity(values.len());

                for value in values {
                    let avro_value =
                        json_to_avro(
                            value,
                            array_schema.items.as_ref(),
                        )
                        .context(
                            "Failed to convert Avro array item",
                        )?;

                    result.push(avro_value);
                }

                Ok(Value::Array(result))
            }

            _ => anyhow::bail!(
                "Expected array, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Map
    // ------------------------------------------------------------

    Schema::Map(map_schema) => {
        match json {
            JsonValue::Object(object) => {
                let mut result =
                    std::collections::HashMap::new();

                for (key, value) in object {
                    let avro_value =
                        json_to_avro(
                            value,
                            &map_schema.types,
                        )
                        .with_context(|| {
                            format!(
                                "Failed to convert map value for key '{}'",
                                key
                            )
                        })?;

                    result.insert(
                        key.clone(),
                        avro_value,
                    );
                }

                Ok(Value::Map(result))
            }

            _ => anyhow::bail!(
                "Expected object/map, got {}",
                json
            ),
        }
    }

    // ------------------------------------------------------------
    // Record
    // ------------------------------------------------------------

    Schema::Record(record_schema) => {
        let object = json
            .as_object()
            .context(
                "Expected JSON object for Avro record",
            )?;

        let mut fields =
            Vec::with_capacity(
                record_schema.fields.len(),
            );

        for field in &record_schema.fields {
            let value = object
                .get(&field.name)
                .with_context(|| {
                    format!(
                        "Missing required field '{}'",
                        field.name
                    )
                })?;

            let avro_value =
                json_to_avro(
                    value,
                    &field.schema,
                )
                .with_context(|| {
                    format!(
                        "Failed to convert field '{}'",
                        field.name
                    )
                })?;

            fields.push((
                field.name.clone(),
                avro_value,
            ));
        }

        Ok(Value::Record(fields))
    }

    // ------------------------------------------------------------
    // Enum
    // ------------------------------------------------------------

    Schema::Enum(enum_schema) => {
        let symbol = json
            .as_str()
            .context(
                "Expected string for Avro enum",
            )?;

        let index = enum_schema
            .symbols
            .iter()
            .position(|item| item == symbol)
            .with_context(|| {
                format!(
                    "Unknown Avro enum symbol '{}'",
                    symbol
                )
            })?;

        Ok(Value::Enum(
            index as u32,
            symbol.to_string(),
        ))
    }

    // ------------------------------------------------------------
    // Fixed
    // ------------------------------------------------------------

    Schema::Fixed(fixed_schema) => {
        let bytes = match json {
            JsonValue::String(value) => {
                value.as_bytes().to_vec()
            }

            JsonValue::Array(values) => {
                let mut bytes =
                    Vec::with_capacity(values.len());

                for value in values {
                    let number = value
                        .as_u64()
                        .context(
                            "Expected byte-compatible integer",
                        )?;

                    let byte =
                        u8::try_from(number)
                            .context(
                                "JSON fixed value is outside u8 range",
                            )?;

                    bytes.push(byte);
                }

                bytes
            }

            _ => anyhow::bail!(
                "Expected string or array for Avro fixed"
            ),
        };

        if bytes.len() != fixed_schema.size {
            anyhow::bail!(
                "Avro fixed '{}' expects {} bytes, got {}",
                fixed_schema.name,
                fixed_schema.size,
                bytes.len()
            );
        }

        Ok(Value::Fixed(
            fixed_schema.size,
            bytes,
        ))
    }

    // ------------------------------------------------------------
    // Union
    // ------------------------------------------------------------

    Schema::Union(union_schema) => {
        for (index, branch) in
            union_schema.variants().iter().enumerate()
        {
            if let Ok(value) =
                json_to_avro(json, branch)
            {
                return Ok(Value::Union(
                    index as u32,
                    Box::new(value),
                ));
            }
        }

        anyhow::bail!(
            "JSON value {} does not match any Avro union branch",
            json
        )
    }

    // ------------------------------------------------------------
    // References / unsupported schemas
    // ------------------------------------------------------------

    _ => {
        anyhow::bail!(
            "Unsupported Avro schema for JSON conversion: {:?}",
            schema
        )
    }
}


}
