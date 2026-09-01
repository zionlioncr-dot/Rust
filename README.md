# Financial Intelligence Platform

Event-driven financial intelligence platform built as a Rust workspace using microservices, PostgreSQL, transactional outbox, Apache Kafka/Redpanda, Confluent Schema Registry, Apache Avro, OpenTelemetry, Jaeger, and Prometheus.

The project is designed around asynchronous event-driven architecture, reliable event publication, schema governance, observability, and independently deployable services.

---

## Architecture

```text
                         ┌──────────────────────┐
                         │      API Gateway     │
                         │        :3000         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    audit-service     │
                         │                      │
                         │ REST /audit          │
                         │ OpenTelemetry        │
                         │ Prometheus metrics   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      PostgreSQL      │
                         │        :5432         │
                         │                      │
                         │ audit records        │
                         │ outbox_events        │
                         └──────────┬───────────┘
                                    │
                         Transactional Outbox
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    outbox-worker     │
                         │                      │
                         │ Poll unpublished     │
                         │ events               │
                         └──────────┬───────────┘
                                    │
                                    │ Schema lookup
                                    ▼
                         ┌──────────────────────┐
                         │  Schema Registry     │
                         │       :18081         │
                         │                      │
                         │ AuditCreated-value   │
                         │ Avro schema          │
                         └──────────┬───────────┘
                                    │
                                    │ Avro datum
                                    ▼
                         ┌──────────────────────┐
                         │      Redpanda        │
                         │       :19092         │
                         │                      │
                         │ topic: audit-events  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    audit-consumer    │
                         │                      │
                         │ Avro decoding        │
                         │ validation           │
                         │ version routing      │
                         │ event dispatch       │
                         └──────────────────────┘


             ┌─────────────────────────────────────────┐
             │              Observability             │
             │                                         │
             │  OpenTelemetry → Jaeger                │
             │  Prometheus ← /metrics                 │
             └─────────────────────────────────────────┘
```

---

## Current Status

The core event-driven pipeline is operational.

### Working

* Rust workspace compilation
* `audit-service`
* PostgreSQL persistence
* Transactional Outbox Pattern
* `outbox-worker`
* Redpanda
* `audit-events` topic
* Confluent Schema Registry
* `AuditCreated-value` Avro schema
* JSON → Apache Avro conversion
* Confluent Avro wire format
* Avro producer
* Avro consumer
* `audit-consumer`
* Event validation
* Event version routing
* Event dispatching
* Prometheus metrics
* Jaeger/OpenTelemetry infrastructure

### End-to-end event flow validated

```text
POST /audit
     │
     ▼
audit-service
     │
     ▼
PostgreSQL
     │
     ▼
outbox_events
     │
     ▼
outbox-worker
     │
     ├── Schema Registry
     │
     ▼
Avro serialization
     │
     ▼
Redpanda / audit-events
     │
     ▼
audit-consumer
     │
     ▼
EventEnvelope
     │
     ▼
EventSchemaValidator
     │
     ▼
EventVersionRouter
     │
     ▼
EventDispatcher
     │
     ▼
AuditProcessingService
```

The consumer has successfully processed:

```text
event_type = AuditCreated
user       = alejandro
action     = LOGIN
```

---

# Technology Stack

## Backend

* Rust
* Tokio
* Axum
* SQLx
* PostgreSQL
* Apache Kafka protocol
* Redpanda
* Apache Avro
* Confluent Schema Registry
* Reqwest
* Serde / Serde JSON
* Tracing
* OpenTelemetry

## Observability

* OpenTelemetry
* Jaeger
* Prometheus
* `tracing`
* `tracing-subscriber`

## Infrastructure

* Docker
* Docker Compose
* Redpanda
* PostgreSQL
* Schema Registry
* Prometheus
* Jaeger

---

# Workspace Structure

```text
financial-intelligence-platform/
│
├── apps/
│   │
│   ├── api-gateway/
│   │
│   ├── audit-service/
│   │
│   ├── audit-consumer/
│   │
│   └── outbox-worker/
│
├── libs/
│   │
│   ├── common/
│   │
│   ├── domain/
│   │
│   ├── event-bus/
│   │
│   ├── health/
│   │
│   ├── http-server/
│   │
│   ├── kafka/
│   │
│   ├── metrics/
│   │
│   ├── repository/
│   │
│   └── telemetry/
│
├── migrations/
│
├── prometheus/
│   └── prometheus.yml
│
├── docker-compose.yml
│
├── Cargo.toml
│
└── README.md
```

---

# Core Services

## audit-service

Responsible for receiving audit events and persisting them.

Endpoint:

```text
POST /audit
```

Example:

```bash
curl -i -X POST http://localhost:3000/audit \
  -H 'Content-Type: application/json' \
  -d '{"user":"alejandro","action":"LOGIN"}'
```

Example response:

```json
{
  "id": "f71e9a95-1b68-42d3-874b-f16770328209",
  "timestamp": "2026-09-01T01:53:33.766705572Z"
}
```

The service writes the business record and its corresponding outbox event transactionally.

---

# PostgreSQL

Default development configuration:

```text
Host: localhost
Port: 5432
Database: financial
User: postgres
Password: postgres
```

The main event table is:

```text
outbox_events
```

Important fields include:

```text
id
event_type
payload
published
created_at
```

Check pending events:

```bash
docker exec -it postgres \
  psql -U postgres -d financial \
  -c "SELECT COUNT(*) AS pending_events FROM outbox_events WHERE published = false;"
```

Expected result after successful publication:

```text
pending_events
---------------
0
```

---

# Transactional Outbox Pattern

The platform uses the Transactional Outbox Pattern to prevent the classic dual-write problem.

Instead of:

```text
Database
   +
Kafka
```

being independently committed, the event is first persisted in PostgreSQL.

```text
Business operation
       │
       ├── business data
       │
       └── outbox event
              │
              ▼
          PostgreSQL
              │
              ▼
        outbox-worker
              │
              ▼
           Redpanda
```

The worker publishes only events where:

```text
published = false
```

After successful publication, the event is marked as published.

---

# Redpanda

Redpanda is used as the Kafka-compatible event streaming platform.

Development broker:

```text
localhost:19092
```

Topic:

```text
audit-events
```

Check the topic:

```bash
docker exec redpanda rpk topic describe audit-events
```

Expected configuration:

```text
NAME        audit-events
PARTITIONS  1
REPLICAS    1
```

Consume messages:

```bash
docker exec redpanda \
  rpk topic consume audit-events --num 10
```

---

# Schema Registry

Schema Registry:

```text
http://localhost:18081
```

Current subject:

```text
AuditCreated-value
```

Retrieve the latest schema:

```bash
curl -s \
  http://localhost:18081/subjects/AuditCreated-value/versions/latest \
  | python3 -m json.tool
```

Current schema ID:

```text
1
```

Current schema version:

```text
1
```

---

# Avro Schema

The current `AuditCreated` schema is:

```json
{
  "type": "record",
  "name": "AuditCreated",
  "namespace": "financial.audit.events",
  "fields": [
    {
      "name": "event_type",
      "type": "string"
    },
    {
      "name": "metadata",
      "type": {
        "type": "record",
        "name": "EventMetadata",
        "fields": [
          {
            "name": "correlation_id",
            "type": "string"
          },
          {
            "name": "event_id",
            "type": "string"
          },
          {
            "name": "source",
            "type": "string"
          },
          {
            "name": "timestamp",
            "type": "string"
          },
          {
            "name": "trace_id",
            "type": "string"
          }
        ]
      }
    },
    {
      "name": "payload",
      "type": {
        "type": "record",
        "name": "AuditPayload",
        "fields": [
          {
            "name": "action",
            "type": "string"
          },
          {
            "name": "created_at",
            "type": "string"
          },
          {
            "name": "id",
            "type": "string"
          },
          {
            "name": "user",
            "type": "string"
          }
        ]
      }
    },
    {
      "name": "version",
      "type": {
        "type": "record",
        "name": "EventVersion",
        "fields": [
          {
            "name": "major",
            "type": "int"
          },
          {
            "name": "minor",
            "type": "int"
          },
          {
            "name": "patch",
            "type": "int"
          }
        ]
      }
    }
  ]
}
```

---

# Confluent Avro Wire Format

The producer uses the Confluent wire format:

```text
+------------+-------------------+------------------+
| Magic Byte | Schema ID         | Avro Datum       |
|    0       | 4 bytes big endian | binary payload   |
+------------+-------------------+------------------+
```

For schema ID `1`:

```text
00 00 00 00 01
```

The implementation uses:

```rust
to_avro_datum(&schema, avro_value)
```

rather than `apache_avro::Writer`.

This distinction is important.

`Writer` produces an Avro Object Container File (OCF), beginning with:

```text
Obj
avro.schema
avro.codec
```

That is **not** the payload format expected after the Confluent Schema Registry wire-format header.

The current implementation therefore produces:

```text
magic byte
    +
schema ID
    +
Avro binary datum
```

---

# outbox-worker

Start the worker:

```bash
cargo run -p outbox-worker
```

Expected startup:

```text
AppConfig kafka_brokers = localhost:19092
KafkaProducer brokers = localhost:19092
Schema Registry = http://localhost:18081
```

Successful publication:

```text
Event published successfully with Avro schema
```

The worker also resolves the schema from Schema Registry:

```text
Schema Registry schema resolved
subject=AuditCreated-value
schema_version=1
schema_id=1
```

---

# audit-consumer

Start the consumer:

```bash
cargo run -p audit-consumer
```

Expected configuration:

```text
KafkaConsumer brokers = localhost:19092
Group = audit-group
Schema Registry = http://localhost:18081
```

The consumer:

1. Connects to Redpanda.
2. Reads `audit-events`.
3. Reads the Confluent schema ID from the message.
4. Resolves the schema through Schema Registry.
5. Decodes the Avro datum.
6. Builds `EventEnvelope`.
7. Validates the event.
8. Routes the event according to its version.
9. Dispatches the event.
10. Processes the audit event.

Successful processing looks like:

```text
Audit Event Processed
event_id=...
user=alejandro
action=LOGIN

Event processed successfully
event_type=AuditCreated
event_id=...
```

---

# Event Envelope

Events use the following logical structure:

```json
{
  "event_type": "AuditCreated",
  "metadata": {
    "correlation_id": "...",
    "event_id": "...",
    "source": "audit-service",
    "timestamp": "...",
    "trace_id": "..."
  },
  "payload": {
    "action": "LOGIN",
    "created_at": "...",
    "id": "...",
    "user": "alejandro"
  },
  "version": {
    "major": 1,
    "minor": 0,
    "patch": 0
  }
}
```

---

# Prometheus

Prometheus is configured to scrape:

```text
audit-service
```

Metrics endpoint:

```text
http://localhost:3000/metrics
```

Check metrics:

```bash
curl -s http://localhost:3000/metrics
```

Prometheus:

```text
http://localhost:9090
```

Check targets:

```bash
curl -s \
  http://localhost:9090/api/v1/targets
```

The `audit-service` target should report:

```text
health = up
```

Current metrics include HTTP request duration histograms and consumer/outbox metrics.

---

# Jaeger / OpenTelemetry

Jaeger is used for distributed tracing.

Jaeger UI:

```text
http://localhost:16686
```

OTLP gRPC:

```text
localhost:4317
```

OTLP HTTP:

```text
localhost:4318
```

The architecture is intended to propagate tracing context through:

```text
audit-service
      │
      ▼
PostgreSQL Outbox
      │
      ▼
outbox-worker
      │
      ▼
Redpanda
      │
      ▼
audit-consumer
```

The event metadata includes:

```text
trace_id
correlation_id
event_id
```

This allows business-event correlation with distributed traces.

---

# Docker Infrastructure

The development infrastructure is managed through Docker Compose.

Start infrastructure:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

Expected infrastructure includes:

```text
postgres
redpanda
schema-registry
prometheus
jaeger
```

Stop infrastructure:

```bash
docker compose down
```

Stop and remove volumes:

```bash
docker compose down -v
```

> Use `down -v` carefully because it removes persistent development data.

---

# Build

Build the complete workspace:

```bash
cargo build --workspace
```

Run tests:

```bash
cargo test --workspace
```

Check the workspace:

```bash
cargo check --workspace
```

---

# Running the Platform

## 1. Start infrastructure

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

---

## 2. Verify PostgreSQL

```bash
docker exec -it postgres \
  psql -U postgres -d financial
```

Then:

```sql
SELECT COUNT(*)
FROM outbox_events
WHERE published = false;
```

---

## 3. Start audit-service

```bash
cargo run -p audit-service
```

---

## 4. Create an audit event

From another terminal:

```bash
curl -i -X POST http://localhost:3000/audit \
  -H 'Content-Type: application/json' \
  -d '{"user":"alejandro","action":"LOGIN"}'
```

---

## 5. Start outbox-worker

```bash
cargo run -p outbox-worker
```

Expected:

```text
Event published successfully with Avro schema
```

---

## 6. Start audit-consumer

```bash
cargo run -p audit-consumer
```

Expected:

```text
Audit Event Processed
```

followed by:

```text
Event processed successfully
```

---

# Validation Checklist

## PostgreSQL

```bash
docker exec -it postgres \
  psql -U postgres -d financial \
  -c "SELECT id,event_type,published FROM outbox_events ORDER BY created_at DESC LIMIT 10;"
```

Successful events should have:

```text
published = true
```

---

## Redpanda

```bash
docker exec redpanda \
  rpk topic describe audit-events
```

Then:

```bash
docker exec redpanda \
  rpk topic consume audit-events --num 10
```

---

## Schema Registry

```bash
curl -s \
  http://localhost:18081/subjects/AuditCreated-value/versions/latest \
  | python3 -m json.tool
```

---

## Prometheus

```bash
curl -s \
  http://localhost:9090/api/v1/targets
```

---

## Jaeger

Open:

```text
http://localhost:16686
```

Search for the relevant service and trace.

---

# Important Avro Implementation Detail

The producer must not use:

```rust
Writer::new(&schema, &mut avro_payload)
```

for Confluent messages.

That creates an Avro Object Container File.

The correct implementation is:

```rust
let avro_payload = to_avro_datum(
    &schema,
    avro_value,
)?;
```

followed by:

```rust
wire_payload.push(0u8);
wire_payload.extend_from_slice(
    &schema_id.to_be_bytes()
);
wire_payload.extend_from_slice(
    &avro_payload
);
```

This produces the expected Confluent wire format.

---

# Event Processing Guarantees

The current architecture provides:

### Reliable persistence

Events are persisted in PostgreSQL before publication.

### Transactional Outbox

Business state and event creation occur within the database transaction.

### Schema governance

Event schemas are stored and versioned through Schema Registry.

### Binary serialization

Events are serialized using Apache Avro.

### Schema identification

Kafka messages contain the Schema Registry ID.

### Consumer validation

Events are decoded and validated before dispatch.

### Version routing

Event versions are routed through the event-version router.

### Observability

Metrics and distributed tracing are integrated into the architecture.

---

# Development Roadmap

## Completed

* [x] Rust workspace
* [x] PostgreSQL integration
* [x] Audit service
* [x] Transactional Outbox Pattern
* [x] Kafka-compatible messaging
* [x] Redpanda
* [x] Schema Registry
* [x] `AuditCreated` Avro schema
* [x] JSON → Avro conversion
* [x] Confluent wire format
* [x] Avro producer
* [x] Avro consumer
* [x] Audit consumer
* [x] Event validation
* [x] Event version routing
* [x] Event dispatching
* [x] Prometheus metrics
* [x] Jaeger
* [x] OpenTelemetry infrastructure

## Next

* [ ] End-to-end distributed trace propagation validation
* [ ] Correlate `trace_id` across producer and consumer
* [ ] Improve consumer/outbox metrics
* [ ] Clean unused Rust modules and warnings
* [ ] Add integration tests for Avro serialization
* [ ] Add integration tests for Schema Registry
* [ ] Add Kafka/Redpanda integration tests
* [ ] Add failure/retry scenarios
* [ ] Add dead-letter strategy
* [ ] Add schema compatibility validation
* [ ] Add additional event types
* [ ] Containerize application services
* [ ] Kubernetes deployment
* [ ] Infrastructure as Code
* [ ] AWS deployment

---

# Design Principles

The platform follows these principles:

```text
Event-driven architecture
        +
Transactional consistency
        +
Schema governance
        +
Strongly typed contracts
        +
Distributed observability
        +
Independent services
```

The objective is to provide a foundation suitable for a production-oriented financial event-processing platform.

---

# Development Notes

For local development, application services connect to:

```text
PostgreSQL      localhost:5432
Redpanda        localhost:19092
Schema Registry localhost:18081
Prometheus      localhost:9090
Jaeger UI       localhost:16686
OTLP gRPC       localhost:4317
OTLP HTTP       localhost:4318
```

The current event topic is:

```text
audit-events
```

The current event type is:

```text
AuditCreated
```

The current Schema Registry subject is:

```text
AuditCreated-value
```

The current schema ID is:

```text
1
```

---

# License

This project is currently under development.
