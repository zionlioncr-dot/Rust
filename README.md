# Financial Intelligence Platform

A production-oriented financial intelligence platform built as a Rust-based microservice architecture with event-driven processing, transactional outbox, Apache Avro serialization, Confluent-compatible Schema Registry, Redpanda/Kafka, OpenTelemetry, Jaeger, Prometheus, MCP, and an AI Agent powered by Ollama.

The platform is designed around reliable event processing, observability, schema governance, and AI-assisted access to operational data.

---

## 1. Architecture

```text
                                  ┌──────────────────────┐
                                  │      Client / UI      │
                                  └──────────┬───────────┘
                                             │
                                             │ HTTP
                                             ▼
                                  ┌──────────────────────┐
                                  │       AI Agent       │
                                  │      Rust / Axum      │
                                  │       :8001            │
                                  └──────────┬───────────┘
                                             │
                                             │ MCP
                                             ▼
                                  ┌──────────────────────┐
                                  │      MCP Server      │
                                  │   Streamable HTTP     │
                                  │       :8000/mcp       │
                                  └──────────┬───────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
             ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
             │  PostgreSQL  │        │   Redpanda   │        │    Schema    │
             │     :5432    │        │    :19092    │        │   Registry   │
             │              │        │              │        │    :18081    │
             └──────────────┘        └──────────────┘        └──────────────┘
                    │                        │
                    │                        │
                    ▼                        ▼
             ┌──────────────┐        ┌──────────────┐
             │ Audit Service│        │Audit Consumer│
             │     :3000    │        │              │
             └──────────────┘        └──────────────┘

                                  ┌──────────────────────┐
                                  │        Ollama        │
                                  │      qwen3:0.6b      │
                                  │       :11434         │
                                  └──────────────────────┘


                    Observability
                    ─────────────

             ┌──────────────┐       ┌──────────────┐
             │  Prometheus  │       │    Jaeger    │
             │     :9090    │       │     :16686   │
             └──────────────┘       └──────────────┘
                    ▲                       ▲
                    │                       │
                    └──── Metrics ──────────┴──── OpenTelemetry
```

---

# 2. Main Components

## Audit Service

The first core microservice of the platform.

Responsibilities:

* Receive audit events.
* Persist audit records in PostgreSQL.
* Create transactional outbox records.
* Expose health and metrics endpoints.
* Emit OpenTelemetry traces.
* Participate in the event-driven architecture.

Endpoints:

```text
GET  /health
POST /audit
GET  /metrics
```

Default port:

```text
3000
```

---

## Transactional Outbox

The platform uses the transactional outbox pattern to guarantee consistency between database state and event publication.

The workflow is:

```text
HTTP Request
     │
     ▼
Audit Service
     │
     ├───────────────┐
     ▼               ▼
PostgreSQL       outbox_events
                     │
                     ▼
               Outbox Worker
                     │
                     ▼
                Schema Registry
                     │
                     ▼
                  Avro
                     │
                     ▼
                 Redpanda
```

The outbox worker retrieves unpublished events and publishes them to Kafka/Redpanda.

Published events are marked:

```text
published = true
```

This prevents already-published events from being processed again.

---

# 3. Redpanda / Kafka

Redpanda provides the Kafka-compatible event streaming infrastructure.

Current topic:

```text
audit-events
```

Default configuration:

```text
Kafka Brokers:
localhost:19092
```

Internal Docker address:

```text
redpanda:9092
```

The platform currently uses the topic:

```text
audit-events
```

The architecture is compatible with Kafka-based event-driven processing.

---

# 4. Schema Registry

The platform uses a Confluent-compatible Schema Registry for Avro schema governance.

External endpoint:

```text
http://localhost:18081
```

Internal Docker endpoint:

```text
http://redpanda:8081
```

Current subject:

```text
AuditCreated-value
```

Compatibility:

```text
BACKWARD
```

Current schema version:

```text
1
```

Current schema ID:

```text
1
```

The `AuditCreated` Avro record contains:

```text
AuditCreated
├── event_type
├── metadata
│   ├── correlation_id
│   ├── event_id
│   ├── source
│   ├── timestamp
│   └── trace_id
├── payload
│   ├── action
│   ├── created_at
│   ├── id
│   └── user
└── version
    ├── major
    ├── minor
    └── patch
```

Schema evolution is controlled through Schema Registry compatibility rules.

---

# 5. Avro Event Pipeline

Events are serialized using Apache Avro before publication.

The complete pipeline is:

```text
Audit Event
    │
    ▼
PostgreSQL
    │
    ▼
Transactional Outbox
    │
    ▼
Outbox Worker
    │
    ▼
Schema Registry
    │
    ▼
Avro Serialization
    │
    ▼
Redpanda
    │
    ▼
Audit Consumer
```

The outbox worker has been validated to:

* Fetch unpublished events.
* Resolve the schema from Schema Registry.
* Serialize the event using Avro.
* Publish the event to `audit-events`.
* Mark the outbox record as published.

Example successful flow:

```text
Schema Registry schema resolved
subject=AuditCreated-value
schema_version=1
schema_id=1

Event published successfully
event_type=AuditCreated
```

---

# 6. Audit Consumer

The audit consumer subscribes to the Kafka/Redpanda event stream.

Responsibilities:

* Consume `audit-events`.
* Deserialize Avro events.
* Validate event structure.
* Process `AuditCreated` events.
* Provide a foundation for downstream event-driven processing.

---

# 7. MCP Server

The MCP server provides an AI-accessible interface to platform operations.

Technology:

```text
Rust
rmcp 3.2.0
Streamable HTTP
Axum
```

Endpoint:

```text
http://localhost:8000/mcp
```

Health:

```text
http://localhost:8000/health
```

The MCP server exposes the following tools:

```text
system.health
audit.list_events
audit.get_event
audit.get_pending_outbox
kafka.list_topics
schema.get_latest
```

## MCP Tools

### `system.health`

Returns platform health information.

---

### `audit.list_events`

Lists audit events.

Example conceptual request:

```json
{
  "limit": 5
}
```

---

### `audit.get_event`

Retrieves a specific audit event by UUID.

Example:

```json
{
  "id": "f71e9a95-1b68-42d3-874b-f16770328209"
}
```

---

### `audit.get_pending_outbox`

Returns unpublished transactional outbox events.

Example:

```json
{
  "limit": 10
}
```

---

### `kafka.list_topics`

Returns the Kafka/Redpanda topics currently exposed by the MCP layer.

Current topic:

```text
audit-events
```

---

### `schema.get_latest`

Retrieves the latest schema for a Schema Registry subject.

Example:

```json
{
  "subject": "AuditCreated-value"
}
```

---

# 8. AI Agent

The AI Agent is a Rust service that combines:

* Ollama
* Qwen3 0.6B
* MCP
* deterministic tool routing
* Axum HTTP API

The agent provides natural-language access to real platform information.

Current model:

```text
qwen3:0.6b
```

Ollama endpoint:

```text
http://127.0.0.1:11434/api/chat
```

MCP endpoint:

```text
http://127.0.0.1:8000/mcp
```

AI Agent HTTP port:

```text
8001
```

---

# 9. AI Agent HTTP API

## Health

```http
GET /health
```

Example:

```bash
curl -i http://localhost:8001/health
```

Response:

```json
{
  "status": "ok",
  "service": "ai-agent"
}
```

This endpoint represents the liveness of the AI Agent process.

It does not by itself assert that all platform dependencies are healthy.

---

## Chat

```http
POST /chat
```

Request:

```json
{
  "message": "How many pending events are in the outbox?"
}
```

Example:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"How many pending events are in the outbox?"}'
```

Example response:

```json
{
  "answer": "There are no pending events in the outbox.",
  "tool": "audit.get_pending_outbox",
  "execution_time_ms": 4054
}
```

The response includes:

* `answer` — natural-language response.
* `tool` — MCP tool used.
* `execution_time_ms` — total request execution time.

---

# 10. Deterministic AI Tool Policy

The AI Agent does not rely exclusively on the language model to decide whether operational data should be queried.

For known operational questions, a deterministic policy selects the MCP tool first.

Examples:

| User Question                             | MCP Tool                   |
| ----------------------------------------- | -------------------------- |
| Is the platform healthy?                  | `system.health`            |
| How many pending outbox events are there? | `audit.get_pending_outbox` |
| Show the latest audit events              | `audit.list_events`        |
| Get event by UUID                         | `audit.get_event`          |
| What is the latest Avro schema?           | `schema.get_latest`        |
| What are the Kafka topics?                | `kafka.list_topics`        |

This architecture prevents the LLM from inventing operational state.

The flow is:

```text
User Question
      │
      ▼
Deterministic Policy
      │
      ├── Known operational question
      │          │
      │          ▼
      │       MCP Tool
      │          │
      │          ▼
      │       Real Data
      │          │
      │          ▼
      │        Ollama
      │          │
      │          ▼
      │      Final Answer
      │
      └── Other question
                 │
                 ▼
             Ollama
                 │
                 ▼
           Tool Selection
                 │
                 ▼
                MCP
```

---

# 11. Ollama Optimization

The current local model is:

```text
qwen3:0.6b
```

The platform is designed to run on CPU-only development environments.

To reduce inference overhead, after MCP data is obtained the agent does not send the complete MCP tool definitions to Ollama again.

Initial request:

```text
messages + tool definitions
```

Follow-up request:

```text
messages + MCP result
```

without the tool definitions.

This significantly reduces the prompt size for the second inference request.

---

# 12. Observability

The platform includes:

* OpenTelemetry
* Jaeger
* Prometheus
* structured Rust tracing

## Jaeger

UI:

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

Jaeger is used for distributed tracing.

---

## Prometheus

UI:

```text
http://localhost:9090
```

Audit service metrics:

```text
http://localhost:3000/metrics
```

Prometheus scrape configuration targets:

```text
host.docker.internal:3000
```

Metrics endpoint:

```text
/metrics
```

---

# 13. Infrastructure

Current Docker services include:

```text
postgres
redpanda
jaeger
redpanda-console
prometheus
```

Main ports:

| Component        |  Port |
| ---------------- | ----: |
| PostgreSQL       |  5432 |
| AI Agent         |  8001 |
| MCP Server       |  8000 |
| Audit Service    |  3000 |
| Redpanda Kafka   | 19092 |
| Redpanda Admin   |  9644 |
| Schema Registry  | 18081 |
| Redpanda Console |  8080 |
| Prometheus       |  9090 |
| Jaeger UI        | 16686 |
| OTLP gRPC        |  4317 |
| OTLP HTTP        |  4318 |
| Ollama           | 11434 |

---

# 14. Environment Configuration

The current `.env` configuration includes:

```env
DATABASE_URL=postgres://postgres:postgres@localhost:5432/financial

POLLING_INTERVAL=2

KAFKA_TOPIC=audit-events
KAFKA_BROKERS=localhost:19092

SCHEMA_REGISTRY_URL=http://localhost:18081

KAFKA_DLQ_TOPIC=audit-events-dlq

SERVICE_NAME=audit-service

RUST_LOG=info

METRICS_PORT=3000

RETRY_MAX_ATTEMPTS=5
RETRY_INITIAL_DELAY_MS=100
RETRY_MULTIPLIER=2
RETRY_MAX_DELAY_MS=10000

CONSUMER_WORKERS=4
CHANNEL_SIZE=100
```

---

# 15. Development

From the project root:

```bash
cd /mnt/c/Rust/financial-intelligence-platform
```

Build the complete workspace:

```bash
cargo build -j 2
```

Build the AI Agent:

```bash
cargo build -p ai-agent -j 2
```

Run the AI Agent:

```bash
cargo run -p ai-agent -j 2
```

Run the MCP Server:

```bash
cargo run -p mcp-server -j 2
```

Run tests:

```bash
cargo test -j 2
```

---

# 16. Starting Infrastructure

Start Docker infrastructure:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

Check Redpanda:

```bash
docker logs redpanda
```

List topics:

```bash
docker exec -it redpanda rpk topic list
```

Expected topic:

```text
audit-events
```

---

# 17. PostgreSQL Validation

Connect to PostgreSQL:

```bash
docker exec -it postgres psql \
  -U postgres \
  -d financial
```

Inspect outbox events:

```sql
SELECT
    id,
    event_type,
    published,
    created_at
FROM outbox_events
ORDER BY created_at DESC
LIMIT 10;
```

Expected successful events should have:

```text
published = true
```

---

# 18. Schema Registry Validation

Check registered subjects:

```bash
curl http://localhost:18081/subjects
```

Expected subject:

```text
AuditCreated-value
```

Retrieve the latest schema:

```bash
curl http://localhost:18081/subjects/AuditCreated-value/versions/latest
```

Check compatibility:

```bash
curl http://localhost:18081/config/AuditCreated-value
```

The configured compatibility level is:

```text
BACKWARD
```

---

# 19. Redpanda Validation

List topics:

```bash
docker exec -it redpanda rpk topic list
```

Inspect the audit topic:

```bash
docker exec -it redpanda rpk topic describe audit-events
```

Consume events:

```bash
docker exec -it redpanda rpk topic consume audit-events
```

Published events should contain the `AuditCreated` event structure.

---

# 20. MCP Validation

Check MCP health:

```bash
curl -i http://localhost:8000/health
```

Expected:

```text
HTTP/1.1 200 OK
```

The MCP endpoint is:

```text
http://localhost:8000/mcp
```

MCP Inspector can be used to inspect and invoke the available tools.

Current tools:

```text
system.health
audit.list_events
audit.get_event
audit.get_pending_outbox
kafka.list_topics
schema.get_latest
```

---

# 21. AI Agent Validation

Check AI Agent health:

```bash
curl -i http://localhost:8001/health
```

Test platform health:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"Is the platform healthy?"}'
```

Test audit events:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"Show me the latest 5 audit events"}'
```

Test outbox:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"How many pending events are in the outbox?"}'
```

Test Schema Registry:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"What is the latest Avro schema for AuditCreated-value?"}'
```

Test Kafka:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"What are the Kafka topics?"}'
```

---

# 22. Current Validation Status

The following components have been validated:

```text
Rust workspace                    ✅
Audit Service                     ✅
PostgreSQL                        ✅
Transactional Outbox              ✅
Outbox Worker                     ✅
Redpanda                          ✅
audit-events topic                ✅
Schema Registry                   ✅
AuditCreated-value schema         ✅
Avro serialization                ✅
Avro publication                  ✅
Audit Consumer                    ✅
Prometheus                        ✅
Jaeger / OpenTelemetry            ✅*
MCP Server                        ✅
MCP Streamable HTTP               ✅
MCP Inspector                     ✅
AI Agent                          ✅
Ollama qwen3:0.6b                 ✅
Deterministic MCP routing         ✅
AI Agent HTTP /health             ✅
AI Agent HTTP /chat               ✅
```

`*` Jaeger has previously required troubleshooting around service discovery and OTLP export; the infrastructure itself is configured for OTLP on ports `4317` and `4318`.

---

# 23. Verified AI Agent Operations

The AI Agent has been successfully tested with real platform data.

### Platform health

```text
Question:
¿Está saludable toda la plataforma?

Tool:
system.health
```

### Audit events

```text
Question:
¿Cuáles son los últimos 5 eventos de auditoría?

Tool:
audit.list_events
```

The response contained real PostgreSQL audit events.

### Transactional outbox

```text
Question:
¿Cuántos eventos pendientes hay en el outbox?

Tool:
audit.get_pending_outbox
```

Result:

```text
No hay eventos pendientes en el outbox.
```

### Schema Registry

```text
Question:
¿Cuál es el último schema Avro de AuditCreated-value?

Tool:
schema.get_latest
```

The latest registered schema was successfully retrieved.

### Kafka

```text
Question:
¿Cuáles son los topics de Kafka?

Tool:
kafka.list_topics
```

Current topic:

```text
audit-events
```

---

# 24. Reliability Principles

The platform follows several reliability principles.

## Database/Event Consistency

The transactional outbox prevents losing events between database transactions and message publication.

## Schema Governance

Schema Registry provides centralized schema management and compatibility enforcement.

## Source of Truth

The AI Agent must use MCP results for operational information.

The LLM must not invent:

* audit events
* Kafka topics
* platform health
* Schema Registry data
* PostgreSQL state
* outbox state

## Observability

Distributed tracing and metrics are first-class components.

## Deterministic Tool Selection

Known operational questions are routed directly to the appropriate MCP tool before LLM generation.

---

# 25. Project Structure

The project follows a Rust workspace structure similar to:

```text
financial-intelligence-platform/
│
├── apps/
│   ├── ai-agent/
│   ├── api-gateway/
│   ├── audit-consumer/
│   ├── audit-service/
│   ├── mcp-server/
│   └── outbox-worker/
│
├── libs/
│   ├── common/
│   ├── domain/
│   ├── event-bus/
│   ├── kafka/
│   ├── repository/
│   └── ...
│
├── migrations/
│
├── docker-compose.yml
├── Cargo.toml
├── Cargo.lock
├── prometheus.yml
└── .env
```

---

# 26. Technology Stack

## Backend

* Rust
* Tokio
* Axum
* SQLx
* PostgreSQL

## Event Streaming

* Redpanda
* Kafka-compatible APIs
* Apache Avro
* Schema Registry

## AI

* Ollama
* Qwen3 0.6B
* MCP
* rmcp 3.2.0

## Observability

* OpenTelemetry
* Jaeger
* Prometheus
* tracing
* tracing-subscriber

## Infrastructure

* Docker
* Docker Compose
* Kubernetes-ready architecture

---

# 27. Roadmap

The next planned steps are:

1. Dockerize the AI Agent.
2. Add the AI Agent to `docker-compose.yml`.
3. Configure container-to-container networking.
4. Connect AI Agent to the MCP Server using the Docker service name.
5. Connect AI Agent to Ollama through `host.docker.internal`.
6. Add production-oriented configuration through environment variables.
7. Add AI Agent metrics.
8. Add distributed tracing to AI Agent requests.
9. Improve Kafka topic discovery through Kafka Admin APIs.
10. Improve AI Agent response latency for large Schema Registry responses.
11. Add authentication and authorization to the MCP and AI Agent APIs.
12. Integrate the AI Agent with the future platform UI.
13. Prepare Kubernetes deployment manifests.
14. Add CI/CD automation.

---

# 28. Quick Start

Start infrastructure:

```bash
docker compose up -d
```

Start the MCP Server:

```bash
cargo run -p mcp-server -j 2
```

Start the AI Agent:

```bash
cargo run -p ai-agent -j 2
```

Verify:

```bash
curl http://localhost:8000/health
```

```bash
curl http://localhost:8001/health
```

Ask the AI Agent:

```bash
curl -s -X POST http://localhost:8001/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"¿Cuántos eventos pendientes hay en el outbox?"}'
```

---

# 29. Project Status

The platform has progressed from an initial Rust microservice implementation into a functional event-driven platform with:

```text
Database
    +
Transactional Outbox
    +
Avro
    +
Schema Registry
    +
Redpanda
    +
Event Consumer
    +
OpenTelemetry
    +
Jaeger
    +
Prometheus
    +
MCP
    +
AI Agent
    +
HTTP API
```

The current architecture establishes a foundation for building a production-grade financial intelligence system where operational data can be queried through both conventional APIs and an AI interface while maintaining deterministic access to real system state.
