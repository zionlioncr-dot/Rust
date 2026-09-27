# Financial Intelligence Platform

A production-oriented financial intelligence platform built around **Rust microservices, PostgreSQL, Kafka/Redpanda, Apache Avro, Schema Registry, Kubernetes, OpenTelemetry, Jaeger, Prometheus, MCP and Ollama**.

The platform is designed around asynchronous event processing, transactional consistency, schema evolution, distributed tracing, idempotent consumers and AI-assisted operational intelligence.

---

## Architecture

```text
                         ┌──────────────────────┐
                         │      Client / API     │
                         └──────────┬───────────┘
                                    │
                                    │ JWT / HTTP
                                    ▼
                         ┌──────────────────────┐
                         │     API Gateway      │
                         │      :8088           │
                         │                      │
                         │ JWT / scopes         │
                         │ request-id           │
                         │ trace propagation    │
                         └──────────┬───────────┘
                                    │
                                    │ HTTP
                                    ▼
                         ┌──────────────────────┐
                         │    Audit Service     │
                         │      :3000           │
                         └──────────┬───────────┘
                                    │
                                    │ PostgreSQL
                                    ▼
                         ┌──────────────────────┐
                         │      PostgreSQL      │
                         │                      │
                         │ audit_events         │
                         │ outbox_events        │
                         │ processed_events     │
                         │ dead_letter_events   │
                         └──────────┬───────────┘
                                    │
                                    │ Transactional Outbox
                                    ▼
                         ┌──────────────────────┐
                         │    Outbox Worker     │
                         └──────────┬───────────┘
                                    │
                                    │ Avro
                                    │ traceparent
                                    ▼
                 ┌────────────────────────────────────┐
                 │          Redpanda / Kafka          │
                 │                                    │
                 │          audit-events              │
                 └────────────────┬───────────────────┘
                                  │
                                  │ Kafka Consumer
                                  ▼
                         ┌──────────────────────┐
                         │    Audit Consumer    │
                         │                      │
                         │ validation           │
                         │ version routing      │
                         │ idempotency          │
                         │ retry                 │
                         │ dead-letter           │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ Audit Processing     │
                         └──────────────────────┘


                 ┌────────────────────────────────────┐
                 │          Schema Registry            │
                 │                                    │
                 │          AuditCreated-value        │
                 └────────────────────────────────────┘


                         Observability
                 ┌────────────────────────────────────┐
                 │                                    │
                 │ OpenTelemetry                      │
                 │        │                           │
                 │        ▼                           │
                 │      Jaeger                        │
                 │                                    │
                 │ Prometheus                         │
                 │        │                           │
                 │        ▼                           │
                 │     Metrics                        │
                 └────────────────────────────────────┘


                         AI / MCP Layer
                 ┌────────────────────────────────────┐
                 │          MCP Server                 │
                 │                                    │
                 │ audit.get_event                    │
                 │ audit.list_events                  │
                 │ audit.get_pending_outbox           │
                 │ kafka.list_topics                  │
                 │ schema.get_latest                  │
                 │ system.health                      │
                 └────────────────┬───────────────────┘
                                  │
                                  │ MCP
                                  ▼
                         ┌──────────────────────┐
                         │      AI Agent        │
                         │       Rust           │
                         └──────────┬───────────┘
                                    │
                                    │ HTTP
                                    ▼
                         ┌──────────────────────┐
                         │       Ollama         │
                         │      Local LLM        │
                         │                      │
                         │      qwen3:0.6b      │
                         └──────────────────────┘
```

---

# Project Goals

The platform focuses on the following architectural capabilities:

* Event-driven architecture
* Transactional Outbox Pattern
* Kafka-compatible event streaming
* Apache Avro serialization
* Schema Registry and schema evolution
* Idempotent event processing
* Retry and Dead Letter handling
* Distributed tracing
* W3C Trace Context propagation
* OpenTelemetry
* Prometheus metrics
* Kubernetes deployment
* JWT authentication and authorization scopes
* MCP-based operational access
* Local AI inference through Ollama
* AI-assisted platform observability and analysis

---

# Technology Stack

## Backend

* Rust
* Tokio
* Axum
* SQLx
* PostgreSQL
* rdkafka
* Apache Avro
* Reqwest
* Serde
* JSON Web Tokens

## Event Infrastructure

* Redpanda
* Kafka protocol
* Apache Avro
* Confluent-compatible Schema Registry
* Transactional Outbox Pattern

## Observability

* OpenTelemetry
* Jaeger
* Prometheus
* W3C Trace Context
* `traceparent`

## Container / Infrastructure

* Docker
* Docker Compose
* Kubernetes
* kind

## AI

* Ollama
* Local LLM inference
* MCP
* Rust MCP client/server
* `rmcp`

---

# Workspace Structure

```text
financial-intelligence-platform/
│
├── apps/
│   ├── api-gateway/
│   │
│   ├── audit-service/
│   │
│   ├── audit-consumer/
│   │
│   ├── outbox-worker/
│   │
│   ├── mcp-server/
│   │
│   └── ai-agent/
│
├── libs/
│   ├── common/
│   ├── domain/
│   ├── event-bus/
│   ├── kafka/
│   ├── metrics/
│   ├── repository/
│   └── telemetry/
│
├── deploy/
│   ├── kubernetes/
│   └── ...
│
├── docker-compose.yml
├── Cargo.toml
└── README.md
```

---

# Core Event Flow

An audit request follows this lifecycle:

```text
HTTP Request
    │
    ▼
API Gateway
    │
    │ JWT validation
    │ scope validation
    │ trace creation
    ▼
Audit Service
    │
    │ database transaction
    ├───────────────┐
    │               │
    ▼               ▼
audit_events    outbox_events
                    │
                    │ published = false
                    ▼
              Outbox Worker
                    │
                    │ Avro serialization
                    │ Schema Registry
                    │ traceparent
                    ▼
                 Redpanda
                    │
                    ▼
              Audit Consumer
                    │
                    ├── decode Avro
                    ├── validate schema
                    ├── validate event
                    ├── route version
                    ├── idempotency
                    ├── retry
                    └── dead-letter
                    │
                    ▼
              Audit Processing
```

---

# Transactional Outbox

The Audit Service does not directly depend on Kafka availability to commit an audit operation.

Instead:

```text
BEGIN TRANSACTION

INSERT audit event

INSERT outbox event

COMMIT
```

The Outbox Worker subsequently publishes the event to Redpanda.

This provides a reliable boundary between PostgreSQL state and asynchronous event publication.

Example:

```text
outbox_events

id
event_type
payload
published
created_at
```

The worker retrieves unpublished events and publishes them using the registered Avro schema.

---

# Kafka / Redpanda

Primary event topic:

```text
audit-events
```

Kubernetes configuration:

```text
redpanda:9092
```

The platform uses Kafka-compatible APIs through `rdkafka`.

The producer publishes events using Confluent-compatible Avro wire format:

```text
Byte 0      = magic byte
Bytes 1..5  = schema ID
Bytes 5..   = Avro binary payload
```

The consumer resolves the schema ID through Schema Registry before decoding the event.

---

# Schema Registry

Schema Registry is used to manage event contracts.

Current event subject:

```text
AuditCreated-value
```

Example logical schema:

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

Compatibility is configured through Schema Registry.

The current platform uses backward-compatible schema evolution.

---

# Audit Consumer

The Audit Consumer performs:

1. Kafka subscription
2. Confluent Avro decoding
3. Schema resolution
4. EventEnvelope deserialization
5. Event validation
6. Version routing
7. Idempotency checking
8. Event dispatch
9. Retry handling
10. Dead-letter processing
11. Distributed trace propagation

The consumer uses:

```text
KAFKA_BROKERS=redpanda:9092
KAFKA_TOPIC=audit-events
SCHEMA_REGISTRY_URL=http://redpanda:8081
```

---

# Idempotency

Events are processed through an idempotency layer backed by PostgreSQL.

This prevents duplicate Kafka deliveries from producing duplicate business processing.

Conceptually:

```text
Kafka Event
    │
    ▼
event_id
    │
    ▼
processed_events
    │
    ├── already processed ──► ignore
    │
    └── new event ──────────► process
```

---

# Retry and Dead Letter Handling

The consumer includes:

* Retry policy
* Retry executor
* Dead Letter Service
* Persistent processing state

Failures are therefore not treated as simple application crashes.

The intended flow is:

```text
Event
 │
 ▼
Process
 │
 ├── success ───────────────► ACK / completed
 │
 └── failure
       │
       ▼
     Retry
       │
       ├── success ─────────► completed
       │
       └── retry exhausted
              │
              ▼
          Dead Letter
```

---

# Authentication

The API Gateway validates JWT tokens using:

```text
Algorithm: HS256

Issuer:
financial-intelligence-platform

Audience:
financial-api
```

Authorization is scope-based.

Current audit scopes:

```text
audit:read
audit:write
```

HTTP methods map to scopes:

```text
GET
 └── audit:read

POST
PUT
PATCH
DELETE
 └── audit:write
```

Example claims:

```json
{
  "sub": "user",
  "iss": "financial-intelligence-platform",
  "aud": "financial-api",
  "iat": 0,
  "exp": 0,
  "scopes": [
    "audit:read",
    "audit:write"
  ]
}
```

Authentication infrastructure is currently present at the Gateway level. A dedicated user/login/identity service is still a future component.

---

# Distributed Tracing

The platform implements W3C Trace Context.

Example:

```text
traceparent:
00-9c3c3750c0821c53b1b1fd89336bdcdd-0ca4604a606a3a16-01
```

The propagation path is:

```text
API Gateway
    │
    ▼
Audit Service
    │
    ▼
PostgreSQL Outbox
    │
    ▼
Outbox Worker
    │
    │ Kafka traceparent header
    ▼
Redpanda
    │
    ▼
Audit Consumer
    │
    ▼
OpenTelemetry Context
    │
    ▼
Audit Dispatcher
    │
    ▼
Audit Processing
```

The current implementation has been verified with a real audit request.

Observed trace ID:

```text
9c3c3750c0821c53b1b1fd89336bdcdd
```

The same W3C `traceparent` was observed in:

* Outbox Worker
* Kafka message
* Audit Consumer

The consumer successfully extracted the context and processed the event under the propagated trace.

---

# OpenTelemetry

Services use OpenTelemetry for distributed tracing.

Example Kubernetes configuration:

```text
OTEL_SERVICE_NAME=audit-consumer
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
```

Jaeger receives OTLP traffic through:

```text
4317  OTLP gRPC
4318  OTLP HTTP
16686 Jaeger UI
```

Expected services include:

```text
api-gateway
audit-service
outbox-worker
audit-consumer
```

---

# Prometheus

The platform exposes application metrics for Prometheus scraping.

Example:

```text
/metrics
```

Prometheus is configured to scrape the platform services.

Metrics include consumer processing activity and application-level counters.

---

# MCP

The platform includes an MCP server providing controlled operational access to platform data.

The MCP server exposes tools including:

```text
audit.get_event
audit.list_events
audit.get_pending_outbox
kafka.list_topics
schema.get_latest
system.health
```

The MCP layer intentionally exposes controlled capabilities instead of giving the AI unrestricted access to PostgreSQL, Kafka or the Kubernetes cluster.

This creates a useful separation:

```text
AI
 │
 ▼
MCP
 │
 ├── Audit
 ├── Outbox
 ├── Kafka
 ├── Schema Registry
 └── System Health
```

---

# Ollama AI Layer

The platform includes local LLM inference through Ollama.

Current AI Agent flow:

```text
User
 │
 ▼
AI Agent
 │
 ▼
MCP Server
 │
 ├── audit.get_event
 ├── audit.list_events
 ├── audit.get_pending_outbox
 ├── kafka.list_topics
 ├── schema.get_latest
 └── system.health
 │
 ▼
Ollama
 │
 ▼
Natural Language Response
```

The AI Agent has already been tested with Ollama using:

```text
qwen3:0.6b
```

Example supported questions:

```text
¿Cuántos eventos pendientes hay en el outbox?

¿Cuál es el último schema Avro de AuditCreated-value?

¿Está saludable toda la plataforma?
```

The architecture also supports deterministic tool selection for operational questions, reducing unnecessary LLM reasoning when the requested operation maps directly to an MCP capability.

---

# Kubernetes

The platform runs on a local kind cluster:

```text
financial-cluster
```

Namespace:

```text
financial-platform
```

Example:

```bash
kubectl get pods -n financial-platform
```

Core workloads include:

```text
api-gateway
audit-service
audit-consumer
outbox-worker
postgres
redpanda
jaeger
prometheus
```

Images can be loaded into kind for local development:

```bash
kind load docker-image audit-consumer:local \
  --name financial-cluster
```

---

# Local Development

Build the entire workspace:

```bash
cargo build
```

Build release:

```bash
cargo build --release
```

Format:

```bash
cargo fmt --all
```

Run tests:

```bash
cargo test --workspace
```

---

# Audit Consumer

Build:

```bash
cargo build --release -p audit-consumer
```

Build Docker image:

```bash
docker build \
  --no-cache \
  -f apps/audit-consumer/Dockerfile \
  -t audit-consumer:local .
```

Load into kind:

```bash
kind load docker-image audit-consumer:local \
  --name financial-cluster
```

Restart deployment:

```bash
kubectl rollout restart deployment/audit-consumer \
  -n financial-platform
```

Check rollout:

```bash
kubectl rollout status deployment/audit-consumer \
  -n financial-platform
```

View logs:

```bash
kubectl logs \
  -n financial-platform \
  deployment/audit-consumer \
  --timestamps
```

---

# API Gateway

Default local endpoint:

```text
http://localhost:8088
```

Audit endpoint:

```text
POST /audit
```

Example:

```bash
curl -i -X POST http://localhost:8088/audit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user": "alejandro",
    "action": "TEST_EVENT"
  }'
```

---

# Health Checks

Audit Consumer:

```bash
curl http://localhost:3001/health
```

Expected:

```json
{
  "status": "UP"
}
```

Kubernetes:

```bash
kubectl get pods -n financial-platform
```

---

# PostgreSQL

The platform uses PostgreSQL as the transactional source of truth.

Important tables include:

```text
audit_events
outbox_events
processed_events
dead_letter_events
```

The database is accessed from Kubernetes using:

```text
postgres:5432
```

Example connectivity test:

```bash
kubectl run pg-test \
  -n financial-platform \
  --rm -it \
  --restart=Never \
  --image=postgres:16 \
  --env="PGPASSWORD=postgres" \
  -- psql \
    -h postgres \
    -U postgres \
    -d financial \
    -c "SELECT 1;"
```

---

# Redpanda

Check topics:

```bash
kubectl exec -n financial-platform deploy/redpanda -- \
  rpk topic list
```

Expected topic:

```text
audit-events
```

---

# Schema Registry

The platform uses the Redpanda Schema Registry endpoint inside Kubernetes:

```text
http://redpanda:8081
```

The primary subject is:

```text
AuditCreated-value
```

---

# Observability

The current observability architecture is:

```text
                    ┌──────────────┐
                    │ Applications │
                    └──────┬───────┘
                           │
                ┌──────────┴──────────┐
                │                     │
                ▼                     ▼
        OpenTelemetry            Prometheus
                │                     │
                ▼                     ▼
             Jaeger                Metrics
```

Tracing provides:

* Trace IDs
* Span IDs
* Parent-child relationships
* W3C Trace Context
* Cross-service propagation

Metrics provide:

* Consumer activity
* Application counters
* Prometheus-compatible metrics

---

# Current Validation Status

The following capabilities have been exercised successfully:

| Capability                    | Status |
| ----------------------------- | ------ |
| Rust workspace                | ✅      |
| Audit Service                 | ✅      |
| API Gateway                   | ✅      |
| JWT validation                | ✅      |
| JWT scopes                    | ✅      |
| PostgreSQL                    | ✅      |
| Transactional Outbox          | ✅      |
| Redpanda/Kafka                | ✅      |
| Avro serialization            | ✅      |
| Schema Registry               | ✅      |
| Audit Consumer                | ✅      |
| Idempotency architecture      | ✅      |
| Retry architecture            | ✅      |
| Dead Letter architecture      | ✅      |
| OpenTelemetry                 | ✅      |
| Jaeger                        | ✅      |
| Prometheus                    | ✅      |
| W3C Trace Context             | ✅      |
| Kafka traceparent propagation | ✅      |
| Kubernetes/kind               | ✅      |
| MCP Server                    | ✅      |
| AI Agent                      | ✅      |
| Ollama                        | ✅      |

---

# Current Known Gap

One trace-related detail remains under investigation.

The Kafka header contains the propagated W3C context:

```text
traceparent=Some(
  "00-9c3c3750c0821c53b1b1fd89336bdcdd-0ca4604a606a3a16-01"
)
```

The Audit Consumer successfully extracts that context.

However, the deserialized `EventEnvelope` currently reports:

```text
traceparent=None
```

Therefore:

```text
Kafka traceparent header
        │
        └── working

OpenTelemetry context extraction
        │
        └── working

EventEnvelope.traceparent
        │
        └── requires investigation
```

This does not currently prevent distributed context propagation through the Kafka header, but preserving the traceparent consistently inside the event envelope remains an architectural cleanup item.

---

# Production Readiness

The platform currently has many characteristics expected from a production-oriented distributed system:

* Strong service boundaries
* Persistent transactional state
* Transactional Outbox
* Event-driven communication
* Schema governance
* Idempotent consumers
* Retry handling
* Dead Letter handling
* Authentication
* Authorization scopes
* Distributed tracing
* Metrics
* Kubernetes deployment
* AI operational interface
* Controlled MCP tool access
* Local LLM inference

However, **production-ready** should not yet be interpreted as “ready to deploy to a critical production environment without further hardening.”

Remaining areas include:

* Dedicated identity/login service
* Secret management
* TLS/mTLS where appropriate
* Production PostgreSQL topology and backups
* Kafka/Redpanda replication strategy
* Persistent storage strategy
* Kubernetes resource requests/limits
* PodDisruptionBudgets
* NetworkPolicies
* Production ingress
* Rate limiting
* Security scanning
* Dependency vulnerability management
* Load testing
* Failure/integration testing
* Disaster recovery
* Alerting
* SLO/SLI definitions
* Log retention
* Audit retention policies
* Production-grade AI model sizing and evaluation
* MCP authorization policies
* End-to-end trace validation in Jaeger

---

# Architectural Maturity

The project has evolved beyond a conventional CRUD backend.

Its architecture can be summarized as:

```text
                    FINANCIAL INTELLIGENCE PLATFORM

                         ┌──────────────┐
                         │ API Gateway  │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │ Audit Domain │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │ PostgreSQL   │
                         └──────┬───────┘
                                │
                         Transactional
                            Outbox
                                │
                         ┌──────▼───────┐
                         │ Outbox Worker│
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │ Redpanda     │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │ Audit        │
                         │ Consumer     │
                         └──────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
             OpenTelemetry             PostgreSQL
                    │                       │
                 Jaeger              Idempotency /
                                      DLQ / State


                    AI Operational Plane

                         ┌──────────────┐
                         │   AI Agent   │
                         └──────┬───────┘
                                │
                         ┌──────▼───────┐
                         │     MCP      │
                         └──────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                 Platform               Ollama
                 tools                    LLM
```

---

# Development Philosophy

The platform favors:

* Explicit service boundaries
* Strong typing
* Event contracts
* Asynchronous processing
* Observable systems
* Deterministic infrastructure access
* Failure isolation
* Idempotent operations
* Schema evolution
* AI as an operational interface rather than an uncontrolled infrastructure actor

The AI layer is deliberately separated from the transactional core.

The LLM does not directly own business state.

Instead:

```text
LLM
 │
 ▼
MCP
 │
 ▼
Controlled tools
 │
 ▼
Platform
```

This allows the AI layer to evolve independently from the core financial/audit processing system.

---

# Roadmap

## Phase 1 — Core Platform

* [x] Rust workspace
* [x] Audit Service
* [x] API Gateway
* [x] PostgreSQL
* [x] Transactional Outbox
* [x] Redpanda
* [x] Schema Registry
* [x] Avro
* [x] Audit Consumer

## Phase 2 — Reliability

* [x] Idempotency
* [x] Retry
* [x] Dead Letter handling
* [x] Event version routing
* [x] Schema validation

## Phase 3 — Observability

* [x] OpenTelemetry
* [x] Jaeger
* [x] Prometheus
* [x] Request IDs
* [x] W3C Trace Context
* [x] Kafka traceparent propagation

## Phase 4 — AI Platform

* [x] MCP Server
* [x] MCP tools
* [x] Rust AI Agent
* [x] Ollama
* [x] Local LLM inference
* [x] Deterministic MCP tool selection

## Phase 5 — Production Hardening

* [ ] Dedicated authentication/identity service
* [ ] Production secret management
* [ ] TLS/mTLS
* [ ] Production ingress
* [ ] Rate limiting
* [ ] Network policies
* [ ] Resource limits
* [ ] Production storage
* [ ] Backup/restore
* [ ] Disaster recovery
* [ ] Load testing
* [ ] Security testing
* [ ] SLO/SLI monitoring
* [ ] Alerting
* [ ] End-to-end trace validation
* [ ] AI evaluation and model strategy

---

# License

Internal / project-specific.

---

# Status

**Architecture:** Production-oriented

**Core event pipeline:** Operational

**Event reliability patterns:** Implemented

**Observability:** Operational

**AI/MCP layer:** Operational

**Kubernetes environment:** Operational

**Production hardening:** In progress
