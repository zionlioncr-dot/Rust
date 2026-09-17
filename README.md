# Financial Intelligence Platform

A distributed, event-driven financial intelligence platform built with **Rust, PostgreSQL, Redpanda/Kafka, Apache Avro, Schema Registry, OpenTelemetry, Jaeger, Prometheus, Kubernetes, MCP, and AI agents**.

The platform is designed around reliable event processing, transactional outbox patterns, observability, schema evolution, secure API access, and AI-assisted operational tooling.

---

## 1. Project Status

The core event-driven platform is currently running successfully on a local **Kind Kubernetes cluster**.

### Current status

| Component                | Status           |
| ------------------------ | ---------------- |
| Kubernetes / Kind        | ✅ Operational    |
| PostgreSQL               | ✅ Operational    |
| Audit Service            | ✅ Operational    |
| Transactional Outbox     | ✅ Operational    |
| Outbox Worker            | ✅ Operational    |
| Redpanda                 | ✅ Operational    |
| Schema Registry          | ✅ Operational    |
| Audit Consumer           | ✅ Operational    |
| Prometheus               | ✅ Operational    |
| OpenTelemetry            | ✅ Operational    |
| Jaeger                   | ✅ Operational    |
| Kafka Consumer Group     | ✅ Stable / Lag 0 |
| API Gateway              | 🚧 Next          |
| JWT Authentication       | 🚧 Next          |
| Authorization / Scopes   | 🚧 Next          |
| MCP Server on Kubernetes | 🚧 Next          |
| AI Agent on Kubernetes   | 🚧 Next          |
| Frontend                 | 🚧 Planned       |

The end-to-end audit event pipeline has already been validated in Kubernetes.

---

# 2. Architecture

The current architecture is based on asynchronous event processing and a transactional outbox.

```text
                         ┌──────────────────────┐
                         │      Client          │
                         │ Web / Mobile / API   │
                         └──────────┬───────────┘
                                    │
                              Future HTTPS
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     API Gateway      │
                         │                      │
                         │ JWT Validation       │
                         │ Authorization        │
                         │ Routing              │
                         │ Rate Limiting       │
                         │ Observability       │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Audit Service     │
                         │       Rust           │
                         └──────────┬───────────┘
                                    │
                           Transactional Write
                                    │
                    ┌───────────────┴────────────────┐
                    │                                │
                    ▼                                ▼
             audit_events                     outbox_events
                    │                                │
                    │                                ▼
                    │                         Outbox Worker
                    │                                │
                    │                         Avro Serialization
                    │                                │
                    │                                ▼
                    │                           Schema Registry
                    │                                │
                    │                                ▼
                    │                            Redpanda
                    │                                │
                    │                                ▼
                    │                         Audit Consumer
                    │                                │
                    │                                ▼
                    │                       processed_events
                    │
                    └──────────────────────────────────────┐
                                                           │
                                                           ▼
                                                  PostgreSQL
```

Observability runs alongside the platform:

```text
                     ┌─────────────────────┐
                     │   Rust Services     │
                     └─────────┬───────────┘
                               │
                 ┌─────────────┴─────────────┐
                 │                           │
                 ▼                           ▼
           Prometheus                 OpenTelemetry
           /metrics                       │
                 │                        ▼
                 │                      Jaeger
                 │
                 ▼
              Metrics
```

---

# 3. Event-Driven Flow

The validated Kubernetes flow is:

```text
HTTP POST /audit
        │
        ▼
audit-service
        │
        ├── INSERT audit_events
        │
        └── INSERT outbox_events
                    │
                    ▼
             outbox-worker
                    │
                    ├── Resolve Avro schema
                    │
                    ├── Serialize event
                    │
                    └── Publish to Redpanda
                              │
                              ▼
                       audit-events topic
                              │
                              ▼
                       audit-consumer
                              │
                              └── INSERT processed_events
```

This architecture provides reliable event publication without requiring the HTTP request handler to directly publish to Kafka/Redpanda.

---

# 4. Repository Structure

```text
financial-intelligence-platform/
│
├── apps/
│   ├── audit-service/
│   ├── audit-consumer/
│   ├── outbox-worker/
│   ├── api-gateway/
│   ├── mcp-server/
│   ├── ai-agent/
│   │
│   ├── analytics/
│   ├── embedding/
│   ├── llm-gateway/
│   ├── notification/
│   ├── rag/
│   └── workflow/
│
├── libs/
│   ├── bootstrap/
│   ├── common/
│   ├── domain/
│   ├── event-bus/
│   ├── health/
│   ├── http-server/
│   ├── kafka/
│   ├── metrics/
│   ├── repository/
│   └── telemetry/
│
├── migrations/
│   ├── 0001_create_audit_events.sql
│   ├── 0002_create_outbox_events.sql
│   ├── 0003_dead_letter.sql
│   ├── 0004_processed_events.sql
│   └── 0007_alter_processed_events.sql
│
├── k8s/
│   ├── namespace.yaml
│   ├── postgres/
│   ├── redpanda/
│   ├── jaeger/
│   ├── prometheus/
│   ├── audit-service/
│   ├── outbox-worker/
│   ├── audit-consumer/
│   ├── api-gateway/
│   ├── mcp-server/
│   └── ai-agent/
│
├── docker-compose.yml
├── Cargo.toml
├── Cargo.lock
├── .env
├── kind-config.yaml
└── README.md
```

---

# 5. Technology Stack

## Backend

* Rust
* Tokio
* Axum
* SQLx
* PostgreSQL
* Apache Avro
* Kafka protocol
* Redpanda
* Schema Registry
* OpenTelemetry
* Prometheus
* Jaeger

## Infrastructure

* Docker
* Docker Compose
* Kubernetes
* Kind
* Kubernetes Services
* PersistentVolumeClaims
* ConfigMaps
* Secrets

## AI / Tooling

* Model Context Protocol (MCP)
* `rmcp`
* Ollama
* AI Agent
* Tool-based deterministic workflows

---

# 6. Kubernetes Environment

The local Kubernetes cluster is:

```text
financial-cluster
```

Kind configuration:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: financial-cluster

nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

Current namespace:

```text
financial-platform
```

Check the cluster:

```bash
kubectl get nodes
```

Check all workloads:

```bash
kubectl get pods -n financial-platform
```

Check services:

```bash
kubectl get svc -n financial-platform
```

---

# 7. Kubernetes Components

## PostgreSQL

Internal Kubernetes address:

```text
postgres:5432
```

Database:

```text
financial
```

Credentials used by the local development environment:

```text
username: postgres
password: postgres
```

PostgreSQL uses a persistent volume.

Verify:

```bash
kubectl get pods -n financial-platform -l app=postgres
kubectl get pvc -n financial-platform
```

---

# 8. Redpanda

Redpanda is deployed as a StatefulSet.

Internal Kafka address:

```text
redpanda:9092
```

Admin API:

```text
redpanda:9644
```

Schema Registry:

```text
http://redpanda:8081
```

The current deployment uses the **integrated Redpanda Schema Registry**.

No separate Schema Registry deployment is required.

Check Redpanda:

```bash
kubectl get pods -n financial-platform -l app=redpanda
```

Check cluster:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  rpk cluster info
```

Check topics:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  rpk topic list
```

Current main topic:

```text
audit-events
```

---

# 9. Schema Registry

The current event subject is:

```text
AuditCreated-value
```

Compatibility mode:

```text
BACKWARD
```

Check subjects:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  curl -s http://localhost:8081/subjects
```

Check versions:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  curl -s http://localhost:8081/subjects/AuditCreated-value/versions
```

The current schema is an Avro record:

```text
financial.audit.events.AuditCreated
```

with:

```text
event_type
metadata
payload
version
```

The metadata contains:

```text
correlation_id
event_id
source
timestamp
trace_id
```

The payload contains:

```text
action
created_at
id
user
```

The version contains:

```text
major
minor
patch
```

---

# 10. Audit Service

The Audit Service is implemented in Rust using Axum.

Internal Kubernetes service:

```text
audit-service:3000
```

Endpoints:

```text
GET  /health
GET  /live
GET  /ready
GET  /metrics
GET  /version
POST /audit
```

Example:

```bash
kubectl port-forward -n financial-platform svc/audit-service 3000:3000
```

Then:

```bash
curl -i -X POST http://localhost:3000/audit \
  -H 'Content-Type: application/json' \
  -d '{"user":"alejandro","action":"KUBERNETES_TEST"}'
```

Example response:

```json
{
  "id": "fe1672c2-5f16-4753-bb08-3d76e92e2333",
  "user": "alejandro",
  "action": "KUBERNETES_TEST",
  "created_at": "2026-09-17T02:31:46.630764469Z"
}
```

---

# 11. Transactional Outbox

The Audit Service writes the business record and the outbox record as part of the database transaction.

Tables:

```text
audit_events
outbox_events
```

The Outbox Worker periodically searches for:

```text
published = false
```

events.

It then:

1. Loads the event.
2. Resolves the Avro schema.
3. Serializes the event.
4. Publishes it to Redpanda.
5. Marks the event as published.

Example validation:

```bash
kubectl exec -n financial-platform deploy/postgres -- \
  psql -U postgres -d financial -c \
  "SELECT id, event_type, published, created_at
   FROM outbox_events
   ORDER BY created_at DESC
   LIMIT 5;"
```

A successful event should show:

```text
published = t
```

---

# 12. Outbox Worker

The Outbox Worker runs independently from the Audit Service.

Configuration inside Kubernetes:

```text
DATABASE_URL=postgres://postgres:postgres@postgres:5432/financial
KAFKA_BROKERS=redpanda:9092
KAFKA_TOPIC=audit-events
SCHEMA_REGISTRY_URL=http://redpanda:8081
POLLING_INTERVAL=2
```

Check logs:

```bash
kubectl logs -n financial-platform deploy/outbox-worker --tail=100
```

Successful output includes:

```text
Event published successfully with Avro schema
```

---

# 13. Audit Consumer

The Audit Consumer reads:

```text
audit-events
```

using the consumer group:

```text
audit-group
```

The consumer:

1. Reads the Kafka event.
2. Resolves the Avro schema.
3. Deserializes the event.
4. Validates the event envelope.
5. Routes according to event version.
6. Processes the audit event.
7. Records successful processing in PostgreSQL.

Check the consumer group:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  rpk group describe audit-group
```

A healthy state should look like:

```text
STATE        Stable
TOTAL-LAG    0
```

The current Kubernetes deployment has been validated with:

```text
CURRENT-OFFSET  2
LOG-END-OFFSET  2
LAG             0
```

---

# 14. Idempotent Event Processing

Processed events are stored in:

```text
processed_events
```

Schema:

```text
event_id
consumer
handler
processed_at
```

The primary key on `event_id` provides an idempotency mechanism for consumer processing.

Example:

```bash
kubectl exec -n financial-platform deploy/postgres -- \
  psql -U postgres -d financial -c \
  "SELECT event_id, consumer, handler, processed_at
   FROM processed_events
   ORDER BY processed_at DESC
   LIMIT 5;"
```

---

# 15. Prometheus

Prometheus is deployed inside Kubernetes.

Internal service:

```text
prometheus:9090
```

The current scrape target is:

```text
audit-service:3000/metrics
```

Check Prometheus:

```bash
kubectl get pods -n financial-platform -l app=prometheus
```

Port-forward:

```bash
kubectl port-forward -n financial-platform svc/prometheus 9090:9090
```

Check targets:

```bash
curl -s http://localhost:9090/api/v1/targets
```

The current target is:

```text
audit-service:3000
```

with:

```text
health = up
```

The platform exposes metrics including:

```text
http_requests_total
```

Example:

```bash
curl -s http://localhost:9090/api/v1/query \
  --data-urlencode 'query=http_requests_total'
```

---

# 16. OpenTelemetry and Jaeger

OpenTelemetry is integrated into the Rust services through the shared `telemetry` library.

Current libraries include:

```text
opentelemetry       0.32
opentelemetry_sdk   0.32
opentelemetry-otlp  0.32
tracing-opentelemetry 0.33
```

The Kubernetes configuration uses:

```text
OTEL_SERVICE_NAME=audit-service
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
```

Jaeger receives OTLP over gRPC.

Internal service:

```text
jaeger:4317
```

Jaeger UI:

```text
jaeger:16686
```

Port-forward:

```bash
kubectl port-forward -n financial-platform svc/jaeger 16686:16686
```

Check registered services:

```bash
curl -s http://localhost:16686/api/services
```

Current result includes:

```text
audit-service
jaeger
```

The platform has successfully produced traces such as:

```text
operationName: audit.create
```

with attributes including:

```text
audit.action = KUBERNETES_TEST
audit.user   = alejandro
```

This confirms that OpenTelemetry → OTLP → Jaeger is working end-to-end.

---

# 17. Observability Architecture

The current observability architecture is:

```text
                    Rust Services
                         │
              ┌──────────┴──────────┐
              │                     │
              ▼                     ▼
         Prometheus            OpenTelemetry
              │                     │
              ▼                     ▼
           Metrics                Jaeger
```

The platform therefore provides both:

* Metrics
* Distributed tracing

The next step is to propagate correlation and trace information consistently across:

```text
API Gateway
     │
     ▼
Audit Service
     │
     ▼
Outbox Worker
     │
     ▼
Redpanda
     │
     ▼
Audit Consumer
```

---

# 18. API Gateway

The API Gateway application exists in the workspace and is planned as the external entry point for the platform.

Target architecture:

```text
Client
  │
  ▼
API Gateway
  │
  ├── Authentication
  ├── JWT validation
  ├── Authorization
  ├── Routing
  ├── Rate limiting
  ├── Request ID
  ├── CORS
  ├── Metrics
  └── Tracing
  │
  ▼
Internal Services
```

The API Gateway is the next major implementation step.

Planned protected endpoint:

```text
POST /api/audit
```

which will route to:

```text
audit-service:3000/audit
```

---

# 19. JWT Authentication

JWT-based authentication is planned at the API Gateway layer.

The Gateway will validate:

```text
Authorization: Bearer <JWT>
```

Expected validation includes:

* Signature
* Algorithm
* Issuer
* Audience
* Expiration
* Not-before timestamp
* Required claims
* Scopes / permissions

Example conceptual claims:

```json
{
  "sub": "user-123",
  "iss": "financial-platform",
  "aud": "api",
  "exp": 1789616000,
  "scope": "audit:read audit:write"
}
```

Authorization will be based on scopes/permissions.

Example:

```text
audit:read
audit:write
```

The Gateway should reject:

```text
401 Unauthorized
```

when authentication fails.

It should reject:

```text
403 Forbidden
```

when authentication succeeds but the required permission is missing.

---

# 20. MCP Server

The project includes an MCP server implemented using `rmcp`.

Current MCP endpoint:

```text
http://localhost:8000/mcp
```

The MCP server currently exposes tools for:

```text
system.health
audit.list_events
audit.get_event
audit.get_pending_outbox
kafka.list_topics
schema.get_latest
```

The MCP server has already been tested with MCP Inspector.

The Kubernetes deployment is planned as the next stage.

The internal Kubernetes architecture will use:

```text
mcp-server
    │
    ├── PostgreSQL
    ├── Redpanda
    ├── Schema Registry
    └── observability
```

---

# 21. AI Agent

The project includes an AI Agent that communicates with the MCP Server.

Current architecture:

```text
AI Agent
   │
   ▼
MCP Client
   │
   ▼
MCP Server
   │
   ├── PostgreSQL
   ├── Redpanda
   └── Schema Registry
```

The local AI Agent uses Ollama and has been tested with:

```text
qwen3:0.6b
```

The current development HTTP endpoint is:

```text
http://localhost:8001
```

The next stage is deploying both the MCP Server and AI Agent to Kubernetes.

---

# 22. Environment Variables

Example local `.env`:

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

Kubernetes uses internal service discovery instead of localhost.

For example:

```text
PostgreSQL:
postgres:5432

Redpanda:
redpanda:9092

Schema Registry:
http://redpanda:8081

Jaeger:
http://jaeger:4317
```

---

# 23. Local Docker Compose

Docker Compose remains available as a separate local development environment.

It should not be mixed with the Kubernetes environment during Kubernetes testing.

Before testing Kubernetes, stop the Compose application containers:

```bash
docker compose down
```

Do not remove volumes unless intentionally resetting the local environment.

Avoid:

```bash
docker compose down -v
```

unless a complete local data reset is intended.

Docker Desktop itself must remain running because Kind uses Docker as its container runtime.

---

# 24. Kubernetes Deployment Workflow

Start the Kind cluster:

```bash
kind create cluster --config kind-config.yaml
```

Verify:

```bash
kubectl get nodes
```

Create namespace:

```bash
kubectl apply -f k8s/namespace.yaml
```

Deploy infrastructure first:

```bash
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/redpanda/
kubectl apply -f k8s/jaeger/
kubectl apply -f k8s/prometheus/
```

Verify:

```bash
kubectl get pods -n financial-platform
```

Then deploy the Rust services:

```bash
kubectl apply -f k8s/audit-service/
kubectl apply -f k8s/outbox-worker/
kubectl apply -f k8s/audit-consumer/
```

Future deployments:

```bash
kubectl apply -f k8s/api-gateway/
kubectl apply -f k8s/mcp-server/
kubectl apply -f k8s/ai-agent/
```

---

# 25. Building Local Images for Kind

Build an image:

```bash
docker build -t audit-service:local -f apps/audit-service/Dockerfile .
```

Load it into Kind:

```bash
kind load docker-image audit-service:local \
  --name financial-cluster
```

Repeat for other services:

```text
outbox-worker:local
audit-consumer:local
api-gateway:local
mcp-server:local
ai-agent:local
```

The Kubernetes deployments use:

```yaml
imagePullPolicy: Never
```

for local Kind images.

---

# 26. Health Checks

Check all pods:

```bash
kubectl get pods -n financial-platform
```

Check services:

```bash
kubectl get svc -n financial-platform
```

Check deployments:

```bash
kubectl get deployments -n financial-platform
```

Check persistent volumes:

```bash
kubectl get pvc -n financial-platform
```

---

# 27. End-to-End Validation

A complete audit event test can be performed with:

```bash
curl -i -X POST http://localhost:3000/audit \
  -H 'Content-Type: application/json' \
  -d '{"user":"alejandro","action":"KUBERNETES_TEST"}'
```

Then verify PostgreSQL:

```bash
kubectl exec -n financial-platform deploy/postgres -- \
  psql -U postgres -d financial -c \
  "SELECT id, username, action, created_at
   FROM audit_events
   ORDER BY created_at DESC
   LIMIT 5;"
```

Verify outbox:

```bash
kubectl exec -n financial-platform deploy/postgres -- \
  psql -U postgres -d financial -c \
  "SELECT id, event_type, published, created_at
   FROM outbox_events
   ORDER BY created_at DESC
   LIMIT 5;"
```

Verify worker:

```bash
kubectl logs -n financial-platform deploy/outbox-worker --tail=50
```

Verify consumer:

```bash
kubectl logs -n financial-platform deploy/audit-consumer --tail=50
```

Verify processed events:

```bash
kubectl exec -n financial-platform deploy/postgres -- \
  psql -U postgres -d financial -c \
  "SELECT event_id, consumer, handler, processed_at
   FROM processed_events
   ORDER BY processed_at DESC
   LIMIT 5;"
```

Verify Kafka lag:

```bash
kubectl exec -n financial-platform redpanda-0 -- \
  rpk group describe audit-group
```

Expected:

```text
STATE        Stable
TOTAL-LAG    0
```

Verify Prometheus:

```bash
curl -s http://localhost:9090/api/v1/targets
```

Verify Jaeger:

```bash
curl -s http://localhost:16686/api/services
```

---

# 28. Current End-to-End Validation Result

The following event has successfully traversed the complete Kubernetes event pipeline:

```text
event:
fe1672c2-5f16-4753-bb08-3d76e92e2333

user:
alejandro

action:
KUBERNETES_TEST
```

The event was observed in:

```text
audit_events
        ↓
outbox_events
        ↓
Redpanda
        ↓
audit-consumer
        ↓
processed_events
```

The outbox record was marked:

```text
published = true
```

The consumer group reported:

```text
STATE = Stable
TOTAL-LAG = 0
```

The corresponding OpenTelemetry trace was also received by Jaeger:

```text
operationName = audit.create
audit.action = KUBERNETES_TEST
audit.user = alejandro
```

This validates the core distributed architecture.

---

# 29. Security Roadmap

Security is intentionally being implemented after the core event infrastructure has been validated.

Planned security layers:

```text
Internet
   │
   ▼
API Gateway
   │
   ├── TLS
   ├── JWT Authentication
   ├── Authorization
   ├── Rate Limiting
   └── Request Validation
   │
   ▼
Internal Services
   │
   ├── Kubernetes NetworkPolicies
   ├── Service-level authorization
   ├── Secrets
   └── Least-privilege access
```

Future security improvements include:

* JWT key rotation
* JWKS support
* Kubernetes Secrets
* NetworkPolicies
* service-to-service authentication
* TLS
* RBAC
* audit logging
* security headers
* rate limiting
* request size limits

---

# 30. Reliability Roadmap

Planned reliability improvements include:

* Dead-letter topic processing
* Retry backoff
* Consumer concurrency tuning
* Kafka partition scaling
* PostgreSQL connection pool tuning
* Persistent Prometheus storage
* Persistent Jaeger storage
* Kubernetes resource limits
* PodDisruptionBudgets
* Horizontal Pod Autoscaling
* NetworkPolicies
* readiness and liveness hardening

---

# 31. Testing Roadmap

The next testing stage will expand beyond manual integration tests.

Planned tests:

```text
Unit tests
Integration tests
Repository tests
Kafka integration tests
Schema compatibility tests
Outbox reliability tests
Consumer idempotency tests
API Gateway tests
JWT authentication tests
Authorization tests
MCP tool tests
AI Agent integration tests
End-to-end Kubernetes tests
```

---

# 32. CI/CD Roadmap

Future CI/CD should include:

```text
cargo fmt --check
cargo clippy
cargo test
cargo build --release
Docker image builds
Container security scanning
Kubernetes manifest validation
Integration tests
End-to-end tests
```

Deployment targets can later include:

```text
AWS EKS
ECS
EC2
Managed PostgreSQL
Managed Kafka / Redpanda
```

---

# 33. Development Principles

The project follows several architectural principles:

### Event-driven architecture

Business events are persisted and published asynchronously.

### Transactional outbox

Database state and event creation are committed atomically.

### Schema evolution

Events use Avro and Schema Registry compatibility rules.

### Idempotent consumers

Processed event identifiers prevent duplicate processing.

### Observability

Metrics and distributed traces are first-class platform capabilities.

### Kubernetes-native service discovery

Services communicate using Kubernetes DNS rather than localhost.

### Separation of concerns

Business logic, infrastructure, messaging, telemetry, and HTTP concerns are separated into reusable Rust libraries.

### AI through tools

AI agents interact with the platform through explicit MCP tools rather than unrestricted direct infrastructure access.

---

# 34. Roadmap

## Phase 1 — Core Platform

* [x] Rust workspace
* [x] PostgreSQL
* [x] Audit Service
* [x] Transactional Outbox
* [x] Redpanda
* [x] Avro
* [x] Schema Registry
* [x] Audit Consumer
* [x] Idempotent processing

## Phase 2 — Observability

* [x] Prometheus
* [x] Metrics endpoint
* [x] OpenTelemetry
* [x] OTLP
* [x] Jaeger
* [x] Distributed tracing validation

## Phase 3 — Kubernetes

* [x] Kind cluster
* [x] Kubernetes namespace
* [x] PostgreSQL deployment
* [x] Redpanda deployment
* [x] Jaeger deployment
* [x] Prometheus deployment
* [x] Audit Service deployment
* [x] Outbox Worker deployment
* [x] Audit Consumer deployment
* [x] End-to-end event validation

## Phase 4 — Security and API

* [ ] API Gateway
* [ ] JWT authentication
* [ ] Authorization
* [ ] Scopes / permissions
* [ ] Rate limiting
* [ ] Request correlation
* [ ] Gateway observability
* [ ] Kubernetes NetworkPolicies

## Phase 5 — MCP

* [x] MCP Server prototype
* [x] MCP tools
* [x] MCP Inspector validation
* [ ] Kubernetes MCP deployment
* [ ] MCP authentication
* [ ] MCP authorization

## Phase 6 — AI

* [x] AI Agent prototype
* [x] Ollama integration
* [x] MCP client
* [x] Deterministic tool routing
* [ ] Kubernetes AI Agent deployment
* [ ] Production model integration
* [ ] AI observability
* [ ] AI safety controls

## Phase 7 — Production Readiness

* [ ] CI/CD
* [ ] Automated integration tests
* [ ] Security scanning
* [ ] Persistent observability storage
* [ ] Autoscaling
* [ ] NetworkPolicies
* [ ] Secrets management
* [ ] Disaster recovery
* [ ] Production cloud deployment

---

# 35. License

This project is currently under active development.

License and commercial terms will be defined before production distribution.

---

# 36. Current Milestone

The platform has reached a significant architectural milestone:

```text
Rust
  +
PostgreSQL
  +
Transactional Outbox
  +
Redpanda
  +
Avro
  +
Schema Registry
  +
Idempotent Consumers
  +
Kubernetes
  +
Prometheus
  +
OpenTelemetry
  +
Jaeger
```

The complete event-driven pipeline has been validated in Kubernetes.

The next major milestone is:

```text
API Gateway
      +
JWT Authentication
      +
Authorization
      +
MCP
      +
AI Agent
```

The goal is to evolve the current event-driven backend into a secure, observable, Kubernetes-native financial intelligence platform with AI-assisted operational capabilities.
