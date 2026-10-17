# Financial Intelligence Platform

A production-oriented financial intelligence platform built around **Rust microservices, PostgreSQL, Kafka/Redpanda, Apache Avro, Schema Registry, Kubernetes, OpenTelemetry, Jaeger, Prometheus, MCP, and Ollama**.

The platform is designed around asynchronous event processing, transactional consistency, schema evolution, distributed tracing, idempotent consumers, and AI-assisted operational intelligence.

> **Status note:** This README consolidates the latest project documentation and known updates as of October 16, 2026. Historical successful checks do not guarantee that the current code, migrations, or Kubernetes workloads are healthy. Items marked pending require current verification.

---

## Architecture

```text
Client / API
    |
    | JWT / HTTP
    v
API Gateway (:8088)
    | JWT, scopes, request ID, trace propagation
    v
Audit Service (:3000) -------------------- Financial Transactions
    |                                                |
    v                                                v
PostgreSQL <--- transactional outbox ---> Outbox Worker
    |                                                |
    |                                                v
    +------------------------------------------> Redpanda / Kafka
                                                   |         |
                                                   v         v
                                            Audit Consumer  AML Service
                                                   |         |
                                                   v         +--> Deterministic AML rules
                                               PostgreSQL    +--> Regulatory RAG
                                                             +--> Ollama embeddings
                                                             +--> PostgreSQL/pgvector
                                                             +--> Idempotent AML alerts

Anti-Fraud consumes audit events and persists fraud state/alerts in PostgreSQL.

Schema Registry manages Avro event contracts.
OpenTelemetry exports traces to Jaeger; Prometheus scrapes application metrics.
The MCP Server exposes controlled operational tools to the Rust AI Agent, which uses Ollama for local inference.
```

## Project goals

- Event-driven architecture
- Transactional Outbox Pattern
- Kafka-compatible event streaming
- Apache Avro serialization and schema evolution
- Idempotent event processing
- Retry and dead-letter handling
- Distributed tracing and W3C Trace Context propagation
- OpenTelemetry and Prometheus metrics
- Kubernetes deployment
- JWT authentication and scope-based authorization
- MCP-based operational access
- Local AI inference through Ollama
- AI-assisted platform observability and analysis
- Financial transaction monitoring, anti-fraud, and AML analysis
- Regulatory RAG that distinguishes verified legal evidence from documents awaiting review

## Technology stack

### Backend

- Rust, Tokio, Axum, SQLx, PostgreSQL
- `rdkafka`, Apache Avro, Reqwest, Serde, JSON Web Tokens

### Event infrastructure

- Redpanda / Kafka protocol
- Confluent-compatible Schema Registry
- Transactional Outbox Pattern

### Observability

- OpenTelemetry, Jaeger, Prometheus
- W3C Trace Context and `traceparent`

### Containers and infrastructure

- Docker, Docker Compose, Kubernetes, kind

### AI

- Ollama and local LLM inference
- MCP and Rust MCP client/server (`rmcp`)
- PostgreSQL/pgvector-backed regulatory retrieval

## Workspace structure

```text
financial-intelligence-platform/
├── apps/
│   ├── api-gateway/
│   ├── audit-service/
│   ├── audit-consumer/
│   ├── outbox-worker/
│   ├── anti-fraud/
│   ├── aml-service/
│   ├── mcp-server/
│   └── ai-agent/
├── libs/
│   ├── common/
│   ├── domain/
│   ├── event-bus/
│   ├── health/
│   ├── kafka/
│   ├── repository/
│   └── telemetry/
├── deploy/kubernetes/
├── migrations/
├── schemas/
├── tools/regulatory-corpus/
├── docker-compose.yml
├── Cargo.toml
└── README.md
```

## Core audit event flow

1. The client sends an HTTP request to the API Gateway.
2. The Gateway validates JWT claims and required scopes and propagates request/trace context.
3. The Audit Service persists the audit event and an outbox event in one database transaction.
4. The Outbox Worker publishes pending events to Redpanda using the registered Avro schema and trace context.
5. The Audit Consumer decodes and validates the event, routes by version, applies idempotency, and handles retries/dead letters.

## Transactional Outbox

The Audit Service does not need Kafka to be available in order to commit an audit operation. It writes the business event and outbox record atomically:

```sql
BEGIN;
INSERT INTO audit_events (...);
INSERT INTO outbox_events (...);
COMMIT;
```

The Outbox Worker subsequently publishes the event to Redpanda. This establishes a reliable boundary between PostgreSQL state and asynchronous event publication.

The financial transaction repository follows the same principle: the `financial_transactions` row and its corresponding `outbox_events` row are inserted within the same SQL transaction. Preserve this atomicity in future changes.

## Kafka / Redpanda

Primary audit topic:

```text
audit-events
```

Kubernetes broker:

```text
redpanda:9092
```

Financial transactions are published to the `financial-transactions` topic for the AML pipeline.

The platform uses Kafka-compatible APIs through `rdkafka`. The producer uses Confluent-compatible Avro wire format: a magic byte, schema ID, and Avro binary payload. The consumer resolves the schema ID through Schema Registry before decoding.

## Schema Registry

The primary audit subject is:

```text
AuditCreated-value
```

The logical `AuditCreated` schema contains event type, metadata (correlation ID, event ID, source, timestamp, trace ID), payload (action, creation timestamp, ID, user), and version information (major, minor, patch).

Schema compatibility is configured through Schema Registry. The audit schema is intended to evolve backward-compatibly. The financial transaction Avro schema is maintained separately. New optional fields should use nullable unions and `default: null` where required for compatibility.

## Audit Consumer, idempotency, retry, and dead letters

The Audit Consumer performs:

1. Kafka subscription
2. Confluent Avro decoding and schema resolution
3. `EventEnvelope` deserialization and event validation
4. Version routing and event dispatch
5. Idempotency checking
6. Retry handling
7. Dead-letter processing
8. Distributed trace propagation

Idempotency is backed by PostgreSQL so duplicate Kafka deliveries do not repeat business effects. AML processing also uses a processed-event claim and inserts the corresponding alerts within a SQL transaction.

Intended failure flow:

```text
Event -> Process -> success: ACK/completed
                 -> failure: Retry -> success: completed
                                  -> retries exhausted: Dead Letter
```

Retry, duplicate delivery, consumer restart, and dead-letter recovery scenarios should be verified end to end in the target environment.

## Authentication and authorization

The API Gateway uses JWT validation with the following observed configuration:

```text
Algorithm: HS256
Issuer:    financial-intelligence-platform
Audience:  financial-api
```

Audit scopes:

```text
audit:read
audit:write
```

Typical mapping:

- `GET` → `audit:read`
- `POST`, `PUT`, `PATCH`, `DELETE` → `audit:write`

Example claims:

```json
{
  "sub": "user",
  "iss": "financial-intelligence-platform",
  "aud": "financial-api",
  "iat": 0,
  "exp": 0,
  "scopes": ["audit:read", "audit:write"],
  "tenant_id": "tenant-id"
}
```

The Gateway requires `tenant_id` in authenticated claims. The audit proxy removes an incoming `x-tenant-id` header and injects the tenant derived from the JWT claims. A dedicated user/login/identity service remains future work.

**MCP security caveat:** a `tenant_id` supplied as a tool argument is not a trusted identity by itself. MCP tools must have a verified authentication and authorization boundary before they can be considered to enforce secure tenant isolation.

## Financial transactions and multi-tenancy

The transaction creation route obtains `tenant_id` from authenticated JWT claims rather than trusting the request body.

The current transaction model includes:

- `id`, `tenant_id`, optional `external_transaction_id`
- `amount`, `currency`
- `source_account_id`, `destination_account_id`
- optional `counterparty`
- `country_code`, `channel`, optional `client_ip`
- `occurred_at`, `created_at`

The repository validates the tenant, aggregate identity, positive decimal amount, three-letter alphabetic currency, account IDs, country, and channel. The transaction and outbox event are committed atomically.

### Pending transaction-contract improvements

- Add explicit origin and destination geography without conflating a transaction's country with the tenant's regulatory jurisdiction.
- Capture a bank-reported FX rate only when it is present in the source institution's payload. Never invent a rate or assume an implicit rate.
- Define the meaning of original amount/currency, optional normalized amount, base currency, and conversion provenance/date.
- Update the Rust model, HTTP request, Avro schema, database migration, repository queries, consumers, and tests together.
- Preserve Avro backward compatibility for optional fields.
- Test rollback so that a failed outbox insert cannot leave a transaction persisted without its corresponding event.

## Anti-Fraud Service

The Anti-Fraud Service consumes audit events and applies deterministic rules. Capabilities observed in the project include:

- Sensitive-action rule
- `FailedLoginBurstRule`
- PostgreSQL-backed fraud state
- Tenant-aware processing and idempotent event handling
- Optional `client_ip` on fraud events

Documented `LOGIN_FAILED` burst severity thresholds:

- Low: 0–2
- Medium: 3–4
- High: 5–9
- Critical: 10+

These thresholds must be checked against current business requirements.

### Pending Anti-Fraud validation

- Repeat tenant-isolation tests with current data.
- Test concurrency, duplicate messages, consumer restarts, and state recovery.
- Keep bank-reported `client_ip` distinct from the network source IP used by the Gateway allowlist.
- Verify current Kubernetes deployment and rule behavior.

## AML Service

The AML service consumes transactions from `financial-transactions` using consumer group `aml-group`. Its HTTP port defaults to `3003`.

Observed code capabilities include:

- Tenant validation in consumed events and positive-amount validation
- High-value transaction rule
- High-risk-country rule
- Potential-structuring rule
- PostgreSQL persistence for AML alerts
- Idempotent processed-event records
- Regulatory RAG enrichment

Configuration observed during development:

```text
AML_HIGH_VALUE_THRESHOLD=10000
AML_STRUCTURING_MIN_TOTAL=10000
AML_STRUCTURING_MIN_TRANSACTIONS=5
AML_HIGH_RISK_COUNTRIES=...
```

The minimum allowed value for `AML_STRUCTURING_MIN_TRANSACTIONS` is 2. AML event processing uses a SQL transaction to claim the event idempotently and insert alerts. Preserve this atomicity.

**Deployment status must be rechecked.** Earlier checks reported that `cargo check -p aml-service` and three AML tests passed, but that does not prove the latest code, migrations, and Kubernetes deployment have all been validated.

### High-priority AML work

1. Do not compare amounts in different currencies against one numeric threshold without verifiable normalization.
2. Do not sum amounts in different currencies for structuring rules. Group by currency or use amounts normalized to an explicit common base currency.
3. Resolve regulatory jurisdiction from a trusted server-side tenant profile; do not infer it from transaction country or hardcode `CR` globally.
4. Define behavior for transactions without a usable conversion. Do not emit a monetary structuring signal that depends on incomparable sums.
5. Add tests for mixed currencies, missing conversions, bank-reported FX rates, tenant-specific jurisdiction, duplicate events, and tenant isolation.
6. Apply and verify the AML migration and test health endpoints, Kafka consumption, and database persistence in the target environment.

## Regulatory RAG and legal corpus

`RegulatoryRag` obtains embeddings through Ollama and queries `RagRepository`. Regulatory search requires 768-dimensional embeddings and filters by:

- Document type `regulation`
- Matching jurisdiction
- `verified=true`
- Non-empty regulatory code and article
- HTTPS source URL
- Effective date range covering the analysis date

Results include regulatory code, article, title, document version, effective dates, source URL, jurisdiction, similarity, and excerpt. When no current verified evidence is found, the status is `no_verified_current_evidence`. Retrieval errors can be represented as `retrieval_unavailable`.

### Identified source PDFs

- `tools/regulatory-corpus/downloads/conassif-12-21-v07.pdf`
- `tools/regulatory-corpus/downloads/sugef-11-18-v11.pdf`
- `tools/regulatory-corpus/downloads/sugef-13-19-v6.pdf`

Manifest builder: `tools/regulatory-corpus/build_manifest.py`.

Text extraction was inspected for `conassif-12-21-v07.pdf`; a search found 68 exact matches in plain-text mode.

### Legal verification status

Manifest records observed during inspection were marked:

```text
verified=false
verification_status=pending_legal_review
effective_from=null
effective_to=null
```

Some records had official URLs, but effective dates were unresolved. **Do not mark documents verified or invent effective dates merely to make RAG return results.** Verification must be based on official sources and appropriate legal review.

### Pending RAG work

- Improve PDF extraction for words split across lines, headers/footers, numbering, and article references.
- Preserve traceability between excerpt, page, source document, and official URL.
- Complete legal review of regulation code, article, jurisdiction, official URL, and effective period before marking a record verified.
- Clearly distinguish candidates awaiting review from current verified evidence.
- Test that pending, expired, or incomplete records are not presented as verified legal evidence.
- Resolve jurisdiction from the tenant profile instead of using the hardcoded `CR` value.

## Tenant regulatory profile and jurisdiction

A trusted server-side source must define each tenant's regulatory attributes. Depending on business requirements, the profile may include:

- `tenant_id`
- Primary regulatory jurisdiction
- Base currency for aggregations and thresholds
- Applicable AML threshold policy
- Effective dates and profile status
- Configuration provenance and audit metadata

Agree on the schema and constraints before creating the migration. A transaction's country is not a substitute for the tenant's legal jurisdiction. Organizations operating in multiple countries may need multiple jurisdictional profiles rather than a single jurisdiction column.

## FX and currency normalization

A monetary comparison is valid only when both amounts use the same currency and any conversion is traceable. The transaction contract should distinguish:

- `amount` and `currency`: the original amount and currency reported by the bank
- Bank-reported exchange rate, if available
- Source and target currencies for the rate
- Rate timestamp, source, and type, when available
- Optional normalized amount and `base_currency`
- Normalization status and the reason conversion could not be performed

Never invent exchange rates, assume an implicit rate, or sum amounts in different currencies. High-value and structuring rules must use comparable amounts or explicitly defined currency-specific policies. Conversion precision, rounding, reference date, and provenance must be defined.

## MCP Server and AI Agent

The MCP server initializes PostgreSQL/RAG repositories and an embedding client. Existing operational tools include:

```text
audit.get_event
audit.list_events
audit.get_pending_outbox
kafka.list_topics
schema.get_latest
system.health
```

The AI Agent uses MCP tools for operational data and Ollama for local LLM inference. The architecture keeps the LLM separate from the transactional core: the LLM does not directly own business state.

The agent has been tested with Ollama using `qwen3:0.6b`. Example operational questions include:

- How many pending events are in the outbox?
- What is the latest Avro schema for `AuditCreated-value`?
- Is the platform healthy?

Deterministic tool selection is intended to reduce unnecessary LLM reasoning when a question maps directly to an MCP operation.

### MCP security work pending

- Review authentication and authorization for every MCP tool.
- Do not trust a caller-provided `tenant_id` without a verified identity boundary.
- Add a regulatory lookup tool that resolves jurisdiction from the trusted tenant profile.
- Prevent the agent from citing documents pending legal review as current legal authority.
- Test cross-tenant access and rejection of unauthorized calls.

## Distributed tracing

The platform implements W3C Trace Context. The observed propagation path is:

```text
API Gateway -> Audit Service -> PostgreSQL Outbox -> Outbox Worker
            -> Redpanda (Kafka traceparent header) -> Audit Consumer
            -> OpenTelemetry Context -> Audit Dispatcher -> Audit Processing
```

A real audit request previously demonstrated trace propagation. The same W3C `traceparent` was observed in the Outbox Worker, Kafka message, and Audit Consumer; the consumer extracted the context and processed the event under the propagated trace.

### Known trace-envelope gap

The Kafka header contains the propagated W3C context and the Audit Consumer extracts it successfully. However, the deserialized `EventEnvelope` was observed with:

```text
traceparent=None
```

Kafka-header propagation and OpenTelemetry context extraction worked in the observed test, but preserving `traceparent` consistently inside the event envelope remains an investigation item. This did not prevent propagation through the Kafka header in that test.

Telemetry is considered functional. Do not change the telemetry library while addressing unrelated transaction, AML, or RAG work.

## OpenTelemetry and Prometheus

Example Kubernetes configuration:

```text
OTEL_SERVICE_NAME=audit-consumer
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
```

Jaeger endpoints:

```text
4317   OTLP gRPC
4318   OTLP HTTP
16686  Jaeger UI
```

Expected traced services include `api-gateway`, `audit-service`, `outbox-worker`, and `audit-consumer`.

The platform exposes application metrics for Prometheus scraping, including consumer activity and application-level counters. `/metrics` is the documented metrics endpoint; verify the actual endpoint for each service before relying on it operationally.

## Kubernetes

Local kind cluster:

```text
financial-cluster
```

Namespace:

```text
financial-platform
```

Inspect workloads:

```bash
kubectl config current-context
kubectl get nodes
kubectl get pods -n financial-platform -o wide
kubectl get deployments,services -n financial-platform
kubectl get events -n financial-platform --sort-by=.lastTimestamp
```

Workloads observed across the project include `api-gateway`, `audit-service`, `audit-consumer`, `outbox-worker`, `anti-fraud`, `aml-service`, `postgres`, `redpanda`, `jaeger`, `prometheus`, `ollama`, and `redis`. Check the current state before assuming every service is deployed and healthy.

Example: load a local image into kind:

```bash
kind load docker-image audit-consumer:local \
  --name financial-cluster
```

Restart and verify a deployment:

```bash
kubectl rollout restart deployment/audit-consumer \
  -n financial-platform
kubectl rollout status deployment/audit-consumer \
  -n financial-platform
kubectl logs -n financial-platform deployment/audit-consumer --timestamps
```

## Local development

Build the workspace:

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

Check formatting without modifying files:

```bash
cargo fmt --all -- --check
```

Run tests:

```bash
cargo test --workspace
```

AML-specific checks:

```bash
cargo check -p aml-service
cargo test -p aml-service
```

These commands validate the code; they do not deploy services or apply database migrations.

## API Gateway

Default local endpoint:

```text
http://localhost:8088
```

Audit endpoint: `POST /audit`.

Example:

```bash
curl -i -X POST http://localhost:8088/audit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user":"example-user","action":"TEST_EVENT"}'
```

The Gateway's `/health` and `/ready` endpoints returned successful responses in previous checks. Recheck against the current environment:

```bash
curl -i http://localhost:8088/health
curl -i http://localhost:8088/ready
```

## PostgreSQL and migrations

PostgreSQL is the transactional source of truth. Audit-related tables include:

```text
audit_events
outbox_events
processed_events
dead_letter_events
```

The financial transaction pipeline uses `financial_transactions` and `outbox_events`. AML processing uses its own processed-event and alert persistence.

Migrations identified during development:

- `0016_fraud_event_idempotency.sql`
- `0017_fraud_alert_event_associations.sql`
- `0018_create_financial_transactions.sql`
- `0019` — associated with AML integration; confirm its exact filename, contents, and whether it has been applied in each environment.

The next manually numbered migration should start at `0020`.

Before applying migrations:

1. Review the migration against the actual database schema.
2. Confirm the project's migration tool and procedure.
3. Apply it to a controlled database first.
4. Verify columns, constraints, indexes, and data.
5. Confirm rollback and recovery procedures.
6. Do not claim a migration has run merely because its file exists.

## Redpanda and Schema Registry

List topics:

```bash
kubectl exec -n financial-platform deploy/redpanda -- rpk topic list
```

Topics:

```text
audit-events
financial-transactions
```

Schema Registry inside Kubernetes:

```text
http://redpanda:8081
```

Primary audit subject: `AuditCreated-value`.

The locally configured URL `http://localhost:18081` was observed not to respond during an earlier check. Verify the current local port-forward or service configuration before using it.

## Current validation status

The following capabilities were exercised or reported as operational during previous development checks. This is historical evidence, not a guarantee of current health.

| Capability | Previously reported status |
|---|---|
| Rust workspace | Verified in prior checks; rerun against current revision |
| Audit Service | Exercised |
| API Gateway | Exercised |
| JWT validation and scopes | Exercised |
| PostgreSQL | Exercised |
| Transactional Outbox | Exercised |
| Redpanda/Kafka and Avro | Exercised |
| Schema Registry | Exercised |
| Audit Consumer | Exercised |
| Idempotency, retry, dead-letter architecture | Implemented; revalidate end to end |
| OpenTelemetry and Jaeger | Operational in prior checks |
| Prometheus | Present in the environment |
| W3C Trace Context and Kafka `traceparent` | Exercised; envelope field still needs investigation |
| Kubernetes/kind | Used for local deployment; inspect current workloads |
| MCP Server, AI Agent, Ollama | Exercised; authorization review remains |
| Anti-Fraud | Implemented; repeat current integration checks |
| AML | Code and some tests reported; revalidate deployment and end-to-end flow |
| Regulatory corpus legal verification | Pending |
| Tenant-specific regulatory jurisdiction | Pending |
| Safe cross-currency AML aggregation | Pending |
| Bank-reported FX contract | Pending |
| MCP tenant authorization | Requires review |

## Production readiness

The platform has many characteristics expected from a production-oriented distributed system: clear service boundaries, persistent transactional state, a transactional outbox, event-driven communication, schema governance, idempotent consumers, retry/dead-letter handling, Gateway authentication, scopes, distributed tracing, metrics, Kubernetes deployment, an AI operational interface, and local LLM inference.

However, **production-oriented does not mean ready for a critical production environment without further hardening**.

Remaining areas include:

- Dedicated identity/login service
- Production secret management
- TLS/mTLS where appropriate
- Production PostgreSQL topology and backups
- Kafka/Redpanda replication strategy
- Persistent storage strategy
- Kubernetes resource requests/limits and PodDisruptionBudgets
- NetworkPolicies and production ingress
- Rate limiting
- Security scanning and dependency vulnerability management
- Load, failure, and integration testing
- Disaster recovery and alerting
- SLO/SLI definitions
- Log and audit retention policies
- Production-grade AI model sizing and evaluation
- MCP authorization policies
- End-to-end trace validation in Jaeger

## Development philosophy

The project favors explicit service boundaries, strong typing, event contracts, asynchronous processing, observable systems, deterministic infrastructure access, failure isolation, idempotent operations, schema evolution, and AI as an operational interface rather than an uncontrolled infrastructure actor.

The AI layer is deliberately separated from the transactional core. The LLM does not directly own business state; it invokes controlled tools through MCP.

## Roadmap

### Phase 1 — Core platform

- [x] Rust workspace
- [x] Audit Service
- [x] API Gateway
- [x] PostgreSQL
- [x] Transactional Outbox
- [x] Redpanda
- [x] Schema Registry
- [x] Avro
- [x] Audit Consumer

### Phase 2 — Reliability

- [x] Idempotency
- [x] Retry architecture
- [x] Dead-letter architecture
- [x] Event version routing
- [x] Schema validation

### Phase 3 — Observability

- [x] OpenTelemetry
- [x] Jaeger
- [x] Prometheus integration
- [x] Request IDs
- [x] W3C Trace Context
- [x] Kafka `traceparent` propagation
- [ ] Resolve the `EventEnvelope.traceparent` persistence/representation gap

### Phase 4 — AI platform

- [x] MCP Server and tools
- [x] Rust AI Agent
- [x] Ollama and local LLM inference
- [x] Deterministic MCP tool selection
- [ ] Enforce and test MCP authentication and tenant authorization
- [ ] Add tenant-aware regulatory lookup

### Phase 5 — Financial intelligence

- [x] Financial transaction model and ingestion path observed in code
- [x] Atomic transaction + outbox persistence
- [x] Anti-Fraud rules and persistence observed in code
- [x] AML rules and idempotent alert persistence observed in code
- [ ] Verify current AML deployment and end-to-end consumption
- [ ] Resolve regulatory jurisdiction from a trusted tenant profile
- [ ] Prevent AML comparisons and aggregation across incompatible currencies
- [ ] Add bank-reported FX fields with provenance
- [ ] Add explicit transaction origin/destination geography
- [ ] Complete legal verification and effective dates for the regulatory corpus
- [ ] Improve PDF extraction and traceable citations
- [ ] Add end-to-end tenant-isolation and failure-recovery tests

### Phase 6 — Production hardening

- [ ] Dedicated authentication/identity service
- [ ] Production secret management
- [ ] TLS/mTLS
- [ ] Production ingress and rate limiting
- [ ] Network policies and resource limits
- [ ] Production storage and backup/restore
- [ ] Disaster recovery
- [ ] Load and security testing
- [ ] SLO/SLI monitoring and alerting
- [ ] End-to-end trace validation
- [ ] AI evaluation and model strategy

## License

Internal / project-specific.

## Status summary

- **Architecture:** Production-oriented
- **Core audit event pipeline:** Previously exercised
- **Event reliability patterns:** Implemented; revalidate against the current revision
- **Observability:** Operational in prior checks; preserve the existing telemetry implementation
- **AI/MCP layer:** Implemented and exercised; authorization review remains
- **Kubernetes environment:** Used for local development; current workload status must be checked
- **Anti-Fraud:** Implemented; current integration checks remain necessary
- **AML:** Implementation underway; currency semantics, tenant jurisdiction, legal RAG validation, and deployment verification remain
- **Regulatory corpus:** Extraction work underway; legal verification pending
- **Production hardening:** In progress
