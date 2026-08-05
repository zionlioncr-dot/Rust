# Financial Intelligence Platform

A production-oriented event-driven microservices platform built with Rust, PostgreSQL and Kafka (Redpanda), following modern distributed systems architecture patterns.

## Architecture

```
                    HTTP
                     │
                     ▼
             +----------------+
             | audit-service  |
             +----------------+
                     │
                     │ Transactional Outbox
                     ▼
              PostgreSQL
                     │
                     ▼
            +----------------+
            | outbox-worker  |
            +----------------+
                     │
                     ▼
          Kafka / Redpanda Topic
                     │
                     ▼
          +---------------------+
          | audit-consumer      |
          +---------------------+
                     │
                     ▼
             processed_events
```

---

# Technology Stack

- Rust (Edition 2021)
- Tokio
- Axum
- SQLx
- PostgreSQL
- Redpanda (Kafka API)
- Docker
- Docker Compose
- Prometheus
- Tracing
- UUID
- Serde
- Async Trait

---

# Current Features

## Audit Service

- REST API
- Transactional Outbox Pattern
- PostgreSQL persistence
- Event Envelope Builder
- Event Versioning
- Correlation ID
- Trace ID
- Metrics endpoint
- Structured logging

---

## Outbox Worker

- Polling publisher
- Kafka publisher
- Batch processing
- Publish metrics
- Failure metrics
- Structured logging

---

## Audit Consumer

- Kafka Consumer
- Event Dispatcher
- Handler Registry
- Event Version Router
- Schema Validation
- Retry Policy
- Dead Letter Queue
- Idempotency
- Metrics endpoint
- Structured logging

---

## Shared Libraries

### domain

- Domain Events
- Event Envelope
- Event Metadata
- Version Model

### repository

- PostgreSQL repositories
- Transactional Outbox
- Processed Events
- Dead Letter Repository

### kafka

- Kafka Producer
- Kafka Consumer

### telemetry

- Tracing configuration

### metrics

- audit_metrics
- consumer_metrics
- outbox_metrics

---

# Project Structure

```
financial-intelligence-platform/

apps/
│
├── api-gateway/
├── audit-service/
├── audit-consumer/
└── outbox-worker/

libs/
│
├── common/
├── domain/
├── repository/
├── kafka/
├── telemetry/
├── metrics/
├── event-bus/
└── http-server/

docker/
```

---

# Event Flow

```
POST /audit

        │
        ▼

audit-service

        │

stores audit
stores outbox

        │
        ▼

outbox-worker

        │

publishes event

        ▼

Redpanda Topic

        │

audit-consumer

        │

schema validation

        │

version routing

        │

retry policy

        │

idempotency

        │

dead letter

        ▼

processed_events
```

---

# Metrics

## Audit Service

```
GET /metrics
```

```
audit_requests_total
audit_created_total
```

---

## Audit Consumer

```
GET /metrics
```

```
audit_events_consumed_total
audit_events_processed_total
audit_events_failed_total
audit_events_dead_letter_total
audit_events_retry_total
```

---

## Outbox Worker

```
GET /metrics
```

```
outbox_events_published_total
outbox_events_failed_total
outbox_events_retry_total
```

---

# Retry Policy

- Exponential retry
- Configurable attempts
- Configurable delay
- Failure metrics

---

# Dead Letter Queue

Invalid events are automatically stored in PostgreSQL.

Table:

```
dead_letter_events
```

Includes:

- Event ID
- Event Type
- Payload
- Error
- Attempts
- Timestamp

---

# Idempotency

Processed events are stored in

```
processed_events
```

Duplicate events are ignored.

---

# Running the Project

## Infrastructure

```bash
docker compose up -d
```

---

## Audit Service

```bash
cargo run -p audit-service
```

---

## Outbox Worker

```bash
cargo run -p outbox-worker
```

---

## Audit Consumer

```bash
cargo run -p audit-consumer
```

---

# Testing

Create an audit event

```bash
curl -X POST http://localhost:3000/audit \
-H "Content-Type: application/json" \
-d '{
"user":"alejandro",
"action":"LOGIN"
}'
```

---

Check metrics

Audit Service

```bash
curl localhost:3000/metrics
```

Audit Consumer

```bash
curl localhost:3001/metrics
```

Outbox Worker

```bash
curl localhost:3002/metrics
```

---

# Implemented Patterns

- Transactional Outbox
- Event-Driven Architecture
- Repository Pattern
- Dependency Injection
- Retry Pattern
- Dead Letter Queue
- Idempotent Consumer
- Event Versioning
- Schema Validation
- Structured Logging
- Metrics per Service

---

# Roadmap

- OpenTelemetry
- Distributed Tracing
- Grafana Dashboards
- Health Checks
- Readiness Probes
- Graceful Shutdown
- Integration Tests
- Kubernetes Deployment
- Helm Charts
- CI/CD Pipeline
- Authentication & Authorization

---

# License

MIT License

---

Developed as a production-grade Rust microservices platform focused on scalability, resiliency and event-driven architecture.