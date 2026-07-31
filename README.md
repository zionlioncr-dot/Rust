# Financial Intelligence Platform

[![Rust](https://img.shields.io/badge/Rust-1.87+-orange.svg)](https://www.rust-lang.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Redpanda](https://img.shields.io/badge/Redpanda-Kafka-red.svg)](https://redpanda.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Microservices-success.svg)]()

---

# Financial Intelligence Platform

High-throughput event-driven platform written in Rust following modern backend architecture patterns:

- Clean Architecture
- Domain Driven Design (DDD)
- Event Driven Architecture (EDA)
- Transactional Outbox
- CQRS Ready
- Kafka / Redpanda
- Idempotent Consumers
- Retry Policies
- Dead Letter Queue
- Dependency Injection
- OpenTelemetry Ready
- Prometheus Metrics
- Kubernetes Ready

---

# Architecture

```
                  +------------------+
                  |    API Gateway   |
                  +---------+--------+
                            |
                            |
                    HTTP / REST
                            |
                            v
                  +------------------+
                  |  Audit Service   |
                  +---------+--------+
                            |
          PostgreSQL Transaction
                            |
                            v
                  +------------------+
                  | Outbox Events    |
                  +---------+--------+
                            |
                 Outbox Worker
                            |
                            v
                     Redpanda / Kafka
                            |
                            |
                +-----------+-----------+
                |                       |
                v                       v
       Audit Consumer         Future Consumers
                |
                v
      Business Processing
                |
                v
       Processed Events
```

---

# Workspace

```
financial-intelligence-platform/

apps/
    api-gateway/
    audit-service/
    outbox-worker/
    audit-consumer/

libs/
    common/
    domain/
    repository/
    kafka/
    event-bus/
    telemetry/
    metrics/

docker-compose.yml
Cargo.toml
```

---

# Current Features

## API Gateway

- Reverse proxy ready
- Middleware support
- Request ID
- Logging
- Authentication ready

---

## Audit Service

- Create Audit Events
- Persist to PostgreSQL
- Transactional Outbox
- Health endpoint
- Version endpoint
- Metrics endpoint

---

## Outbox Worker

Implements the Transactional Outbox Pattern.

Workflow

```
Audit Service

↓

audit_events

↓

outbox_events

↓

Kafka Producer

↓

Redpanda
```

Features

- Batch publishing
- Configurable polling interval
- Automatic publish flag
- Retry ready

---

## Audit Consumer

Features

- Kafka Consumer
- Handler Registry
- Retry Policy
- Dead Letter Queue
- Idempotency
- Event Dispatcher

---

# Event Flow

```
POST /audit

↓

AuditEvent

↓

OutboxEvent

↓

Redpanda Topic

↓

Audit Consumer

↓

Audit Handler

↓

Business Logic

↓

processed_events
```

---

# Event Envelope

Every event is wrapped in a standard envelope.

```json
{
  "metadata": {
    "event_id": "...",
    "correlation_id": "...",
    "trace_id": "...",
    "source": "...",
    "timestamp": "..."
  },
  "version": {
    "major": 1,
    "minor": 0,
    "patch": 0
  },
  "event_type": "AuditCreated",
  "payload": {}
}
```

---

# Event Versioning

Current Version

```
1.0.0
```

Future compatible.

Supports:

- Major
- Minor
- Patch

---

# Implemented Patterns

- Repository Pattern
- Builder Pattern
- Dependency Injection
- Event Dispatcher
- Handler Registry
- Retry Pattern
- Dead Letter Queue
- Idempotency Pattern
- Transactional Outbox
- Event Envelope
- Event Versioning

---

# Observability

Current

- Structured Logging
- Tracing
- Prometheus Metrics
- Health Checks
- Version Endpoint

Endpoints

```
GET /health

GET /metrics

GET /version
```

---

# Technologies

| Technology | Version |
|------------|----------|
| Rust | stable |
| Tokio | latest |
| Axum | latest |
| SQLx | latest |
| PostgreSQL | 16 |
| Redpanda | Kafka Compatible |
| Docker | latest |
| Prometheus | Ready |
| OpenTelemetry | Ready |

---

# Database

Current tables

```
audit_events

outbox_events

processed_events

dead_letter_events
```

---

# Build

```
cargo fmt

cargo clippy

cargo check --workspace

cargo test --workspace
```

---

# Running

## Infrastructure

```
docker compose up -d
```

---

## API

```
cargo run -p audit-service
```

---

## Worker

```
cargo run -p outbox-worker
```

---

## Consumer

```
cargo run -p audit-consumer
```

---

# Test

Create an audit event

```
curl -X POST http://localhost:3000/audit \
-H "Content-Type: application/json" \
-d '{
    "user":"alejandro",
    "action":"LOGIN"
}'
```

Health

```
curl http://localhost:3000/health
```

Version

```
curl http://localhost:3000/version
```

Metrics

```
curl http://localhost:3000/metrics
```

---

# Current Status

## Completed

- Workspace Architecture
- Dependency Injection
- Audit Service
- Transactional Outbox
- Kafka Producer
- Kafka Consumer
- Event Dispatcher
- Handler Registry
- Retry Executor
- Dead Letter Service
- Idempotency
- Event Envelope
- Event Versioning
- Health Endpoint
- Metrics Endpoint
- Version Endpoint

---

## In Progress

- End-to-End Integration Tests
- Distributed Tracing
- Prometheus Dashboard
- Grafana Dashboard
- Docker Production Images

---

## Planned

- Schema Registry
- Kafka Streams
- CQRS Read Models
- Event Sourcing
- Saga Orchestrator
- Redis Cache
- gRPC Services
- Kubernetes Deployment
- Horizontal Scaling
- GitHub Actions CI/CD
- Terraform Infrastructure
- AWS Deployment

---

# Design Principles

- SOLID
- Clean Architecture
- Domain Driven Design
- Hexagonal Architecture
- Event Driven Architecture
- Observable Systems
- High Throughput
- Fault Tolerant
- Cloud Native

---

# License

MIT

---

# Author

Alejandro Retana

Financial Intelligence Platform

2026