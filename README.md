# Financial Intelligence Platform

> Enterprise Event-Driven Microservices Platform built with Rust, PostgreSQL, Kafka (Redpanda), Axum and Hexagonal Architecture.

---

# Overview

Financial Intelligence Platform is an event-driven distributed system designed using modern enterprise architecture patterns.

Current architecture includes:

- Hexagonal Architecture
- Domain Driven Design (DDD)
- Repository Pattern
- Transactional Outbox Pattern
- Event-Driven Communication
- Kafka Messaging (Redpanda)
- API Gateway
- Audit Service
- Audit Consumer
- Outbox Worker
- Retry Executor
- Event Dispatcher
- Event Handler Registry
- OpenTelemetry Foundation
- Prometheus Metrics
- Redpanda Console

---

# Workspace Structure

```
financial-intelligence-platform/

├── apps/
│
│   ├── api-gateway/
│   ├── audit-service/
│   ├── audit-consumer/
│   └── outbox-worker/
│
├── libs/
│
│   ├── domain/
│   ├── repository/
│   ├── event-bus/
│   ├── telemetry/
│   ├── metrics/
│   ├── config/
│   └── common/
│
├── docker/
│
├── migrations/
│
└── docker-compose.yml
```

---

# Architecture

```
                  Client
                     │
                     ▼
               API Gateway
                     │
                     ▼
              Audit Service
                     │
                     ▼
             PostgreSQL
                     │
         Transactional Outbox
                     │
                     ▼
              Outbox Worker
                     │
                     ▼
          Kafka (Redpanda)
                     │
                     ▼
             Audit Consumer
                     │
                     ▼
            Event Dispatcher
                     │
                     ▼
              Event Handler
```

---

# Implemented Components

## API Gateway

- HTTP Entry Point
- Request Routing
- Health Endpoint
- Version Endpoint

---

## Audit Service

Responsible for:

- Creating audit events
- Persisting audit records
- Writing Outbox events atomically
- REST API

Endpoints

```
POST /audit

GET /health

GET /version

GET /metrics
```

---

## Repository Layer

Implemented using Repository Pattern.

Repositories include:

- AuditRepository
- OutboxRepository
- DeadLetterRepository (foundation)
- ProcessedEventRepository (foundation)

Database:

PostgreSQL

---

## Transactional Outbox

Implemented following the Transactional Outbox Pattern.

Workflow

```
REST Request

↓

Audit Event

↓

Database Transaction

↓

audit_events

+

outbox_events
```

Guarantees:

- Atomic persistence
- No lost events
- Reliable publishing

---

## Outbox Worker

Responsibilities:

- Poll unpublished events
- Publish to Kafka
- Mark events as published

Current status:

Running successfully.

Example logs:

```
Fetched unpublished events pending_events=0
```

---

## Event Bus

Kafka abstraction layer.

Current features:

- Kafka Producer
- Kafka Consumer
- Event Envelope
- Event Metadata

---

## Kafka

Using:

- Redpanda

Topics:

```
audit-events
```

Future:

```
audit-events-dlq
```

---

## Audit Consumer

Current flow:

```
Kafka

↓

Consumer

↓

Dispatcher

↓

Handler Registry

↓

Audit Handler

↓

Processing Service
```

---

## Retry Executor

Implemented.

Supports:

- Retry attempts
- Configurable retries
- Error propagation

DLQ integration planned.

---

## Event Dispatcher

Responsible for:

- Handler lookup
- Dispatching events
- Unknown event detection

---

## Handler Registry

Dynamic handler registration.

Current handlers:

- audit.created

---

## Telemetry

Current implementation:

- tracing
- structured logging
- Prometheus foundation

Future:

- Jaeger
- OpenTelemetry Collector

---

## Metrics

Prometheus metrics foundation.

Current metrics:

- HTTP Requests
- HTTP Duration
- Audit Events

---

## Redpanda Console

Integrated successfully.

Capabilities:

- Topic inspection
- Message visualization
- Consumer monitoring
- Broker monitoring
- Topic statistics

---

# Current Event Flow

```
POST /audit

↓

Audit Service

↓

audit_events

+

outbox_events

↓

Outbox Worker

↓

Kafka

↓

Audit Consumer

↓

Dispatcher

↓

Audit Handler

↓

Audit Processing Service
```

---

# Technology Stack

## Language

- Rust 1.97

---

## Frameworks

- Axum
- Tokio

---

## Database

- PostgreSQL

---

## Messaging

- Kafka
- Redpanda

---

## Serialization

- Serde

---

## Persistence

- SQLx

---

## Logging

- tracing

---

## Metrics

- Prometheus

---

## Containerization

- Docker
- Docker Compose

---

# Running

## Start Infrastructure

```
docker compose up -d
```

---

## Run API Gateway

```
cargo run -p api-gateway
```

---

## Run Audit Service

```
cargo run -p audit-service
```

---

## Run Outbox Worker

```
cargo run -p outbox-worker
```

---

## Run Audit Consumer

```
cargo run -p audit-consumer
```

---

# Build

```
cargo fmt

cargo check --workspace

cargo test --workspace
```

---

# Current Status

Implemented

- Hexagonal Architecture
- Repository Pattern
- Transactional Outbox
- PostgreSQL
- Kafka Producer
- Kafka Consumer
- Event Dispatcher
- Handler Registry
- Retry Executor
- API Gateway
- Audit Service
- Audit Consumer
- Outbox Worker
- Prometheus Foundation
- Telemetry Foundation
- Redpanda Console

---

# Roadmap

## Sprint 11

- Middleware Telemetry
- Automatic Metrics
- Request IDs
- Correlation IDs

---

## Sprint 12

- Idempotent Consumer

---

## Sprint 13

- Dead Letter Queue

---

## Sprint 14

- CQRS Read Models

---

## Sprint 15

- Event Replay

---

## Sprint 16

- Event Sourcing

---

## Sprint 17

- AI Integration
- MCP
- RAG
- Vector Search
- AI Agents

---

# Project Status

Current Version

```
v0.10.0
```

Status

**Production Architecture Foundation Completed**

Next milestone:

**Enterprise Observability (Prometheus + OpenTelemetry + Jaeger)**
