# Financial Intelligence Platform

> High-performance Event-Driven Financial Intelligence Platform built with Rust.

## Overview

Financial Intelligence Platform is a modular, event-driven microservices architecture designed around modern distributed systems principles.

The platform implements several enterprise patterns including:

* Transactional Outbox Pattern
* Inbox Pattern (Idempotent Consumers)
* Domain Events
* Dependency Injection
* Repository Pattern
* Event Dispatcher
* Handler Registry
* Kafka-based Event Streaming
* PostgreSQL Persistence

The objective is to provide a scalable foundation for processing millions of financial events with reliability, consistency, and high throughput.

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
      Transactional Outbox
                    │
                    ▼
              Kafka Broker
                    │
                    ▼
            Audit Consumer
                    │
              Event Dispatcher
                    │
              Handler Registry
                    │
             Audit Handler
                    │
       Audit Processing Service
                    │
         Idempotency Service
                    │
       ProcessedEventRepository
                    │
           PostgreSQL Database
```

---

# Workspace

```
financial-intelligence-platform/

apps/
│
├── api-gateway/
├── audit-service/
├── audit-consumer/
├── outbox-worker/
│
libs/
│
├── common/
├── domain/
├── event-bus/
├── kafka/
├── repository/
├── telemetry/
```

---

# Current Features

## Audit Service

* REST API
* Audit Event persistence
* Transactional Outbox
* PostgreSQL
* Event Envelope generation

---

## Outbox Worker

* Poll unpublished events
* Publish events to Kafka
* Mark events as published
* Retry-ready architecture

---

## Audit Consumer

* Kafka Consumer
* Event Dispatcher
* Dynamic Handler Registry
* Audit Handler
* Dependency Injection
* Inbox Pattern
* Idempotent Processing

---

## Repository Layer

Current repositories:

* AuditRepository
* OutboxRepository
* ProcessedEventRepository

Unified PostgreSQL implementation:

```
PostgresRepository
```

---

# Enterprise Patterns

Implemented patterns:

* Repository Pattern
* Transactional Outbox
* Inbox Pattern
* Dependency Injection
* Domain Events
* Event Envelope
* Event Dispatcher
* Handler Registry
* Idempotent Consumer

---

# Technology Stack

## Language

* Rust 2021

## Database

* PostgreSQL

## Messaging

* Apache Kafka

## Async Runtime

* Tokio

## ORM

* SQLx

## Serialization

* Serde

## Logging

* Tracing

## Containerization

* Docker
* Docker Compose

---

# Current Flow

```
POST /audit
      │
      ▼
AuditService
      │
      ▼
AuditRepository
      │
      ▼
OutboxRepository
      │
      ▼
PostgreSQL
      │
Outbox Worker
      │
      ▼
Kafka
      │
      ▼
Audit Consumer
      │
      ▼
Dispatcher
      │
      ▼
Audit Handler
      │
      ▼
AuditProcessingService
      │
      ▼
IdempotencyService
      │
      ▼
ProcessedEventRepository
```

---

# Project Status

## Completed

* Rust Workspace
* Modular Architecture
* PostgreSQL Integration
* Kafka Integration
* Transactional Outbox
* Inbox Pattern
* Event Dispatcher
* Handler Registry
* Dependency Injection
* Repository Layer
* Idempotency
* Consumer Architecture

---

## In Progress

* Retry Engine
* Dead Letter Queue
* Health Checks
* Metrics
* Configuration Layer

---

## Planned

* CQRS
* Event Replay
* Saga Orchestrator
* OpenTelemetry
* Prometheus
* Grafana
* Kubernetes Deployment
* Horizontal Scaling
* Distributed Tracing

---

# Build

```bash
cargo fmt

cargo check --workspace

cargo test --workspace
```

---

# Running

Start PostgreSQL and Kafka:

```bash
docker compose up -d
```

Run the Audit Service:

```bash
cargo run -p audit-service
```

Run the Outbox Worker:

```bash
cargo run -p outbox-worker
```

Run the Audit Consumer:

```bash
cargo run -p audit-consumer
```

---

# Roadmap

## Sprint 14

* Retry Engine
* Dead Letter Queue
* Health Checks
* Configuration Layer
* Metrics

## Sprint 15

* Event Replay
* CQRS Read Models

## Sprint 16

* Saga Orchestrator

## Sprint 17

* Observability

## Sprint 18

* Kubernetes Deployment

---

# Author

Alejandro Retana

Financial Intelligence Platform

Built with Rust using Event-Driven Architecture principles.
