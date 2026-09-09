FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

COPY apps ./apps
COPY libs ./libs

RUN cargo build --release --package mcp-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/mcp-server /usr/local/bin/mcp-server

ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000
ENV KAFKA_BROKERS=redpanda:9092
ENV SCHEMA_REGISTRY_URL=http://redpanda:8081

EXPOSE 8000

CMD ["mcp-server"]