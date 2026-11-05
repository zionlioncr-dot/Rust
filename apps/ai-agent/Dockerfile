FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY apps ./apps
COPY libs ./libs

RUN cargo build --release --package ai-agent

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/ai-agent /usr/local/bin/ai-agent

ENV HTTP_HOST=0.0.0.0
ENV HTTP_PORT=8001
ENV MCP_URL=http://mcp-server:8000/mcp
ENV OLLAMA_CHAT_URL=http://ollama:11434/api/chat
ENV OLLAMA_MODEL=qwen3:0.6b
ENV RUST_LOG=info

EXPOSE 8001

ENTRYPOINT ["/usr/local/bin/ai-agent"]
