FROM rust:1.97-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

COPY apps ./apps
COPY libs ./libs

RUN cargo build --release -p audit-consumer

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/audit-consumer /usr/local/bin/audit-consumer

ENV RUST_LOG=info

EXPOSE 3001

CMD ["audit-consumer"]