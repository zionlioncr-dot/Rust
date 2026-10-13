FROM rust:1.97-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

COPY apps ./apps
COPY libs ./libs

RUN cargo build --release -p anti-fraud

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/anti-fraud /usr/local/bin/anti-fraud

ENV RUST_LOG=info

CMD ["anti-fraud"]
