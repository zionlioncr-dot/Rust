FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY target/release/api-gateway /usr/local/bin/api-gateway

EXPOSE 8088

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/api-gateway"]