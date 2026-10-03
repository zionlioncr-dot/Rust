FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY target/release/audit-consumer /usr/local/bin/audit-consumer

ENV RUST_LOG=info

EXPOSE 3001

CMD ["audit-consumer"]
