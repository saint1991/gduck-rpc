# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.82
ARG GRPC_HEALTH_PROBE_VERSION=0.4.35

FROM rust:${RUST_VERSION}-slim-bookworm AS builder

WORKDIR /home/gduck
RUN apt-get update -y \
 && apt-get install -y git g++ cmake ninja-build libssl-dev protobuf-compiler

COPY . /home/gduck
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/gduck/target \
    cargo build --release \
 && mkdir /home/gduck/dist \
 && cp /home/gduck/target/release/gduck /home/gduck/dist 

FROM builder AS probe

ARG GRPC_HEALTH_PROBE_VERSION

RUN apt-get update -y \
 && apt-get install -y wget \
 && wget -O /grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64

FROM debian:bookworm-slim

COPY --from=probe --chmod=100 /grpc_health_probe /usr/local/bin
COPY --from=builder /home/gduck/dist/gduck /gduck
ENV RUST_LOG=info RUST_BACKTRACE=1

HEALTHCHECK --interval=10s --timeout=5s --start-period=3s --retries=3 \
    CMD [ "grpc_health_probe", "--addr=127.0.0.1:50051" ]

ENTRYPOINT ["/gduck", "--port", "50051"]
