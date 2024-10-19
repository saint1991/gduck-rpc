# syntax=docker/dockerfile:1

FROM rust:1.82-slim-bookworm AS builder

WORKDIR /home/gduck
RUN apt-get update -y \
 && apt-get install -y git g++ cmake ninja-build libssl-dev protobuf-compiler

COPY . /home/gduck
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/home/gduck/target \
    cargo build --release \
 && mkdir /home/gduck/dist \
 && cp /home/gduck/target/release/gduck-rpc /home/gduck/dist 


FROM debian:bookworm-slim

COPY --from=builder /home/gduck/dist/gduck-rpc /gduck-rpc
ENV RUST_LOG=info RUST_BACKTRACE=1

ENTRYPOINT ["/gduck-rpc"]
