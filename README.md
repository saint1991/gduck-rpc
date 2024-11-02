# gduck-rpc
[![Build](https://github.com/saint1991/gduck-rpc/actions/workflows/docker.yml/badge.svg)](https://github.com/saint1991/gduck-rpc/actions/workflows/docker.yml)

A server implementation to utilize [DuckDB](https://github.com/duckdb/duckdb) features via gRPC API.

## Build server

This project uses bundled DuckDB. 
To build it, please follow its [instruction](https://duckdb.org/docs/dev/building/build_instructions.html).

Then building gduck server by cargo as follows:

```bash
cargo build
```

## Usage

Starting the server by executing a binary or by cargo run.
This server uses [env_logger](https://docs.rs/env_logger/latest/env_logger/) for logging.
Setting `RUST_LOG` is required to see server messages.
In default, it listens 0.0.0.0:50051 but it can be configured by options.

```bash
$ RUST_LOG=INFO cargo run
[2024-11-02T12:56:13Z INFO  gduck] Start listening on 0.0.0.0:50051
```

## Design

gduck server has a single gRPC bidirectional streaming API `Transaction` as defined in [service.proto](./proto/service.proto).

First message must be a `Connect` message then DuckDB connection is established according to it and then you can send any number of Query to query DuckDB.
Connection alives until gRPC connection is closed.

Python clinet implementation is available under [client](./client/)
