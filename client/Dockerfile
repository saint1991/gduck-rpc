# syntax=docker/dockerfile:1

FROM python:3.13-slim-bookworm AS grpc

RUN apt-get update -y \
 && apt-get install -y curl

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH=/root/.cargo/bin:${PATH}

RUN --mount=type=bind,source=client/pyproject.toml,target=/pyproject.toml \
    --mount=type=bind,source=client/uv.lock,target=/uv.lock \
    uv pip install --system -r /pyproject.toml

RUN --mount=type=bind,source=client/pyproject.toml,target=/pyproject.toml \
    --mount=type=bind,source=proto,target=/proto \
    mkdir -p /gen  \
 && python -m grpc_tools.protoc -I/proto --python_out=/gen --grpc_python_out=/gen /proto/*.proto


FROM scratch AS grpc-out

COPY --from=grpc /gen /