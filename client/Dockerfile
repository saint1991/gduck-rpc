# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm AS grpc

RUN apt-get update -y \
 && apt-get install -y curl

ENV POETRY_HOME=/etc/poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH=${POETRY_HOME}/bin:${PATH}

RUN --mount=type=bind,source=client/pyproject.toml,target=/pyproject.toml \
    --mount=type=bind,source=client/poetry.lock,target=/poetry.lock \
    poetry install --only=main

RUN --mount=type=bind,source=client/pyproject.toml,target=/pyproject.toml \
    --mount=type=bind,source=proto,target=/proto \
    mkdir -p /gen  \
 && poetry run python -m grpc_tools.protoc -I/proto --python_out=/gen --grpc_python_out=/gen /proto/*.proto


FROM scratch AS grpc-out

COPY --from=grpc /gen /