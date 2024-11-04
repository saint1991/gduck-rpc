#!/bin/bash -eu

SCRIPT_DIR=$(cd $(dirname $0); pwd)
PROJECT_DIR=$(dirname $SCRIPT_DIR)
PROTO_DIR="${PROJECT_DIR}/proto"
GEN_DIR="${SCRIPT_DIR}/gduck/proto"

cleanup() {
    rm -rf ${GEN_DIR}
    mkdir -p ${GEN_DIR}
}

codegen() {
    uv run python -m grpc_tools.protoc \
        -I ${PROTO_DIR} \
        --python_out=${GEN_DIR} \
        --grpc_python_out=${GEN_DIR} \
        ${PROTO_DIR}/*.proto
}

if [ "${1:-}" = "--clean" ]; then
    cleanup
else
    cleanup
    codegen
fi
