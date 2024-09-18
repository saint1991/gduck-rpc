#!/bin/bash -eu

SCRIPT_DIR=$(cd $(dirname $0); pwd)
PARENT_DIR=$(dirname $SCRIPT_DIR)

docker build -f ${SCRIPT_DIR}/Dockerfile --output=type=local,dest=$SCRIPT_DIR/gen $PARENT_DIR
