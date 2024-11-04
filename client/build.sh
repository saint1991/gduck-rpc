#!/bin/bash -eu

./codegen.sh

uv build --wheel
