#!/usr/bin/env bash
# Generate the Python gRPC stubs from proto/analytics.proto into src/.
#
#     bash scripts/build_proto.sh
#
# Stubs from a newer grpcio-tools fail to import under a cluster-provided protobuf 3.19, so
# generate them on the machine that runs them. Without grpcio-tools, the stubs
# shipped in src/ (grpcio-tools 1.48.2) are kept if they import.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-python3}"


if ! "$PY" -c 'import grpc' >/dev/null 2>&1; then
    echo "grpc is not importable with $PY -- load the modules first:" >&2
    echo "    (on the cluster use the system python: PYTHON=/usr/bin/python3)" >&2
    exit 1
fi
if ! "$PY" -c 'import grpc_tools.protoc' >/dev/null 2>&1; then
    if "$PY" -c "import sys; sys.path.insert(0, '$ROOT/src'); import analytics_pb2, analytics_pb2_grpc" >/dev/null 2>&1; then
        echo "grpcio-tools not available: using the stubs shipped in src/ (they import fine)"
        exit 0
    fi
    echo "grpcio-tools not available and the shipped stubs do not import" >&2
    exit 1
fi

"$PY" -m grpc_tools.protoc -I "$ROOT/proto" \
    --python_out="$ROOT/src" --grpc_python_out="$ROOT/src" \
    "$ROOT/proto/analytics.proto"

echo "generated:"
ls -1 "$ROOT"/src/analytics_pb2*.py | sed 's/^/  /'
