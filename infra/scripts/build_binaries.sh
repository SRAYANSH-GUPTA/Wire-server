#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
mkdir -p "$BIN_DIR"

export CGO_ENABLED=0
export GOCACHE="${GOCACHE:-/tmp/go-build}"
export GOPATH="${GOPATH:-/tmp/go}"

echo "Building binaries into $BIN_DIR"
go build -o "$BIN_DIR/api-gateway" "$ROOT_DIR/services/api-gateway/cmd"
go build -o "$BIN_DIR/chat-service" "$ROOT_DIR/services/chat-service/cmd"
go build -o "$BIN_DIR/call-service" "$ROOT_DIR/services/call-service/cmd"
go build -o "$BIN_DIR/media-service" "$ROOT_DIR/services/media-service/cmd"
go build -o "$BIN_DIR/presence-service" "$ROOT_DIR/services/presence-service/cmd"
go build -o "$BIN_DIR/user-service" "$ROOT_DIR/services/user-service/cmd"

echo "Build complete"
