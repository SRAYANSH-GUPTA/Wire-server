#!/usr/bin/env bash
set -a
source .env
set +a

# Kill previous instances
pkill -f "go run" || true
pkill -f api-gateway || true
pkill -f chat-service || true
pkill -f user-service || true
pkill -f presence-service || true
pkill -f media-service || true
pkill -f call-service || true

mkdir -p bin

echo "Building services..."
go build -o bin/user-service services/user-service/cmd/*.go
go build -o bin/presence-service services/presence-service/cmd/*.go
go build -o bin/media-service services/media-service/cmd/*.go
# Exclude integration tests for chat-service
go build -o bin/chat-service services/chat-service/cmd/main.go services/chat-service/cmd/server.go
go build -o bin/api-gateway services/api-gateway/cmd/*.go

echo "Starting backend services..."

# Assign unique health ports via PORT env var overrides to avoid conflicts
PORT=8110 ./bin/user-service > user-service.log 2>&1 &
echo "User Service started (Health: 8110, gRPC: 8611)"

PORT=8115 ./bin/chat-service > chat-service.log 2>&1 &
echo "Chat Service started (Health: 8115, gRPC: 8111)"

PORT=8114 ./bin/presence-service > presence-service.log 2>&1 &
echo "Presence Service started (Health: 8114, gRPC: 8411)"

PORT=8113 ./bin/media-service > media-service.log 2>&1 &
echo "Media Service started (Health: 8113, gRPC: 8311)"

./bin/api-gateway > api-gateway.log 2>&1 &
echo "API Gateway started (Port: 8082)"

echo "All services started. Check *.log for details."
