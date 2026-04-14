#!/usr/bin/env bash

set -euo pipefail

REDIS_ENV_FILE="infra/env/api-gateway.env"
DOCKER_COMPOSE_FILE="infra/docker-compose.yml"

trim_quotes() {
  local value="$1"
  value="${value%\'}"
  value="${value#\'}"
  value="${value%\"}"
  value="${value#\"}"
  printf '%s' "$value"
}

redis_addr_from_env() {
  local line raw
  line="$(grep -E "^REDIS_CLUSTER_ADDRS=" "$REDIS_ENV_FILE" | tail -n 1 || true)"
  raw="${line#REDIS_CLUSTER_ADDRS=}"
  raw="$(trim_quotes "$raw")"
  printf '%s' "${raw%%,*}"
}

wait_for_redis() {
  local host="$1"
  local port="$2"
  local attempt

  for attempt in {1..20}; do
    if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  return 1
}

ensure_redis() {
  local redis_addr redis_host redis_port compose_cmd
  redis_addr="$(redis_addr_from_env)"
  redis_host="${redis_addr%:*}"
  redis_port="${redis_addr##*:}"

  if wait_for_redis "$redis_host" "$redis_port"; then
    echo "Redis is available at $redis_addr"
    return 0
  fi

  if docker compose version >/dev/null 2>&1; then
    compose_cmd=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    compose_cmd=(docker-compose)
  else
    echo "Redis is not reachable at $redis_addr and no Docker Compose command is available." >&2
    echo "Start Redis manually or install Docker Compose, then rerun ./start_all.sh." >&2
    exit 1
  fi

  echo "Redis is not reachable at $redis_addr. Starting local Redis container..."
  "${compose_cmd[@]}" -f "$DOCKER_COMPOSE_FILE" up -d redis >/dev/null

  if ! wait_for_redis "$redis_host" "$redis_port"; then
    echo "Redis did not become ready at $redis_addr after starting the container." >&2
    exit 1
  fi

  echo "Redis started at $redis_addr"
}

# Kill previous instances
pkill -f api-gateway || true
pkill -f chat-service || true
pkill -f user-service || true
pkill -f presence-service || true
pkill -f media-service || true
pkill -f call-service || true

# Source global env if it exists (for shared vars like APP_ENV if needed)
[ -f .env ] && set -a && source .env && set +a

mkdir -p bin

ensure_redis

echo "Building services..."
go build -o bin/user-service services/user-service/cmd/*.go
go build -o bin/presence-service services/presence-service/cmd/*.go
go build -o bin/media-service services/media-service/cmd/*.go
go build -o bin/chat-service services/chat-service/cmd/main.go services/chat-service/cmd/server.go
go build -o bin/api-gateway services/api-gateway/cmd/*.go

echo "Starting backend services..."

# Start each service with its specific environment
(set -a; source infra/env/user-service.env; ./bin/user-service > user-service.log 2>&1 &)
echo "User Service started"

(set -a; source infra/env/chat-service.env; ./bin/chat-service > chat-service.log 2>&1 &)
echo "Chat Service started"

(set -a; source infra/env/presence-service.env; ./bin/presence-service > presence-service.log 2>&1 &)
echo "Presence Service started"

(set -a; source infra/env/media-service.env; ./bin/media-service > media-service.log 2>&1 &)
echo "Media Service started"

(set -a; source infra/env/api-gateway.env; ./bin/api-gateway > api-gateway.log 2>&1 &)
echo "API Gateway started (Port: 8082)"

echo "All services started. Check *.log for details."
