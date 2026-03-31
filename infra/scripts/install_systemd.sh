#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_USER="${SUDO_USER:-$USER}"
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"

write_unit() {
  local name="$1"
  local env_file="$2"
  local binary="$3"
  local after="$4"

  cat >"/tmp/${name}.service" <<EOF
[Unit]
Description=Wire ${name#wire-} service
After=${after}
Wants=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${ROOT_DIR}
EnvironmentFile=${ROOT_DIR}/infra/env/${env_file}
ExecStart=${ROOT_DIR}/bin/${binary}
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

  sudo install -m 0644 "/tmp/${name}.service" "/etc/systemd/system/${name}.service"
}

write_unit "wire-chat" "chat-service.env" "chat-service" "network.target"
write_unit "wire-call" "call-service.env" "call-service" "network.target"
write_unit "wire-media" "media-service.env" "media-service" "network.target"
write_unit "wire-presence" "presence-service.env" "presence-service" "network.target"
write_unit "wire-user" "user-service.env" "user-service" "network.target"
write_unit "wire-gateway" "api-gateway.env" "api-gateway" "network.target wire-chat.service wire-call.service wire-media.service wire-presence.service wire-user.service"

sudo systemctl daemon-reload
sudo systemctl enable wire-chat wire-call wire-media wire-presence wire-user wire-gateway
sudo systemctl restart wire-chat wire-call wire-media wire-presence wire-user wire-gateway

sudo systemctl --no-pager --full status wire-chat wire-call wire-media wire-presence wire-user wire-gateway | sed -n '1,140p'
