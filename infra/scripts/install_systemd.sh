#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

sudo cp "$ROOT_DIR"/infra/systemd/wire-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable wire-chat wire-call wire-media wire-presence wire-user wire-gateway
sudo systemctl restart wire-chat wire-call wire-media wire-presence wire-user wire-gateway

sudo systemctl --no-pager --full status wire-chat wire-call wire-media wire-presence wire-user wire-gateway | sed -n '1,140p'
