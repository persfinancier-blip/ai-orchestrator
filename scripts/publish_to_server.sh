#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   SERVER_HOST=1.2.3.4 SERVER_USER=ubuntu ./scripts/publish_to_server.sh
# Optional:
#   SERVER_PORT=22 SERVER_NAME=example.com

SERVER_HOST="${SERVER_HOST:-}"
SERVER_USER="${SERVER_USER:-ubuntu}"
SERVER_PORT="${SERVER_PORT:-22}"
SERVER_NAME="${SERVER_NAME:-_}"

if [[ -z "$SERVER_HOST" ]]; then
  echo "ERROR: SERVER_HOST is required" >&2
  exit 1
fi

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
SSH_OPTS=( -p "$SERVER_PORT" -o StrictHostKeyChecking=accept-new )

echo "[1/5] Creating target directories..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo mkdir -p /var/www/ai-orchestrator/site /etc/nginx/sites-available /etc/nginx/sites-enabled"

echo "[2/5] Uploading static site..."
rsync -avz --delete -e "ssh -p ${SERVER_PORT} -o StrictHostKeyChecking=accept-new" site/ "$SSH_TARGET:/tmp/ai-orchestrator-site/"
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo rsync -av --delete /tmp/ai-orchestrator-site/ /var/www/ai-orchestrator/site/"

echo "[3/5] Preparing nginx config..."
TMP_CONF="$(mktemp)"
sed "s/server_name _;/server_name ${SERVER_NAME};/" deploy/nginx/ai-orchestrator.conf > "$TMP_CONF"
scp -P "$SERVER_PORT" -o StrictHostKeyChecking=accept-new "$TMP_CONF" "$SSH_TARGET:/tmp/ai-orchestrator.conf"
rm -f "$TMP_CONF"

ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo mv /tmp/ai-orchestrator.conf /etc/nginx/sites-available/ai-orchestrator && sudo ln -sfn /etc/nginx/sites-available/ai-orchestrator /etc/nginx/sites-enabled/ai-orchestrator"

echo "[4/5] Installing nginx if missing..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "if ! command -v nginx >/dev/null 2>&1; then sudo apt-get update && sudo apt-get install -y nginx rsync; fi"

echo "[5/5] Validating and reloading nginx..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo nginx -t && sudo systemctl reload nginx"

echo "Done. Open: http://${SERVER_HOST}/"
if [[ "$SERVER_NAME" != "_" ]]; then
  echo "If DNS is configured: http://${SERVER_NAME}/"
fi
