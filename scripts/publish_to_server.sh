#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   CONFIRM_DEPLOY=YES SERVER_HOST=1.2.3.4 SERVER_USER=admin ./scripts/publish_to_server.sh
# Optional:
#   SERVER_PORT=22 WEB_ROOT=/var/www/html

DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-}"
if [[ -n "$DEPLOY_ENV_FILE" && -f "$DEPLOY_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$DEPLOY_ENV_FILE"
fi

CONFIRM_DEPLOY="${CONFIRM_DEPLOY:-}"
SERVER_HOST="${SERVER_HOST:-}"
SERVER_USER="${SERVER_USER:-admin}"
SERVER_PORT="${SERVER_PORT:-22}"
WEB_ROOT="${WEB_ROOT:-/var/www/html}"

if [[ "$CONFIRM_DEPLOY" != "YES" ]]; then
  echo "ERROR: deployment is blocked by default." >&2
  echo "Set CONFIRM_DEPLOY=YES to run remote SSH commands." >&2
  exit 1
fi

if [[ -z "$SERVER_HOST" ]]; then
  echo "ERROR: SERVER_HOST is required" >&2
  if [[ -n "$DEPLOY_ENV_FILE" ]]; then
    echo "Set SERVER_HOST in env or in ${DEPLOY_ENV_FILE}" >&2
  else
    echo "Set SERVER_HOST in env or pass DEPLOY_ENV_FILE=/path/to/server.env" >&2
  fi
  exit 1
fi

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
SSH_OPTS=( -p "$SERVER_PORT" -o StrictHostKeyChecking=accept-new )

echo "Target server: ${SSH_TARGET}:${SERVER_PORT}"
echo "Web root: ${WEB_ROOT}"

echo "[1/5] Ensuring web root exists..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo mkdir -p '${WEB_ROOT}'"

echo "[2/5] Uploading site files..."
rsync -avz --delete -e "ssh -p ${SERVER_PORT} -o StrictHostKeyChecking=accept-new" site/ "$SSH_TARGET:/tmp/ai-orchestrator-site/"
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo rsync -av --delete /tmp/ai-orchestrator-site/ '${WEB_ROOT}/'"

echo "[3/5] Ensuring Apache is installed and enabled..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "if ! command -v apache2 >/dev/null 2>&1; then sudo apt-get update && sudo apt-get install -y apache2 rsync; fi"

echo "[4/5] Validating Apache config..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo apache2ctl configtest"

echo "[5/5] Reloading Apache..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "sudo systemctl enable apache2 >/dev/null 2>&1 || true; sudo systemctl reload apache2 || sudo systemctl restart apache2"

echo "Done. Open: http://${SERVER_HOST}/"
