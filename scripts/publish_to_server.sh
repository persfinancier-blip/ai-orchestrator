#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   CONFIRM_DEPLOY=YES SERVER_HOST=155.212.162.184 SERVER_USER=sourcecraft SERVER_PORT=22 WEB_ROOT=/sourcecraft.dev/app/ai-orchestrator ./scripts/publish_to_server.sh
# Optional:
#   DEPLOY_DIR=core/orchestrator/modules/advertising/interface/dist

CONFIRM_DEPLOY="${CONFIRM_DEPLOY:-}"
SERVER_HOST="${SERVER_HOST:-}"
SERVER_USER="${SERVER_USER:-sourcecraft}"
SERVER_PORT="${SERVER_PORT:-22}"
WEB_ROOT="${WEB_ROOT:-/sourcecraft.dev/app/ai-orchestrator}"
DEPLOY_DIR="${DEPLOY_DIR:-core/orchestrator/modules/advertising/interface/dist}"

if [[ "$CONFIRM_DEPLOY" != "YES" ]]; then
  echo "ERROR: set CONFIRM_DEPLOY=YES" >&2
  exit 1
fi

if [[ -z "$SERVER_HOST" ]]; then
  echo "ERROR: SERVER_HOST is required" >&2
  exit 1
fi

if [[ ! -d "$DEPLOY_DIR" ]]; then
  echo "ERROR: deploy directory '$DEPLOY_DIR' not found. Run build first." >&2
  exit 1
fi

SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
SSH_OPTS=( -p "$SERVER_PORT" -o StrictHostKeyChecking=accept-new )

echo "Target server: ${SSH_TARGET}:${SERVER_PORT}"
echo "Web root: ${WEB_ROOT}"
echo "Deploy dir: ${DEPLOY_DIR}"

echo "[1/3] Ensuring target directory exists and is writable..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "mkdir -p '${WEB_ROOT}' && test -w '${WEB_ROOT}'"

echo "[2/3] Uploading build artifacts via tar-over-ssh..."
tar -C "$DEPLOY_DIR" -czf - . | ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "tar -xzf - -C '${WEB_ROOT}'"

echo "[3/3] Verifying deployed entrypoint..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "test -f '${WEB_ROOT}/index.html'"

echo "Done: http://${SERVER_HOST}/"
