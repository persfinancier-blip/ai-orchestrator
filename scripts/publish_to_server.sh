#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   CONFIRM_DEPLOY=YES SERVER_HOST=1.2.3.4 SERVER_USER=admin WEB_ROOT=/sourcecraft.dev/app/ai-orchestrator ./scripts/publish_to_server.sh
# Optional:
#   SERVER_PORT=22

DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-}"
if [[ -n "$DEPLOY_ENV_FILE" && -f "$DEPLOY_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$DEPLOY_ENV_FILE"
fi

CONFIRM_DEPLOY="${CONFIRM_DEPLOY:-}"
SERVER_HOST="${SERVER_HOST:-}"
SERVER_USER="${SERVER_USER:-admin}"
SERVER_PORT="${SERVER_PORT:-22}"
WEB_ROOT="${WEB_ROOT:-/sourcecraft.dev/app/ai-orchestrator}"

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

echo "[1/3] Ensuring web root exists and writable for current user..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "mkdir -p '${WEB_ROOT}'"
if ! ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "test -w '${WEB_ROOT}'"; then
  echo "ERROR: '${WEB_ROOT}' is not writable by ${SERVER_USER} (no sudo mode)." >&2
  echo "Check ownership/permissions on server:" >&2
  echo "  ls -ld '${WEB_ROOT}' && id" >&2
  exit 1
fi

echo "[2/3] Uploading site files (without sudo)..."
rsync -avz --delete -e "ssh -p ${SERVER_PORT} -o StrictHostKeyChecking=accept-new" site/ "$SSH_TARGET:${WEB_ROOT}/"

echo "[3/3] Verifying index file on server..."
ssh "${SSH_OPTS[@]}" "$SSH_TARGET" "test -f '${WEB_ROOT}/index.html'"

echo "Done. Open: http://${SERVER_HOST}/"
