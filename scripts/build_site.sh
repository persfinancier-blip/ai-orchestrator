#!/usr/bin/env bash
set -euo pipefail

APP_DIR="core/orchestrator/modules/advertising/interface"

if [[ ! -f "$APP_DIR/package.json" ]]; then
  echo "ERROR: missing $APP_DIR/package.json" >&2
  exit 1
fi

pushd "$APP_DIR" >/dev/null
npm install
npm run build
popd >/dev/null
