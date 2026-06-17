#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p "$ROOT_DIR/artifacts/overnight"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${1:-$ROOT_DIR/artifacts/overnight/full_politician_asset_backfill_${TIMESTAMP}.log}"
MAX_SECONDS_PER_FILING="${MAX_SECONDS_PER_FILING:-600}"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "== Full Politician Asset Backfill =="
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Repo: $ROOT_DIR"
echo "Log: $LOG_FILE"
echo "Max seconds per filing: $MAX_SECONDS_PER_FILING"

python3 -u ops/backfill_politician_asset_names.py --all --max-seconds-per-filing "$MAX_SECONDS_PER_FILING"

echo "Finished at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "== Full Politician Asset Backfill Complete =="
