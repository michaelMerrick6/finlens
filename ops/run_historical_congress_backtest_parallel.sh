#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PHASE="${1:-apply}"
if [[ "$PHASE" != "apply" && "$PHASE" != "verify" ]]; then
  echo "Usage: $0 [apply|verify]" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%d_%H%M%S)"
ARTIFACT_DIR="$ROOT_DIR/artifacts/overnight"
mkdir -p "$ARTIFACT_DIR"

PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3}"
MANIFEST_PATH="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_manifest.txt"

launch_shard() {
  local shard_name="$1"
  local screen_name="congress_${PHASE}_${shard_name}_${STAMP}"
  local log_path="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}.log"
  local artifact_path="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}.json"

  local command=""
  case "$shard_name" in
    house_2026_2024)
      command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber house --house-start-year 2024 --house-end-year 2026 --skip-duplicate-scan --artifact '$artifact_path'"
      ;;
    house_2023_2020)
      command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber house --house-start-year 2020 --house-end-year 2023 --skip-duplicate-scan --artifact '$artifact_path'"
      ;;
    house_2019_2016)
      command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber house --house-start-year 2016 --house-end-year 2019 --skip-duplicate-scan --artifact '$artifact_path'"
      ;;
    house_2015_2012)
      command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber house --house-start-year 2012 --house-end-year 2015 --skip-duplicate-scan --artifact '$artifact_path'"
      ;;
    senate_all)
      command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber senate --senate-start-date '01/01/2012 00:00:00' --skip-duplicate-scan --artifact '$artifact_path'"
      ;;
    *)
      echo "Unknown shard: $shard_name" >&2
      exit 1
      ;;
  esac

  if [[ "$PHASE" == "apply" ]]; then
    command="env VAIL_ALLOW_CONGRESS_REPAIR_WRITES=1 $command --apply"
  fi

  screen -dmS "$screen_name" bash -lc "cd '$ROOT_DIR' && echo '== $PHASE $shard_name ==' > '$log_path' && echo 'Started at: '\$(date -u '+%Y-%m-%d %H:%M:%S UTC') >> '$log_path' && caffeinate -dimsu bash -lc \"$command\" >> '$log_path' 2>&1"

  printf '%s|%s|%s\n' "$screen_name" "$log_path" "$artifact_path" >> "$MANIFEST_PATH"
}

: > "$MANIFEST_PATH"
for shard in house_2026_2024 house_2023_2020 house_2019_2016 house_2015_2012 senate_all; do
  launch_shard "$shard"
done

echo "PHASE=$PHASE"
echo "MANIFEST=$MANIFEST_PATH"
