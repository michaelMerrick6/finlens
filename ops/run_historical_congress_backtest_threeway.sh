#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PHASE="${1:-apply}"
if [[ "$PHASE" != "apply" && "$PHASE" != "verify" ]]; then
  echo "Usage: $0 [apply|verify]" >&2
  exit 1
fi
shift || true

STAMP="$(date -u +%Y%m%d_%H%M%S)"
ARTIFACT_DIR="$ROOT_DIR/artifacts/overnight"
mkdir -p "$ARTIFACT_DIR"

PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3}"
MANIFEST_PATH="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_manifest.txt"

build_command() {
  local chamber="$1"
  local artifact_path="$2"
  shift 2

  local command="$PYTHON_BIN -u ops/audit_historical_congress_backlog.py --chamber $chamber --skip-duplicate-scan --artifact '$artifact_path' $*"
  if [[ "$PHASE" == "apply" ]]; then
    command="env VAIL_ALLOW_CONGRESS_REPAIR_WRITES=1 $command --apply"
  fi
  printf '%s' "$command"
}

launch_shard() {
  local shard_name="$1"
  local screen_name="congress_${PHASE}_${shard_name}_${STAMP}"
  local log_path="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}.log"
  local primary_artifact_path="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}.json"
  local shard_script=""

  case "$shard_name" in
    recent_house)
      local recent_artifact="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}_2026_2024.json"
      local modern_artifact="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}_2023_2020.json"
      local recent_cmd
      local modern_cmd
      recent_cmd="$(build_command house "$recent_artifact" --house-start-year 2024 --house-end-year 2026)"
      modern_cmd="$(build_command house "$modern_artifact" --house-start-year 2020 --house-end-year 2023)"
      shard_script="set -euo pipefail
echo 'ARTIFACT recent_house_2026_2024=$recent_artifact' >> '$log_path'
$recent_cmd >> '$log_path' 2>&1
echo 'ARTIFACT recent_house_2023_2020=$modern_artifact' >> '$log_path'
$modern_cmd >> '$log_path' 2>&1"
      ;;
    middle_house)
      local middle_cmd
      middle_cmd="$(build_command house "$primary_artifact_path" --house-start-year 2016 --house-end-year 2019)"
      shard_script="set -euo pipefail
$middle_cmd >> '$log_path' 2>&1"
      ;;
    old_house_and_senate)
      local old_artifact="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}_2015_2012.json"
      local senate_artifact="$ARTIFACT_DIR/congress_backtest_${PHASE}_${STAMP}_${shard_name}_senate_all.json"
      local old_cmd
      local senate_cmd
      old_cmd="$(build_command house "$old_artifact" --house-start-year 2012 --house-end-year 2015)"
      senate_cmd="$(build_command senate "$senate_artifact" --senate-start-date '01/01/2012 00:00:00')"
      shard_script="set -euo pipefail
echo 'ARTIFACT old_house_2015_2012=$old_artifact' >> '$log_path'
$old_cmd >> '$log_path' 2>&1
echo 'ARTIFACT senate_all=$senate_artifact' >> '$log_path'
$senate_cmd >> '$log_path' 2>&1"
      ;;
    *)
      echo "Unknown shard: $shard_name" >&2
      exit 1
      ;;
  esac

  screen -dmS "$screen_name" bash -lc "cd '$ROOT_DIR' && echo '== $PHASE $shard_name ==' > '$log_path' && echo 'Started at: '\$(date -u '+%Y-%m-%d %H:%M:%S UTC') >> '$log_path' && caffeinate -dimsu bash -lc \"$shard_script\""
  printf '%s|%s|%s\n' "$screen_name" "$log_path" "$primary_artifact_path" >> "$MANIFEST_PATH"
}

: > "$MANIFEST_PATH"
if [[ "$#" -gt 0 ]]; then
  SHARDS=("$@")
else
  SHARDS=(recent_house middle_house old_house_and_senate)
fi

for shard in "${SHARDS[@]}"; do
  launch_shard "$shard"
done

echo "PHASE=$PHASE"
echo "MANIFEST=$MANIFEST_PATH"
