#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

STAMP="$(date -u +%Y%m%d_%H%M%S)"
ARTIFACT_DIR="$ROOT_DIR/artifacts/overnight"
mkdir -p "$ARTIFACT_DIR"
PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3}"

APPLY_ARTIFACT="$ARTIFACT_DIR/congress_backtest_apply_${STAMP}.json"
VERIFY_ARTIFACT="$ARTIFACT_DIR/congress_backtest_verify_${STAMP}.json"
LOG_PATH="$ARTIFACT_DIR/congress_backtest_${STAMP}.log"

{
  echo "== Historical Congress Backtest =="
  echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "Apply artifact: $APPLY_ARTIFACT"
  echo "Verify artifact: $VERIFY_ARTIFACT"
  echo "Python: $PYTHON_BIN"
  echo
  echo "-- Apply repair pass --"
  set +e
  VAIL_ALLOW_CONGRESS_REPAIR_WRITES=1 "$PYTHON_BIN" -u ops/audit_historical_congress_backlog.py --apply --artifact "$APPLY_ARTIFACT"
  APPLY_STATUS=$?
  set -e
  echo "Apply exit status: $APPLY_STATUS"
  echo
  echo "-- Verification pass --"
  set +e
  "$PYTHON_BIN" -u ops/audit_historical_congress_backlog.py --artifact "$VERIFY_ARTIFACT"
  VERIFY_STATUS=$?
  set -e
  echo "Verify exit status: $VERIFY_STATUS"
  echo "Finished at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

  if [[ "$APPLY_STATUS" -ne 0 || "$VERIFY_STATUS" -ne 0 ]]; then
    exit 1
  fi
} 2>&1 | tee "$LOG_PATH"

echo "LOG_PATH=$LOG_PATH"
