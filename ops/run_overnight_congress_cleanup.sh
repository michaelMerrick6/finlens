#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p "$ROOT_DIR/artifacts/overnight"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${1:-$ROOT_DIR/artifacts/overnight/congress_cleanup_${TIMESTAMP}.log}"

exec > >(tee -a "$LOG_FILE") 2>&1

count_unresolved_asset_rows() {
  python3 - <<'PY'
import sys
sys.path.append('scripts')
from pipeline_support import get_supabase_client

supabase = get_supabase_client()
page_size = 500
offset = 0
total = 0

while True:
    response = (
        supabase.table('raw_filings')
        .select('payload,ticker')
        .eq('source', 'congress')
        .in_('ticker', ['N/A', 'US-TREAS'])
        .order('filed_at', desc=True)
        .range(offset, offset + page_size - 1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        break
    total += sum(1 for row in rows if not str((row.get('payload') or {}).get('asset_name') or '').strip())
    if len(rows) < page_size:
        break
    offset += page_size

print(total)
PY
}

echo "== Overnight Congress Cleanup =="
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Repo: $ROOT_DIR"
echo "Log: $LOG_FILE"

for batch in 1 2 3 4 5 6 7 8; do
  before="$(count_unresolved_asset_rows)"
  echo "-- Asset backfill batch $batch (remaining before: $before)"
  if [ "$before" -le 0 ]; then
    echo "No unresolved Congress asset-name rows remain."
    break
  fi

  python3 -u ops/backfill_politician_asset_names.py --limit 250 --max-seconds-per-filing 120 || true

  after="$(count_unresolved_asset_rows)"
  echo "-- Asset backfill batch $batch complete (remaining after: $after)"
  if [ "$after" -ge "$before" ]; then
    echo "No progress in asset-name backfill; stopping batches."
    break
  fi
done

echo "-- Running 30-day House replay"
python3 -u scripts/sync_recent_house_filings.py --days 30 --limit 300

echo "-- Running 30-day Senate replay"
python3 -u scripts/sync_recent_senate_filings.py --days 30 --limit 150

echo "-- Running 30-day official Congress audit"
python3 -u ops/audit_recent_congress_coverage.py --house-days 30 --house-limit 300 --senate-days 30 --senate-limit 150

echo "Finished at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "== Overnight Congress Cleanup Complete =="
