from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_support import emit_summary, get_supabase_client, utc_now
from signal_baseline_support import enrich_events_with_baseline_snapshots, stored_baseline_snapshot


LOOKBACK_DAYS = int(os.environ.get("SIGNAL_BASELINE_BACKFILL_LOOKBACK_DAYS", "365"))
PAGE_SIZE = int(os.environ.get("SIGNAL_BASELINE_BACKFILL_PAGE_SIZE", "250"))
UPSERT_CHUNK_SIZE = int(os.environ.get("SIGNAL_BASELINE_BACKFILL_UPSERT_CHUNK_SIZE", "200"))
SUPPORTED_SIGNAL_TYPES = [
    "politician_trade",
    "insider_trade",
    "fund_position_change",
    "politician_cluster",
    "cross_source_accumulation",
    "politician_trade_grouped",
    "insider_trade_grouped",
    "politician_filing_summary",
    "insider_filing_summary",
]
SOURCE_VALUES = ["congress", "insider", "hedge_fund", "cross_source"]


def fetch_candidate_events(supabase) -> list[dict]:
    since_date = (utc_now() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    rows: list[dict] = []
    supported_signal_types = set(SUPPORTED_SIGNAL_TYPES)

    for source in SOURCE_VALUES:
        start = 0
        while True:
            response = (
                supabase.table("signal_events")
                .select("*")
                .eq("source", source)
                .gte("created_at", since_date)
                .order("created_at", desc=True)
                .range(start, start + PAGE_SIZE - 1)
                .execute()
            )
            batch = response.data or []
            if not batch:
                break
            rows.extend(row for row in batch if str(row.get("signal_type") or "").strip() in supported_signal_types)
            if len(batch) < PAGE_SIZE:
                break
            start += PAGE_SIZE
    return rows


def chunked_upsert(supabase, rows: list[dict]) -> None:
    if not rows:
        return
    for start in range(0, len(rows), UPSERT_CHUNK_SIZE):
        chunk = rows[start : start + UPSERT_CHUNK_SIZE]
        supabase.table("signal_events").upsert(chunk, on_conflict="source,source_document_id").execute()


def main() -> None:
    print("Backfilling signal baseline snapshots...")
    supabase = get_supabase_client()
    rows = fetch_candidate_events(supabase)

    missing_baseline_rows = []
    for row in rows:
        baseline_price, baseline_reference_date, _, _ = stored_baseline_snapshot(row)
        if baseline_price and baseline_reference_date:
            continue
        missing_baseline_rows.append(row)

    updated_rows = enrich_events_with_baseline_snapshots(missing_baseline_rows)
    rows_to_upsert = []
    for row in updated_rows:
        baseline_price, baseline_reference_date, _, _ = stored_baseline_snapshot(row)
        if baseline_price and baseline_reference_date:
            rows_to_upsert.append(row)

    chunked_upsert(supabase, rows_to_upsert)

    summary = {
        "signal_events_seen": len(rows),
        "signal_events_missing_baseline": len(missing_baseline_rows),
        "signal_events_baseline_upserted": len(rows_to_upsert),
        "lookback_days": LOOKBACK_DAYS,
    }
    emit_summary(summary)
    print(
        "Signal baseline backfill complete: "
        f"{summary['signal_events_baseline_upserted']} updated from "
        f"{summary['signal_events_missing_baseline']} missing rows."
    )


if __name__ == "__main__":
    main()
