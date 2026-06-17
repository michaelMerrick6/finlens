from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

"""
Apply audit fix: dedup insider_trades in streaming batches.
Processes 500 rows at a time, deletes dupes as it finds them.
Run: source .env.local && python3 ops/apply_audit_fixes.py
"""
import os
import sys
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

from supabase import create_client

url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not url or not key:
    print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    sys.exit(1)

sb = create_client(url, key)
BATCH = 500


def stream_dedup_insider_trades():
    """Stream through insider_trades in small batches, deleting dupes immediately."""
    print("Deduplicating insider_trades (streaming mode)...")
    seen: dict[tuple, str] = {}
    total_scanned = 0
    total_deleted = 0
    delete_buffer: list[str] = []
    last_created_at = ""

    while True:
        try:
            query = sb.table("insider_trades") \
                .select("id,source_url,ticker,transaction_date,filer_name,created_at") \
                .not_.is_("source_url", "null") \
                .order("created_at", desc=False) \
                .limit(BATCH)

            if last_created_at:
                query = query.gt("created_at", last_created_at)

            res = query.execute()
        except Exception as exc:
            print(f"  API error at offset ~{total_scanned}, waiting 5s: {exc}")
            time.sleep(5)
            continue

        rows = res.data or []
        if not rows:
            break

        for row in rows:
            dedup_key = (
                row.get("source_url") or "",
                row.get("ticker") or "",
                row.get("transaction_date") or "",
                row.get("filer_name") or "",
            )
            if dedup_key in seen:
                delete_buffer.append(row["id"])
            else:
                seen[dedup_key] = row["id"]

            last_created_at = row["created_at"]

        total_scanned += len(rows)

        # Flush deletes when buffer is large enough
        if len(delete_buffer) >= 200:
            flush_count = flush_deletes(delete_buffer)
            total_deleted += flush_count
            delete_buffer.clear()

        if total_scanned % 5000 == 0:
            print(f"  Scanned {total_scanned:,} rows, deleted {total_deleted:,} dupes, tracking {len(seen):,} unique keys")

        if len(rows) < BATCH:
            break

    # Final flush
    if delete_buffer:
        flush_count = flush_deletes(delete_buffer)
        total_deleted += flush_count

    print(f"\n  ✅ Done! Scanned {total_scanned:,} rows, deleted {total_deleted:,} duplicates")
    print(f"     Remaining unique rows: ~{total_scanned - total_deleted:,}")
    return total_deleted


def flush_deletes(ids: list[str]) -> int:
    """Delete a batch of IDs, retrying on failure."""
    deleted = 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        for attempt in range(3):
            try:
                sb.table("insider_trades").delete().in_("id", chunk).execute()
                deleted += len(chunk)
                break
            except Exception as exc:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f"  ⚠️  Failed to delete {len(chunk)} rows after 3 attempts: {exc}")
    return deleted


def main():
    print("=" * 60)
    print("Audit Fix: Insider Trades Deduplication")
    print("=" * 60)
    print()

    deleted = stream_dedup_insider_trades()

    print()
    if deleted > 0:
        print(f"Cleaned up {deleted:,} duplicate rows.")
    else:
        print("No duplicates found!")

    print()
    print("=" * 60)
    print("Next step: create the unique index in Supabase SQL editor:")
    print()
    print("  CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_trades_source_dedup")
    print("  ON public.insider_trades(source_url, ticker, transaction_date, filer_name)")
    print("  WHERE source_url IS NOT NULL;")
    print("=" * 60)


if __name__ == "__main__":
    main()
