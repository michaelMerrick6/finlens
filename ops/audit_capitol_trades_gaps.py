from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import os
from collections import Counter
from datetime import timedelta

from capitol_trades_support import normalize_actor_name
from pipeline_support import emit_summary, get_supabase_client
from time_utils import congress_today


AUDIT_DAYS = int(os.environ.get("CAPITOL_TRADES_AUDIT_DAYS", "7"))
AUDIT_LIMIT = int(os.environ.get("CAPITOL_TRADES_AUDIT_LIMIT", "500"))


def load_recent_capitol_leads(supabase, *, days: int, limit: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days)).isoformat()
    rows: list[dict] = []
    offset = 0
    batch_size = min(limit, 1000)
    while len(rows) < limit:
        response = (
            supabase.table("raw_filings")
            .select("source_document_id, source_url, filed_at, payload")
            .eq("source", "capitol_trades")
            .gte("filed_at", cutoff)
            .order("filed_at", desc=True)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
    return rows[:limit]


def load_recent_official_rows(supabase, *, days: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days + 1)).isoformat()
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase.table("politician_trades")
            .select("politician_name, ticker, transaction_date, transaction_type, published_date, source_url")
            .gte("published_date", cutoff)
            .range(offset, offset + 999)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def exact_match_key(actor_name: str, ticker: str, tx_date: str, tx_type: str) -> tuple[str, str, str, str]:
    return (normalize_actor_name(actor_name), (ticker or "N/A").upper(), tx_date or "", (tx_type or "").lower())


def loose_match_key(actor_name: str, tx_date: str, tx_type: str) -> tuple[str, str, str]:
    return (normalize_actor_name(actor_name), tx_date or "", (tx_type or "").lower())


def assess_capture_health(
    leads: list[dict],
    *,
    days: int,
) -> dict:
    if not leads:
        return {
            "status": "failed",
            "reason": f"No Capitol Trades lead rows were captured in the last {days} day(s).",
            "latest_lead_date": None,
        }

    latest_lead_date = max(
        (
            str((row.get("payload") or {}).get("published_date") or row.get("filed_at") or "").strip()
            for row in leads
        ),
        default="",
    )
    stale_cutoff = (congress_today() - timedelta(days=min(days, 7))).isoformat()
    if latest_lead_date and latest_lead_date < stale_cutoff:
        return {
            "status": "failed",
            "reason": (
                f"Latest Capitol Trades lead is {latest_lead_date}, older than stale cutoff {stale_cutoff}."
            ),
            "latest_lead_date": latest_lead_date,
        }

    return {
        "status": "ok",
        "reason": "",
        "latest_lead_date": latest_lead_date or None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Capitol Trades provisional leads against official politician_trades.")
    parser.add_argument("--days", type=int, default=AUDIT_DAYS)
    parser.add_argument("--limit", type=int, default=AUDIT_LIMIT)
    args = parser.parse_args()

    supabase = get_supabase_client()
    today = congress_today().isoformat()
    leads = load_recent_capitol_leads(supabase, days=args.days, limit=args.limit)
    official_rows = load_recent_official_rows(supabase, days=args.days)
    capture_health = assess_capture_health(leads, days=args.days)

    official_exact = {
        exact_match_key(row.get("politician_name") or "", row.get("ticker") or "N/A", row.get("transaction_date") or "", row.get("transaction_type") or "")
        for row in official_rows
    }
    official_loose = {
        loose_match_key(row.get("politician_name") or "", row.get("transaction_date") or "", row.get("transaction_type") or "")
        for row in official_rows
    }
    official_by_source = Counter(row.get("source_url") or "" for row in official_rows if row.get("source_url"))

    matched_exact = 0
    matched_loose_only = 0
    matched_by_source_only = 0
    same_day_unmatched: list[dict] = []
    aged_unmatched: list[dict] = []
    missing_official_source_for_aged: list[dict] = []
    lead_counts_by_source: Counter[str] = Counter()
    lead_examples_by_source: dict[str, dict] = {}

    for row in leads:
        payload = row.get("payload") or {}
        actor_name = payload.get("politician_name") or row.get("filer_name") or ""
        ticker = (payload.get("ticker") or "N/A").upper()
        tx_date = payload.get("transaction_date") or ""
        tx_type = payload.get("transaction_type") or ""
        filed_at = payload.get("published_date") or row.get("filed_at") or ""
        official_source_url = payload.get("official_source_url") or ""

        if official_source_url:
            lead_counts_by_source[official_source_url] += 1
            lead_examples_by_source.setdefault(
                official_source_url,
                {
                    "official_source_url": official_source_url,
                    "politician_name": actor_name,
                    "published_date": filed_at,
                },
            )

        if exact_match_key(actor_name, ticker, tx_date, tx_type) in official_exact:
            matched_exact += 1
            continue

        if official_source_url and official_by_source.get(official_source_url, 0) > 0:
            matched_by_source_only += 1
            continue

        if loose_match_key(actor_name, tx_date, tx_type) in official_loose:
            matched_loose_only += 1
            continue

        sample = {
            "source_document_id": row.get("source_document_id"),
            "politician_name": actor_name,
            "ticker": ticker,
            "transaction_date": tx_date,
            "transaction_type": tx_type,
            "published_date": filed_at,
            "official_source_url": official_source_url or None,
            "detail_url": row.get("source_url"),
        }
        if filed_at and filed_at < today:
            aged_unmatched.append(sample)
            if not official_source_url:
                missing_official_source_for_aged.append(sample)
        else:
            same_day_unmatched.append(sample)

    filing_row_gaps: list[dict] = []
    for official_source_url, lead_count in lead_counts_by_source.items():
        official_count = int(official_by_source.get(official_source_url, 0) or 0)
        if lead_count > official_count:
            example = lead_examples_by_source.get(official_source_url, {})
            if example.get("published_date") and example["published_date"] < today:
                filing_row_gaps.append(
                    {
                        "official_source_url": official_source_url,
                        "politician_name": example.get("politician_name"),
                        "published_date": example.get("published_date"),
                        "lead_rows": lead_count,
                        "official_rows": official_count,
                    }
                )

    capture_failures = 0 if capture_health["status"] == "ok" else 1
    parse_failures = len(aged_unmatched) + len(filing_row_gaps) + capture_failures
    emit_summary(
        {
            "capture_status": capture_health["status"],
            "capture_reason": capture_health["reason"],
            "latest_lead_date": capture_health["latest_lead_date"],
            "records_seen": len(leads),
            "matched_exact": matched_exact,
            "matched_by_source_only": matched_by_source_only,
            "matched_loose_only": matched_loose_only,
            "same_day_unmatched": same_day_unmatched[:20],
            "aged_unmatched": aged_unmatched[:20],
            "filing_row_gaps": filing_row_gaps[:20],
            "missing_official_source_for_aged": missing_official_source_for_aged[:20],
            "parse_failures": parse_failures,
        }
    )

    if parse_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
