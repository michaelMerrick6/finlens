from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
OPS_DIR = ROOT_DIR / "ops"
for path in (SCRIPTS_DIR, OPS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import argparse
import requests
import os
from datetime import datetime, timedelta

from congress_member_lookup import load_congress_members
from ingest_house_official import load_company_lookup
from ingest_senate_official import load_valid_tickers
from pipeline_support import emit_summary, get_supabase_client
from repair_senate_filings import create_senate_session, load_members_lookup, parse_senate_filing
from sync_recent_house_filings import load_recent_house_filings, parse_house_doc
from sync_recent_senate_filings import load_recent_senate_filings
from time_utils import congress_today
from parser_write_policy import read_only_parsing
from audit_execution import run_document, save_progress
from audit_trade_comparison import compare_transactions


HOUSE_AUDIT_DAYS = int(os.environ.get("HOUSE_AUDIT_DAYS", "14"))
HOUSE_AUDIT_LIMIT = int(os.environ.get("HOUSE_AUDIT_LIMIT", "100"))
SENATE_AUDIT_DAYS = int(os.environ.get("SENATE_AUDIT_DAYS", "30"))
SENATE_AUDIT_LIMIT = int(os.environ.get("SENATE_AUDIT_LIMIT", "100"))


def summarize_doc_prefix(doc_id: str) -> str:
    if doc_id.startswith("house-"):
        return "-".join(doc_id.split("-")[:3])
    if doc_id.startswith("senate-") and doc_id.count("-") >= 5:
        return "-".join(doc_id.split("-")[:-1])
    return doc_id


def fetch_doc_rows(supabase, prefix: str) -> tuple[int, list[dict]]:
    rows = []
    while True:
        batch = (supabase.table("politician_trades")
                 .select("doc_id,member_id,published_date,ticker,transaction_date,transaction_type,amount_range")
                 .like("doc_id", f"{prefix}-%")
                 .order("id").range(len(rows), len(rows) + 499).execute().data or [])
        rows.extend(batch)
        if len(batch) < 500:
            return len(rows), rows


def summarize_fallback_rows(prefix: str, rows: list[dict]) -> dict | None:
    fallback_rows = [row for row in rows if "-capitol-" in str(row.get("doc_id") or "")]
    if not fallback_rows:
        return None
    return {
        "doc_id": prefix,
        "fallback_rows": len(fallback_rows),
        "fallback_doc_ids": sorted({str(row.get("doc_id") or "") for row in fallback_rows})[:12],
    }


@read_only_parsing
def audit_house(supabase, *, days: int, limit: int, document_timeout: float = 120, checkpoint=None) -> dict:
    members_db = load_congress_members(supabase)
    company_lookup = load_company_lookup()
    filings = load_recent_house_filings(days=days, limit=limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "no_trade_filings": 0,
        "source_parse_failures": [],
        "fallback_doc_rows": [],
        "row_count_mismatches": [],
        "transaction_mismatches": [],
        "published_date_mismatches": [],
        "unexpected_rows_for_no_trade_filings": [],
        "unknown_member_docs": [],
    }

    for filing in filings:
        prefix = f"house-{filing['year']}-{filing['doc_id']}"
        def check():
            status, trades = parse_house_doc(filing, members_db, company_lookup)
            expected_published_date = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").strftime("%Y-%m-%d")
            db_count, db_rows = fetch_doc_rows(get_supabase_client(), prefix)
            mismatch = compare_transactions(trades, db_rows)
            if mismatch:
                summary["transaction_mismatches"].append({"doc_id": prefix, **mismatch})
            db_dates = sorted({row.get("published_date") for row in db_rows if row.get("published_date")})
            unknown_count = sum(1 for row in db_rows if str(row.get("member_id") or "").startswith("unknown-"))
            fallback_summary = summarize_fallback_rows(prefix, db_rows)
            if fallback_summary:
                summary["fallback_doc_rows"].append(fallback_summary)

            if status == "trades":
                summary["filings_with_trades"] += 1
                if db_count != len(trades):
                    summary["row_count_mismatches"].append(
                        {"doc_id": prefix, "expected_rows": len(trades), "actual_rows": db_count}
                    )
                if db_dates != [expected_published_date]:
                    summary["published_date_mismatches"].append(
                        {
                            "doc_id": prefix,
                            "expected_published_date": expected_published_date,
                            "actual_published_dates": db_dates,
                        }
                    )
                if unknown_count:
                    summary["unknown_member_docs"].append({"doc_id": prefix, "unknown_rows": unknown_count})
            elif status == "no_trade":
                summary["no_trade_filings"] += 1
                if db_count:
                    summary["unexpected_rows_for_no_trade_filings"].append({"doc_id": prefix, "actual_rows": db_count})
            else:
                summary["source_parse_failures"].append({"doc_id": prefix, "status": status})
            return summary

        outcome = run_document(check, document_timeout)
        if outcome['status'] == 'completed':
            summary = outcome['result']
        else:
            summary['source_parse_failures'].append({'doc_id': prefix, **outcome})
        summary.setdefault('document_outcomes', []).append({'doc_id': prefix, **{key: value for key, value in outcome.items() if key != 'result'}})
        if checkpoint:
            checkpoint(summary)
        print(f"AUDIT_DOCUMENT {prefix}: {outcome['status']}", flush=True)

    return summary


@read_only_parsing
def audit_senate(supabase, *, days: int, limit: int, document_timeout: float = 120, checkpoint=None) -> dict:
    session = create_senate_session()
    members_db = load_members_lookup()
    valid_tickers = load_valid_tickers()
    filings = load_recent_senate_filings(session, days=days, limit=limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "paper_unmapped_filings": [],
        "source_parse_failures": [],
        "fallback_doc_rows": [],
        "row_count_mismatches": [],
        "transaction_mismatches": [],
        "published_date_mismatches": [],
        "unknown_member_docs": [],
    }

    for filing in filings:
        prefix = f"senate-{filing['doc_key']}"
        def check():
            try:
                # Share authentication cookies, never the parent's live TLS connection pool.
                with requests.Session() as worker_session:
                    worker_session.headers.update(session.headers)
                    worker_session.cookies.update(session.cookies)
                    trades = parse_senate_filing(worker_session, filing["doc_key"], filing, members_db, valid_tickers)
            except Exception as exc:
                if "/search/view/paper/" in filing["source_url"] and "No Senate trades parsed" in str(exc):
                    summary["paper_unmapped_filings"].append(prefix)
                    return summary
                summary["source_parse_failures"].append({"doc_id": prefix, "error": str(exc)})
                return summary

            db_count, db_rows = fetch_doc_rows(get_supabase_client(), prefix)
            mismatch = compare_transactions(trades, db_rows)
            if mismatch:
                summary["transaction_mismatches"].append({"doc_id": prefix, **mismatch})
            db_dates = sorted({row.get("published_date") for row in db_rows if row.get("published_date")})
            unknown_count = sum(1 for row in db_rows if str(row.get("member_id") or "").startswith("unknown-"))
            fallback_summary = summarize_fallback_rows(prefix, db_rows)
            if fallback_summary:
                summary["fallback_doc_rows"].append(fallback_summary)
            summary["filings_with_trades"] += 1

            if db_count != len(trades):
                summary["row_count_mismatches"].append(
                    {"doc_id": prefix, "expected_rows": len(trades), "actual_rows": db_count}
                )
            if db_dates != [filing["published_date"]]:
                summary["published_date_mismatches"].append(
                    {
                        "doc_id": prefix,
                        "expected_published_date": filing["published_date"],
                        "actual_published_dates": db_dates,
                    }
                )
            if unknown_count:
                summary["unknown_member_docs"].append({"doc_id": prefix, "unknown_rows": unknown_count})
            return summary

        outcome = run_document(check, document_timeout)
        if outcome['status'] == 'completed':
            summary = outcome['result']
        else:
            summary['source_parse_failures'].append({'doc_id': prefix, **outcome})
        summary.setdefault('document_outcomes', []).append({'doc_id': prefix, **{key: value for key, value in outcome.items() if key != 'result'}})
        if checkpoint:
            checkpoint(summary)
        print(f"AUDIT_DOCUMENT {prefix}: {outcome['status']}", flush=True)

    return summary


def fetch_recent_unknown_rows(supabase, cutoff_iso: str) -> list[dict]:
    rows = (
        supabase.table("politician_trades")
        .select("doc_id, politician_name, member_id, published_date, chamber")
        .gte("published_date", cutoff_iso)
        .like("member_id", "unknown-%")
        .order("published_date", desc=True)
        .limit(200)
        .execute()
        .data
        or []
    )

    seen_prefixes: set[str] = set()
    summarized: list[dict] = []
    for row in rows:
        prefix = summarize_doc_prefix(row.get("doc_id") or "")
        if prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        summarized.append(
            {
                "doc_id_prefix": prefix,
                "politician_name": row.get("politician_name"),
                "published_date": row.get("published_date"),
                "chamber": row.get("chamber"),
            }
        )
    return summarized


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit recent official Congress filings against stored rows.")
    parser.add_argument("--house-days", type=int, default=HOUSE_AUDIT_DAYS)
    parser.add_argument("--house-limit", type=int, default=HOUSE_AUDIT_LIMIT)
    parser.add_argument("--senate-days", type=int, default=SENATE_AUDIT_DAYS)
    parser.add_argument("--senate-limit", type=int, default=SENATE_AUDIT_LIMIT)
    parser.add_argument('--document-timeout', type=float, default=120, help='Maximum wall-clock seconds per document, including OCR and database comparison.')
    parser.add_argument('--progress-file', type=Path, default=None)
    args = parser.parse_args()
    if args.document_timeout <= 0 or min(args.house_days, args.senate_days, args.house_limit, args.senate_limit) <= 0:
        parser.error('Timeout, days, and filing limits must be positive')
    progress_file = args.progress_file or ROOT_DIR / 'data' / 'audits' / f"congress-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}.json"
    progress = {'status': 'running', 'scope': 'bounded recent filing sample, not historical completeness',
                'document_timeout_seconds': args.document_timeout, 'house': None, 'senate': None}
    save_progress(progress_file, progress)
    print(f"AUDIT_PROGRESS {progress_file}", flush=True)

    def checkpoint(chamber, summary):
        progress[chamber] = summary
        save_progress(progress_file, progress)

    try:
        supabase = get_supabase_client()
        cutoff = (congress_today() - timedelta(days=max(args.house_days, args.senate_days))).isoformat()

        house_summary = audit_house(supabase, days=args.house_days, limit=args.house_limit, document_timeout=args.document_timeout, checkpoint=lambda summary: checkpoint("house", summary))
        senate_summary = audit_senate(supabase, days=args.senate_days, limit=args.senate_limit, document_timeout=args.document_timeout, checkpoint=lambda summary: checkpoint("senate", summary))
        recent_unknown_rows = fetch_recent_unknown_rows(supabase, cutoff)

        parse_failures = (
            len(house_summary["source_parse_failures"])
            + len(house_summary["fallback_doc_rows"])
            + len(house_summary["row_count_mismatches"])
            + len(house_summary["published_date_mismatches"])
            + len(house_summary["unexpected_rows_for_no_trade_filings"])
            + len(house_summary["unknown_member_docs"])
            + len(senate_summary["source_parse_failures"])
            + len(senate_summary["fallback_doc_rows"])
            + len(senate_summary["row_count_mismatches"])
            + len(house_summary["transaction_mismatches"])
            + len(senate_summary["transaction_mismatches"])
            + len(senate_summary["published_date_mismatches"])
            + len(senate_summary["unknown_member_docs"])
            + len(senate_summary["paper_unmapped_filings"])
            + len(recent_unknown_rows)
        )

        progress.update(status='failed' if parse_failures else 'completed', house=house_summary,
                        senate=senate_summary, recent_unknown_rows=recent_unknown_rows, parse_failures=parse_failures)
        save_progress(progress_file, progress)
        emit_summary(
            {
                "house": house_summary,
                "senate": senate_summary,
                "recent_unknown_rows": recent_unknown_rows,
                "parse_failures": parse_failures,
            }
        )

        if parse_failures:
            raise SystemExit(1)
    except (Exception, KeyboardInterrupt) as exc:
        progress.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'error', error_type=type(exc).__name__)
        save_progress(progress_file, progress)
        raise


if __name__ == "__main__":
    main()
