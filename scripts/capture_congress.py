"""Discover official filings, claim durable work, and publish complete documents."""
import argparse
import json
from pathlib import Path
from datetime import date, datetime
import os
import time

from audit_execution import run_document
from congress_filing_store import publish_filing, register_filings
from congress_member_lookup import load_congress_members
from parser_write_policy import read_only_parser_scope
from pipeline_support import emit_summary, get_supabase_client
from time_utils import congress_today


def discover(client, chamber, start_year):
    if chamber == "House":
        from sync_recent_house_filings import load_house_index
        count = 0
        for year in range(congress_today().year, start_year - 1, -1):
            filings = load_house_index(year)
            for filing in filings:
                filing["published_date"] = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").date().isoformat()
            register_filings(client, chamber, filings)
            count += len(filings)
        return count
    from sync_recent_senate_filings import create_senate_session, load_senate_interval
    count = 0
    with create_senate_session() as session:
        for year in range(congress_today().year, start_year - 1, -1):
            filings = load_senate_interval(session, date(year, 1, 1), min(date(year, 12, 31), congress_today()))
            register_filings(client, chamber, filings)
            count += len(filings)
    return count


def parse_claim(chamber, filing, members, lookup):
    # The worker can be killed on timeout. All writes happen in the parent after validation.
    # Never reuse the parent's live TLS pool after fork (ticker resolution can read DB).
    import ingest_house_official as house
    import ingest_senate_official as senate
    house.supabase = senate.supabase = get_supabase_client()
    with read_only_parser_scope():
        if chamber == "House":
            from sync_recent_house_filings import parse_house_doc
            status, trades = parse_house_doc(filing, members, lookup)
            if status not in {"trades", "no_trade"}:
                raise ValueError(f"House filing requires review: {status}")
        else:
            from sync_recent_senate_filings import create_senate_session
            from repair_senate_filings import parse_senate_filing
            with create_senate_session() as session:
                trades = parse_senate_filing(session, filing["doc_key"], filing, members, lookup)
        for trade in trades:
            if str(trade.get("member_id") or "").startswith("unknown-") or not trade.get("member_id"):
                raise ValueError("Unresolved member identity requires source review")
        return trades


def capture_chamber(client, chamber, *, start_year, limit, seconds, document_timeout, backfill=False):
    summary = {"chamber": chamber, "filings_seen": 0, "filings_completed": 0,
               "records_inserted": 0, "failed_doc_ids": [], "discovery_errors": []}
    try:
        if not backfill:
            summary["filings_seen"] = discover(client, chamber, start_year)
    except Exception as exc:
        # A source outage must not prevent retrying already discovered work.
        summary["discovery_errors"].append(str(exc))
    members = load_congress_members(client)
    if chamber == "House":
        from ingest_house_official import load_company_lookup
        lookup = load_company_lookup()
    else:
        from ingest_senate_official import load_valid_tickers
        lookup = load_valid_tickers()
    deadline = time.monotonic() + seconds
    for index in range(limit):
        if deadline - time.monotonic() < document_timeout + 10:
            break
        # Reserve every fourth slot for oldest due work, so retries/history cannot starve.
        claim = client.rpc("claim_congress_backfill" if backfill else "claim_congress_filing",
            {"target_chamber": chamber} if backfill else
            {"target_chamber": chamber, "recent_first": index % 4 != 3}).execute().data
        if not claim:
            break
        key, token = claim["filing_id"], claim["claim_token"]
        try:
            outcome = run_document(lambda: parse_claim(chamber, claim["filing"], members, lookup), document_timeout)
            if outcome["status"] != "completed":
                raise ValueError(outcome.get("error") or f"Document timed out after {document_timeout}s")
            trades = outcome["result"]
            if chamber == "House":
                from ingest_house_official import prepare_house_trades_for_insert
                trades = prepare_house_trades_for_insert(trades)
            else:
                from ingest_senate_official import prepare_senate_trades_for_insert
                trades = prepare_senate_trades_for_insert(trades)
            from sync_recent_house_filings import ensure_referenced_companies
            ensure_referenced_companies(trades)
            _, inserted = publish_filing(client, key, trades, filing=claim["filing"], token=token, verified_no_trades=not trades)
            summary["filings_completed"] += 1
            summary["records_inserted"] += inserted
            print(f"Published {key}: {inserted} rows", flush=True)
        except Exception as exc:
            summary["failed_doc_ids"].append(key)
            client.rpc("fail_congress_filing", {"target_filing_id": key, "token": token,
                       "error_message": str(exc)}).execute()
            print(f"Review/retry required {key}: {exc}", flush=True)
    unresolved = client.table("congress_filings").select("filing_id", count="exact").eq("chamber", chamber).neq("status", "complete").limit(1).execute()
    pending = client.table("congress_filings").select("filing_id", count="exact").eq("chamber", chamber).in_("status", ["pending", "processing"]).limit(1).execute()
    summary["filings_pending"] = pending.count
    summary["filings_unresolved"] = unresolved.count
    summary["coverage_complete"] = unresolved.count == 0 and not summary["discovery_errors"]
    summary["parse_failures"] = len(summary["failed_doc_ids"]) + len(summary["discovery_errors"])
    return summary


def main(chamber=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chamber", choices=["House", "Senate", "both"], default=chamber or "both")
    parser.add_argument("--start-year", type=int, default=int(os.environ.get("CONGRESS_INVENTORY_START_YEAR", "2012")))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("CONGRESS_CAPTURE_LIMIT", "40")))
    parser.add_argument("--seconds", type=int, default=int(os.environ.get("CONGRESS_CAPTURE_SECONDS", "240")), help="Processing budget per chamber; remaining work stays queued")
    parser.add_argument("--document-timeout", type=int, default=45)
    parser.add_argument("--backfill", action="store_true", help="Process untouched/expired work only; skip discovery and completed/failed retries")
    parser.add_argument("--summary-path", type=Path)
    args = parser.parse_args()
    if not 2012 <= args.start_year <= congress_today().year or args.limit < 1 or not 0 < args.document_timeout < 540 or args.seconds <= args.document_timeout + 10:
        parser.error("Invalid year, work limit, processing budget or document timeout")
    client = get_supabase_client()
    summaries = []
    for selected in (["House", "Senate"] if args.chamber == "both" else [args.chamber]):
        try:
            summaries.append(capture_chamber(client, selected, start_year=args.start_year,
                limit=args.limit, seconds=args.seconds, document_timeout=args.document_timeout, backfill=args.backfill))
        except Exception as exc:
            summaries.append({"chamber": selected, "parse_failures": 1, "error": str(exc)})
    report = {"chambers": summaries, "parse_failures": sum(row["parse_failures"] for row in summaries),
        "records_seen": sum(row.get("filings_seen", 0) for row in summaries),
        "records_inserted": sum(row.get("records_inserted", 0) for row in summaries),
        "failed_doc_ids": [key for row in summaries for key in row.get("failed_doc_ids", [])],
        "coverage_complete": all(row.get("coverage_complete", False) for row in summaries)}
    if args.summary_path:
        args.summary_path.parent.mkdir(parents=True, exist_ok=True)
        args.summary_path.write_text(json.dumps(report, indent=2) + "\n")
    emit_summary(report)
    if any(row["parse_failures"] for row in summaries):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
