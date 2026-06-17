import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
OPS_DIR = ROOT_DIR / "ops"
for path in (SCRIPTS_DIR, OPS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from ingest_house_official import load_company_lookup
from ingest_senate_official import load_valid_tickers
from pipeline_support import emit_summary, get_supabase_client
from repair_senate_filings import create_senate_session, load_members_lookup, parse_senate_filing
from sec_form4_support import create_session, load_recent_form4_filings, parse_form4_filing
from sync_recent_house_filings import load_recent_house_filings, parse_house_doc
from sync_recent_senate_filings import load_recent_senate_filings


DEFAULT_LOOKBACK_DAYS = int(os.environ.get("CAPTURE_FRESHNESS_LOOKBACK_DAYS", "10"))
DEFAULT_GRACE_DAYS = int(os.environ.get("CAPTURE_FRESHNESS_GRACE_DAYS", "2"))
DEFAULT_SOURCE_LIMIT = int(os.environ.get("CAPTURE_FRESHNESS_SOURCE_LIMIT", "120"))
DEFAULT_SEC_PAGES = int(os.environ.get("CAPTURE_FRESHNESS_SEC_PAGES", "3"))
DEFAULT_PARSE_LIMIT = int(os.environ.get("CAPTURE_FRESHNESS_PARSE_LIMIT", "50"))


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def latest_db_date(supabase, table: str) -> date | None:
    response = (
        supabase.table(table)
        .select("published_date")
        .order("published_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not rows:
        return None
    return parse_date(rows[0].get("published_date"))


def fetch_existing_filing_rows(supabase, table: str, *, doc_prefix: str, doc_ids: list[str]) -> tuple[dict[str, dict], list[dict]]:
    """Return exact doc-id matches plus all rows stored under the filing prefix."""
    clean_ids = [doc_id for doc_id in doc_ids if doc_id]
    exact_matches: dict[str, dict] = {}
    for index in range(0, len(clean_ids), 100):
        chunk = clean_ids[index : index + 100]
        if not chunk:
            continue
        response = supabase.table(table).select("doc_id,published_date").in_("doc_id", chunk).execute()
        for row in response.data or []:
            doc_id = str(row.get("doc_id") or "")
            if doc_id:
                exact_matches[doc_id] = row

    prefix_rows = (
        supabase.table(table)
        .select("doc_id,published_date")
        .ilike("doc_id", f"{doc_prefix}%")
        .limit(2000)
        .execute()
        .data
        or []
    )
    return exact_matches, prefix_rows


def summarize_coverage_gap(
    *,
    source: str,
    source_doc_id: str,
    filing_date: date | None,
    politician_name: str,
    trades: list[dict],
    existing_rows: dict[str, dict],
    prefix_rows: list[dict],
) -> dict | None:
    expected_ids = [str(trade.get("doc_id") or "") for trade in trades if trade.get("doc_id")]
    if not expected_ids:
        return None

    expected_date = filing_date.isoformat() if filing_date else None
    missing_ids = [doc_id for doc_id in expected_ids if doc_id not in existing_rows]
    date_mismatched_ids: list[str] = []
    fallback_doc_ids = sorted(
        {
            str(row.get("doc_id") or "")
            for row in prefix_rows
            if "-capitol-" in str(row.get("doc_id") or "")
        }
    )
    if expected_date:
        for doc_id in expected_ids:
            row = existing_rows.get(doc_id)
            if not row:
                continue
            if parse_date(row.get("published_date")) != filing_date:
                date_mismatched_ids.append(doc_id)

    if not missing_ids and not date_mismatched_ids and not fallback_doc_ids:
        return None

    tickers = sorted({str(trade.get("ticker") or "").upper() for trade in trades if trade.get("ticker")})
    return {
        "source": source,
        "source_doc_id": source_doc_id,
        "filing_date": expected_date,
        "politician_name": politician_name,
        "expected_rows": len(expected_ids),
        "stored_rows": len(prefix_rows),
        "missing_rows": len(missing_ids),
        "date_mismatched_rows": len(date_mismatched_ids),
        "fallback_rows": len(fallback_doc_ids),
        "tickers": tickers[:12],
        "missing_doc_ids": missing_ids[:12],
        "date_mismatched_doc_ids": date_mismatched_ids[:12],
        "fallback_doc_ids": fallback_doc_ids[:12],
    }


def collect_coverage_gaps(value: Any) -> list[dict]:
    gaps: list[dict] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "missing_or_partial_filings" and isinstance(child, list):
                gaps.extend(item for item in child if isinstance(item, dict))
            else:
                gaps.extend(collect_coverage_gaps(child))
    elif isinstance(value, list):
        for child in value:
            gaps.extend(collect_coverage_gaps(child))
    return gaps


def latest_house_trade_date(supabase, *, days: int, limit: int, parse_limit: int) -> tuple[date | None, dict]:
    members_db = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    company_lookup = load_company_lookup()
    filings = load_recent_house_filings(days=days, limit=limit)

    trade_dates: list[date] = []
    failures: list[dict] = []
    coverage_gaps: list[dict] = []
    no_trade_count = 0

    for filing in filings[:parse_limit]:
        filing_date = parse_date(filing.get("filing_date_raw"))
        try:
            status, trades = parse_house_doc(filing, members_db, company_lookup)
        except Exception as exc:
            failures.append({"doc_id": filing.get("doc_id"), "error": str(exc)[:240]})
            continue
        if status == "trades" and trades and filing_date:
            trade_dates.append(filing_date)
            expected_ids = [str(trade.get("doc_id") or "") for trade in trades if trade.get("doc_id")]
            existing_rows, prefix_rows = fetch_existing_filing_rows(
                supabase,
                "politician_trades",
                doc_prefix=f"house-{filing.get('year')}-{filing.get('doc_id')}",
                doc_ids=expected_ids,
            )
            gap = summarize_coverage_gap(
                source="house",
                source_doc_id=f"house-{filing.get('year')}-{filing.get('doc_id')}",
                filing_date=filing_date,
                politician_name=f"{filing.get('first_name', '')} {filing.get('last_name', '')}".strip(),
                trades=trades,
                existing_rows=existing_rows,
                prefix_rows=prefix_rows,
            )
            if gap:
                coverage_gaps.append(gap)
        elif status == "no_trade":
            no_trade_count += 1

    return (
        max(trade_dates) if trade_dates else None,
        {
            "filings_seen": len(filings),
            "filings_parsed": min(len(filings), parse_limit),
            "trade_filings_seen": len(trade_dates),
            "no_trade_filings_seen": no_trade_count,
            "parse_failures": failures[:10],
            "missing_or_partial_count": len(coverage_gaps),
            "missing_or_partial_filings": coverage_gaps[:20],
        },
    )


def latest_senate_trade_date(supabase, *, days: int, limit: int, parse_limit: int) -> tuple[date | None, dict]:
    session = create_senate_session()
    members_db = load_members_lookup()
    valid_tickers = load_valid_tickers()
    filings = load_recent_senate_filings(session, days=days, limit=limit)

    trade_dates: list[date] = []
    failures: list[dict] = []
    coverage_gaps: list[dict] = []
    paper_unmapped_count = 0

    for filing in filings[:parse_limit]:
        filing_date = parse_date(filing.get("published_date"))
        try:
            trades = parse_senate_filing(session, filing["doc_key"], filing, members_db, valid_tickers)
        except Exception as exc:
            if "/search/view/paper/" in str(filing.get("source_url") or "") and "No Senate trades parsed" in str(exc):
                paper_unmapped_count += 1
                continue
            failures.append({"doc_key": filing.get("doc_key"), "error": str(exc)[:240]})
            continue
        if trades and filing_date:
            trade_dates.append(filing_date)
            expected_ids = [str(trade.get("doc_id") or "") for trade in trades if trade.get("doc_id")]
            existing_rows, prefix_rows = fetch_existing_filing_rows(
                supabase,
                "politician_trades",
                doc_prefix=f"senate-{filing.get('doc_key')}",
                doc_ids=expected_ids,
            )
            gap = summarize_coverage_gap(
                source="senate",
                source_doc_id=f"senate-{filing.get('doc_key')}",
                filing_date=filing_date,
                politician_name=str(filing.get("politician_name") or ""),
                trades=trades,
                existing_rows=existing_rows,
                prefix_rows=prefix_rows,
            )
            if gap:
                coverage_gaps.append(gap)

    return (
        max(trade_dates) if trade_dates else None,
        {
            "filings_seen": len(filings),
            "filings_parsed": min(len(filings), parse_limit),
            "trade_filings_seen": len(trade_dates),
            "paper_unmapped_filings_seen": paper_unmapped_count,
            "parse_failures": failures[:10],
            "missing_or_partial_count": len(coverage_gaps),
            "missing_or_partial_filings": coverage_gaps[:20],
        },
    )


def latest_congress_source_trade_date(supabase, *, days: int, limit: int, parse_limit: int) -> tuple[date | None, dict]:
    house_date, house_summary = latest_house_trade_date(supabase, days=days, limit=limit, parse_limit=parse_limit)
    senate_date, senate_summary = latest_senate_trade_date(supabase, days=days, limit=limit, parse_limit=parse_limit)
    source_dates = [value for value in (house_date, senate_date) if value]
    return (
        max(source_dates) if source_dates else None,
        {
            "house": house_summary,
            "senate": senate_summary,
            "house_latest_trade_date": house_date.isoformat() if house_date else None,
            "senate_latest_trade_date": senate_date.isoformat() if senate_date else None,
        },
    )


def latest_insider_source_trade_date(*, days: int, limit: int, pages: int, parse_limit: int) -> tuple[date | None, dict]:
    session = create_session()
    filings = load_recent_form4_filings(session, days=days, limit=limit, pages=pages, use_cache=False)
    trade_dates: list[date] = []
    failures: list[dict] = []
    zero_trade_count = 0

    for filing in filings[:parse_limit]:
        filed_date = parse_date(filing.get("filed_date"))
        try:
            parsed = parse_form4_filing(session, filing["source_url"], filed_date=filing.get("filed_date"))
        except Exception as exc:
            failures.append({"accession": filing.get("accession"), "error": str(exc)[:240]})
            continue
        rows = (parsed or {}).get("rows") or []
        if rows and filed_date:
            trade_dates.append(filed_date)
        elif not rows:
            zero_trade_count += 1

    return (
        max(trade_dates) if trade_dates else None,
        {
            "filings_seen": len(filings),
            "filings_parsed": min(len(filings), parse_limit),
            "trade_filings_seen": len(trade_dates),
            "zero_trade_filings_seen": zero_trade_count,
            "parse_failures": failures[:10],
        },
    )


def evaluate_lag(name: str, source_latest: date | None, db_latest: date | None, *, grace_days: int, details: dict) -> dict:
    coverage_gaps = collect_coverage_gaps(details)
    if source_latest is None:
        return {
            "name": name,
            "status": "skipped",
            "reason": "No trade-bearing source filing found in lookback window.",
            "source_latest": None,
            "db_latest": db_latest.isoformat() if db_latest else None,
            "lag_days": None,
            "coverage_gap_count": len(coverage_gaps),
            "details": details,
        }

    lag_days = None if db_latest is None else (source_latest - db_latest).days
    status = "ok"
    reason = ""
    if coverage_gaps:
        status = "failed"
        reason = f"{len(coverage_gaps)} parsed source filing(s) are missing or mismatched in destination rows."
    elif db_latest is None:
        status = "failed"
        reason = "No rows found in destination table."
    elif lag_days is not None and lag_days > grace_days:
        status = "failed"
        reason = f"Database is {lag_days} days behind source trade filings."

    return {
        "name": name,
        "status": status,
        "reason": reason,
        "source_latest": source_latest.isoformat(),
        "db_latest": db_latest.isoformat() if db_latest else None,
        "lag_days": lag_days,
        "grace_days": grace_days,
        "coverage_gap_count": len(coverage_gaps),
        "details": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fail if stored trade rows lag official recent source feeds.")
    parser.add_argument("--checks", choices=["all", "congress", "insider"], default="all")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--grace-days", type=int, default=DEFAULT_GRACE_DAYS)
    parser.add_argument("--source-limit", type=int, default=DEFAULT_SOURCE_LIMIT)
    parser.add_argument("--sec-pages", type=int, default=DEFAULT_SEC_PAGES)
    parser.add_argument("--parse-limit", type=int, default=DEFAULT_PARSE_LIMIT)
    args = parser.parse_args()

    supabase = get_supabase_client()
    checks: list[dict] = []

    if args.checks in ("all", "congress"):
        source_latest, details = latest_congress_source_trade_date(
            supabase,
            days=args.lookback_days,
            limit=args.source_limit,
            parse_limit=args.parse_limit,
        )
        checks.append(
            evaluate_lag(
                "congress",
                source_latest,
                latest_db_date(supabase, "politician_trades"),
                grace_days=args.grace_days,
                details=details,
            )
        )

    if args.checks in ("all", "insider"):
        source_latest, details = latest_insider_source_trade_date(
            days=args.lookback_days,
            limit=args.source_limit,
            pages=args.sec_pages,
            parse_limit=args.parse_limit,
        )
        checks.append(
            evaluate_lag(
                "insider",
                source_latest,
                latest_db_date(supabase, "insider_trades"),
                grace_days=args.grace_days,
                details=details,
            )
        )

    failed_checks = [check for check in checks if check["status"] == "failed"]
    summary = {
        "checks": checks,
        "failed_checks": failed_checks,
        "parse_failures": len(failed_checks),
    }
    emit_summary(summary)

    if failed_checks:
        print(json.dumps(failed_checks, indent=2, sort_keys=True))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
