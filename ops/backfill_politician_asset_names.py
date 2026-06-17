from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from functools import lru_cache

from ingest_senate_official import load_valid_tickers
from pipeline_support import emit_summary, get_supabase_client
from repair_house_filings import load_house_index, load_company_lookup, parse_house_filing
from repair_senate_filings import create_senate_session, load_members_lookup, parse_senate_filing


HOUSE_SOURCE_RE = re.compile(r"/ptr-pdfs/(?P<year>\d{4})/(?P<doc_id>\d+)\.pdf$")
UNRESOLVED_TICKERS = {"N/A", "US-TREAS"}
MANUAL_SOURCE_ASSET_NAMES = {
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2015/9108306.pdf": {
        "house-2015-9108306-0": "Anchorage, AK Electric Utility Bond",
        "house-2015-9108306-1": "CA State General Obligation Bond",
        "house-2015-9108306-2": "WA State General Obligation Bond",
    }
}


@lru_cache(maxsize=1)
def get_members_db() -> list[dict]:
    return load_members_lookup()


@lru_cache(maxsize=1)
def get_company_lookup_cached() -> list[dict]:
    return load_company_lookup()


@lru_cache(maxsize=1)
def get_valid_tickers_cached() -> set[str]:
    return load_valid_tickers()


@lru_cache(maxsize=8)
def get_house_index_cached(year: int) -> dict[str, dict]:
    return load_house_index(year)


def load_unresolved_raw_filings(
    supabase,
    *,
    limit: int | None = None,
    load_all: bool = False,
    page_size: int = 1000,
) -> list[dict]:
    unresolved: list[dict] = []
    offset = 0

    while True:
        query = (
            supabase.table("raw_filings")
            .select("id,source_document_id,source_url,ticker,payload,filed_at")
            .eq("source", "congress")
            .in_("ticker", sorted(UNRESOLVED_TICKERS))
            .order("filed_at", desc=True)
        )
        if load_all:
            response = query.range(offset, offset + page_size - 1).execute()
        else:
            response = query.limit(max(1, limit or page_size)).execute()
        rows = response.data or []
        if not rows:
            break

        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            payload = row.get("payload") or {}
            if ticker not in UNRESOLVED_TICKERS:
                continue
            if str(payload.get("asset_name") or "").strip():
                continue
            unresolved.append(row)
            if not load_all and limit and len(unresolved) >= limit:
                return unresolved

        if not load_all or len(rows) < page_size:
            break
        offset += page_size

    return unresolved


def fallback_house_filing(row: dict, year: int, doc_id: str) -> dict:
    payload = row.get("payload") or {}
    full_name = str(payload.get("politician_name") or "").strip()
    if " " in full_name:
        first_name, last_name = full_name.rsplit(" ", 1)
    else:
        first_name, last_name = full_name, ""

    published_date = str(payload.get("published_date") or row.get("filed_at") or "").strip()
    filing_date = published_date
    if published_date:
        try:
            filing_date = datetime.strptime(published_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        except ValueError:
            pass

    return {
        "first_name": first_name,
        "last_name": last_name,
        "filing_date": filing_date,
        "year": year,
    }


def trade_asset_name(trade: dict) -> str:
    asset_name = str(
        trade.get("asset_name")
        or trade.get("_company_name")
        or trade.get("_asset_name")
        or ""
    ).strip()
    if asset_name:
        return asset_name
    if str(trade.get("ticker") or "").strip().upper() == "US-TREAS":
        return "U.S. Treasury"
    return ""


def parse_house_source(source_url: str, row: dict) -> dict[str, str]:
    match = HOUSE_SOURCE_RE.search(source_url)
    if not match:
        return {}

    year = int(match.group("year"))
    doc_id = match.group("doc_id")
    filing = get_house_index_cached(year).get(doc_id) or fallback_house_filing(row, year, doc_id)
    trades = parse_house_filing(year, doc_id, filing, get_members_db(), get_company_lookup_cached())
    return {str(trade.get("doc_id") or ""): trade_asset_name(trade) for trade in trades if trade_asset_name(trade)}


def parse_senate_source(
    source_url: str,
    row: dict,
) -> dict[str, str]:
    payload = row.get("payload") or {}
    doc_id = str(row.get("source_document_id") or payload.get("doc_id") or "").strip()
    if not doc_id.startswith("senate-"):
        return {}

    doc_key = str(source_url.rstrip("/").split("/")[-1] or "").strip()
    if not doc_key:
        return {}

    filing = {
        "politician_name": payload.get("politician_name") or "",
        "source_url": source_url,
        "published_date": payload.get("published_date") or row.get("filed_at"),
    }
    trades = parse_senate_filing(create_senate_session(), doc_key, filing, get_members_db(), get_valid_tickers_cached())
    return {str(trade.get("doc_id") or ""): trade_asset_name(trade) for trade in trades if trade_asset_name(trade)}


def parse_source_asset_names(source_url: str, row: dict) -> dict[str, str]:
    if "disclosures-clerk.house.gov" in source_url:
        return parse_house_source(source_url, row)
    if "efdsearch.senate.gov" in source_url:
        return parse_senate_source(source_url, row)
    return {}


def parse_source_with_timeout(source_url: str, row: dict, timeout_seconds: int) -> dict[str, str]:
    if timeout_seconds <= 0:
        return parse_source_asset_names(source_url, row)

    payload = json.dumps({"source_url": source_url, "row": row})
    result = subprocess.run(
        [sys.executable, __file__, "--parse-source-json", payload],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(stderr or f"Child parser exited with code {result.returncode}")
    stdout = (result.stdout or "").strip()
    if not stdout:
        return {}
    return json.loads(stdout)


def update_raw_filing_asset_name(supabase, row_id: str, payload: dict, asset_name: str) -> None:
    next_payload = dict(payload or {})
    next_payload["asset_name"] = asset_name
    supabase.table("raw_filings").update({"payload": next_payload}).eq("id", row_id).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill official asset names into congress raw_filings payloads.")
    parser.add_argument("--limit", type=int, default=300, help="Max unresolved congress raw_filings rows to inspect.")
    parser.add_argument("--all", action="store_true", help="Process the full unresolved congress asset-name backlog.")
    parser.add_argument(
        "--max-seconds-per-filing",
        type=int,
        default=120,
        help="Abort a single official filing parse if it exceeds this many seconds.",
    )
    parser.add_argument("--parse-source-json", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.parse_source_json:
        request = json.loads(args.parse_source_json)
        parsed = parse_source_asset_names(str(request.get("source_url") or ""), request.get("row") or {})
        print(json.dumps(parsed))
        return

    supabase = get_supabase_client()
    rows = load_unresolved_raw_filings(
        supabase,
        limit=None if args.all else args.limit,
        load_all=args.all,
    )
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        source_url = str(row.get("source_url") or "").strip()
        if source_url:
            grouped[source_url].append(row)

    summary = {
        "rows_seen": len(rows),
        "filings_seen": len(grouped),
        "rows_updated": 0,
        "rows_unmatched": 0,
        "filings_failed": 0,
        "failed_sources": [],
    }

    for index, (source_url, source_rows) in enumerate(grouped.items(), start=1):
        print(
            f"[{index}/{len(grouped)}] parsing {source_url} "
            f"for {len(source_rows)} unresolved rows",
            flush=True,
        )
        try:
            parsed_asset_names = parse_source_with_timeout(
                source_url,
                source_rows[0],
                args.max_seconds_per_filing,
            )
        except Exception as exc:
            summary["filings_failed"] += 1
            summary["failed_sources"].append({"source_url": source_url, "error": str(exc)})
            summary["rows_unmatched"] += len(source_rows)
            print(f"  failed: {exc}", flush=True)
            continue

        manual_asset_names = MANUAL_SOURCE_ASSET_NAMES.get(source_url, {})
        for doc_id, asset_name in manual_asset_names.items():
            if asset_name:
                parsed_asset_names.setdefault(doc_id, asset_name)

        matched_rows = 0
        for row in source_rows:
            source_document_id = str(row.get("source_document_id") or "").strip()
            asset_name = parsed_asset_names.get(source_document_id, "")
            if not asset_name:
                summary["rows_unmatched"] += 1
                continue
            update_raw_filing_asset_name(supabase, row["id"], row.get("payload") or {}, asset_name)
            summary["rows_updated"] += 1
            matched_rows += 1

        print(f"  matched {matched_rows}/{len(source_rows)} rows", flush=True)

    emit_summary(summary)
    print(
        f"Asset-name backfill complete: {summary['rows_updated']} rows updated, "
        f"{summary['rows_unmatched']} unmatched, {summary['filings_failed']} filings failed."
    )


if __name__ == "__main__":
    main()
