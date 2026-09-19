import json
import os
import sys
from datetime import datetime, timedelta

from pipeline_support import get_supabase_client
from sec_form4_support import create_session, extract_sec_accession, load_recent_form4_filings, parse_form4_filing
from time_utils import congress_today


MAX_PAGES = int(os.environ.get("SEC_DAILY_MAX_PAGES", "10"))
MAX_FILINGS = int(os.environ.get("SEC_DAILY_MAX_FILINGS", "600"))
RECENT_SCAN_DAYS = int(os.environ.get("SEC_DAILY_RECENT_SCAN_DAYS", "30"))
CONSECUTIVE_EXISTING_LIMIT = int(os.environ.get("SEC_DAILY_EXISTING_STOP", "0"))
RECENT_DB_LOOKBACK_DAYS = int(os.environ.get("SEC_DAILY_DB_LOOKBACK_DAYS", str(RECENT_SCAN_DAYS + 2)))
RECENT_DB_ACCESSION_LIMIT = int(os.environ.get("SEC_DAILY_DB_ACCESSION_LIMIT", "6000"))


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def load_existing_accessions(supabase, *, days: int, accession_limit: int = RECENT_DB_ACCESSION_LIMIT) -> set[str]:
    accessions: set[str] = set()
    cutoff = (congress_today() - timedelta(days=days)).isoformat()
    offset = 0
    while True:
        # Use the indexed ingestion timestamp for this dedupe scan. Filtering
        # and sorting by published_date can time out once insider_trades grows.
        response = (
            supabase.table("insider_trades")
            .select("source_url, created_at")
            .gte("created_at", cutoff)
            .order("created_at", desc=True)
            .range(offset, offset + 999)
            .execute()
        )
        rows = response.data or []
        if not rows:
            break
        for row in rows:
            accession = extract_sec_accession(row.get("source_url"))
            if accession:
                accessions.add(accession)
                if len(accessions) >= accession_limit:
                    return accessions
        if len(rows) < 1000:
            break
        offset += 1000
    return accessions


def upsert_companies(supabase, rows: list[dict]) -> int:
    company_rows = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        company_name = str(row.get("_company_name") or ticker).strip() or ticker
        if ticker and ticker != "UNKNOWN":
            company_rows[ticker] = {"ticker": ticker[:10], "name": company_name[:255], "sector": "Unknown", "industry": "Unknown"}
    if not company_rows:
        return 0
    supabase.table("companies").upsert(list(company_rows.values()), on_conflict="ticker", ignore_duplicates=True).execute()
    return len(company_rows)


def main() -> None:
    import time
    from sec_filing_store import register_filings, pending_filings, publish, fail
    client = get_supabase_client()
    session = create_session()
    errors = 0
    seen = inserted = completed = 0
    try:
        filings = load_recent_form4_filings(session, days=RECENT_SCAN_DAYS,
            limit=MAX_FILINGS, pages=MAX_PAGES, use_cache=False)
        register_filings(client, filings)
    except Exception as exc:
        errors += 1
        log(f"Discovery failed; processing previously saved filings: {exc}")
    # Persisted queue includes filings that have fallen out of the current RSS feed.
    deadline = time.monotonic() + int(os.environ.get('SEC_CAPTURE_SECONDS', '600'))
    for filing in pending_filings(client, MAX_FILINGS):
        if time.monotonic() >= deadline:
            log("Run budget reached; remaining filings stay queued")
            break
        seen += 1
        try:
            parsed = parse_form4_filing(session, filing['source_url'], filed_date=filing.get('filed_date'))
            if parsed is None:
                raise ValueError('No ownership XML found')
            rows = parsed.get('rows') or []
            upsert_companies(client, rows)
            inserted += publish(client, filing, rows)
            completed += 1
            log(f"Published {filing['accession']}: {len(rows)} rows")
        except Exception as exc:
            fail(client, filing, exc)
            errors += 1
            log(f"Retained for retry {filing['accession']}: {exc}")
            if getattr(getattr(exc, 'response', None), 'status_code', None) in (403, 429):
                log('SEC access restricted; stopping requests until a later run')
                break
    pending = client.table('sec_filing_queue').select('accession', count='exact').neq('status','complete').limit(1).execute().count
    print('SUMMARY_JSON:' + json.dumps(dict(filings_seen=seen, filings_completed=completed,
        records_published=inserted, parse_failures=errors, filings_pending=pending)))
    if errors or pending:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
