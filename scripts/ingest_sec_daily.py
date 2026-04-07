import json
import os
import sys
from datetime import datetime

from pipeline_support import get_supabase_client
from sec_form4_support import create_session, extract_sec_accession, load_recent_form4_filings, parse_form4_filing


MAX_PAGES = int(os.environ.get("SEC_DAILY_MAX_PAGES", "10"))
MAX_FILINGS = int(os.environ.get("SEC_DAILY_MAX_FILINGS", "600"))
RECENT_SCAN_DAYS = int(os.environ.get("SEC_DAILY_RECENT_SCAN_DAYS", "30"))
CONSECUTIVE_EXISTING_LIMIT = int(os.environ.get("SEC_DAILY_EXISTING_STOP", "0"))


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def load_existing_accessions(supabase) -> set[str]:
    accessions: set[str] = set()
    offset = 0
    while True:
        response = (
            supabase.table("insider_trades")
            .select("source_url")
            .order("created_at", desc=True)
            .range(offset, offset + 1999)
            .execute()
        )
        rows = response.data or []
        if not rows:
            break
        for row in rows:
            accession = extract_sec_accession(row.get("source_url"))
            if accession:
                accessions.add(accession)
        if len(rows) < 2000:
            break
        offset += 2000
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
    supabase.table("companies").upsert(list(company_rows.values()), on_conflict="ticker").execute()
    return len(company_rows)


def main() -> None:
    log("Starting Daily SEC EDGAR Form 4 Scraper...")
    supabase = get_supabase_client()
    session = create_session()

    existing_accessions = load_existing_accessions(supabase)
    log(f"Loaded {len(existing_accessions)} existing accessions for dedup")

    try:
        filings = load_recent_form4_filings(
            session,
            days=RECENT_SCAN_DAYS,
            limit=MAX_FILINGS,
            pages=MAX_PAGES,
            use_cache=False,
        )
    except Exception as exc:
        print(
            "SUMMARY_JSON:"
            + json.dumps(
                {
                    "filings_seen": 0,
                    "filings_inserted": 0,
                    "filings_without_buy_sell": 0,
                    "records_seen": 0,
                    "records_inserted": 0,
                    "records_skipped": 0,
                    "companies_upserted": 0,
                    "document_fetch_errors": 1,
                    "fatal_error": True,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
        sys.exit(1)
    if not filings:
        print(
            "SUMMARY_JSON:"
            + json.dumps(
                {
                    "filings_seen": 0,
                    "filings_inserted": 0,
                    "filings_without_buy_sell": 0,
                    "records_seen": 0,
                    "records_inserted": 0,
                    "records_skipped": 0,
                    "companies_upserted": 0,
                    "document_fetch_errors": 0,
                    "fatal_error": True,
                },
                sort_keys=True,
            )
        )
        sys.exit(1)

    new_rows: list[dict] = []
    filings_inserted = 0
    filings_without_buy_sell = 0
    records_skipped = 0
    document_fetch_errors = 0
    consecutive_existing = 0
    existing_accessions_seen = 0

    for filing in filings:
        accession = filing["accession"]
        if accession in existing_accessions:
            records_skipped += 1
            existing_accessions_seen += 1
            if CONSECUTIVE_EXISTING_LIMIT > 0:
                consecutive_existing += 1
            if CONSECUTIVE_EXISTING_LIMIT > 0 and consecutive_existing >= CONSECUTIVE_EXISTING_LIMIT:
                log(f"Hit {CONSECUTIVE_EXISTING_LIMIT} consecutive existing accessions. Stopping early.")
                break
            continue

        consecutive_existing = 0
        try:
            parsed = parse_form4_filing(session, filing["source_url"], filed_date=filing.get("filed_date"))
        except Exception as exc:
            document_fetch_errors += 1
            log(f"Failed to parse Form 4 filing {accession}: {exc}")
            continue

        if not parsed:
            document_fetch_errors += 1
            log(f"Failed to parse Form 4 filing {accession}: no ownership XML found")
            continue

        parsed_rows = parsed.get("rows") or []
        if not parsed_rows:
            filings_without_buy_sell += 1
            continue

        existing_accessions.add(accession)
        filings_inserted += 1
        new_rows.extend(parsed_rows)

    companies_upserted = upsert_companies(supabase, new_rows)

    inserted_count = 0
    insert_rows = [{key: value for key, value in row.items() if not key.startswith("_")} for row in new_rows]
    for index in range(0, len(insert_rows), 100):
        chunk = insert_rows[index : index + 100]
        if not chunk:
            continue
        try:
            supabase.table("insider_trades").insert(chunk).execute()
            inserted_count += len(chunk)
        except Exception as exc:
            log(f"Failed to insert insider chunk: {exc}")
            for row in chunk:
                try:
                    supabase.table("insider_trades").insert(row).execute()
                    inserted_count += 1
                except Exception as inner_exc:
                    log(f"Failed to insert insider row {row.get('source_url')}: {inner_exc}")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "filings_seen": len(filings),
                "filings_inserted": filings_inserted,
                "filings_without_buy_sell": filings_without_buy_sell,
                "records_seen": len(new_rows),
                "records_inserted": inserted_count,
                "records_skipped": records_skipped,
                "companies_upserted": companies_upserted,
                "document_fetch_errors": document_fetch_errors,
                "existing_accessions_seen": existing_accessions_seen,
            },
            sort_keys=True,
        )
    )
    log("Daily SEC Scraper Complete.")


if __name__ == "__main__":
    main()
