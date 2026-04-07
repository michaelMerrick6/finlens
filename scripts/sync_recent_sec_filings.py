import argparse
import os
from collections import defaultdict
from datetime import timedelta

from pipeline_support import emit_summary, get_supabase_client
from sec_form4_support import create_session, extract_sec_accession, load_recent_form4_filings, parse_form4_filing
from time_utils import congress_today


RECENT_DAYS = int(os.environ.get("SEC_RECENT_SYNC_DAYS", "14"))
RECENT_LIMIT = int(os.environ.get("SEC_RECENT_SYNC_LIMIT", "800"))
RECENT_PAGES = int(os.environ.get("SEC_RECENT_SYNC_PAGES", "10"))
RECENT_DB_ACCESSION_LIMIT = int(os.environ.get("SEC_RECENT_SYNC_DB_ACCESSION_LIMIT", "80"))


def load_recent_insider_rows(supabase, *, days: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days + 2)).isoformat()
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase.table("insider_trades")
            .select("id, ticker, filer_name, filer_relation, transaction_date, published_date, transaction_code, amount, price, value, source_url")
            .gte("published_date", cutoff)
            .order("published_date", desc=True)
            .order("created_at", desc=True)
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


def upsert_companies(supabase, rows: list[dict]) -> None:
    company_rows = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        company_name = str(row.get("_company_name") or ticker).strip() or ticker
        if ticker and ticker != "UNKNOWN":
            company_rows[ticker] = {"ticker": ticker[:10], "name": company_name[:255], "sector": "Unknown", "industry": "Unknown"}
    if company_rows:
        supabase.table("companies").upsert(list(company_rows.values()), on_conflict="ticker").execute()


def build_filing_inputs(feed_filings: list[dict], recent_rows: list[dict], *, db_accession_limit: int) -> list[dict]:
    filings_by_accession: dict[str, dict] = {}
    for filing in feed_filings:
        filings_by_accession[filing["accession"]] = filing

    db_added = 0
    for row in recent_rows:
        accession = extract_sec_accession(row.get("source_url"))
        if not accession or accession in filings_by_accession:
            continue
        if db_added >= db_accession_limit:
            break
        filings_by_accession[accession] = {
            "accession": accession,
            "filed_date": row.get("published_date"),
            "source_url": str(row.get("source_url") or "").split("#", 1)[0],
        }
        db_added += 1

    return sorted(
        filings_by_accession.values(),
        key=lambda filing: (str(filing.get("filed_date") or ""), str(filing.get("accession") or "")),
        reverse=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay recent SEC Form 4 filings and replace insider_trades rows by accession.")
    parser.add_argument("--days", type=int, default=RECENT_DAYS)
    parser.add_argument("--limit", type=int, default=RECENT_LIMIT)
    parser.add_argument("--pages", type=int, default=RECENT_PAGES)
    parser.add_argument("--fresh-feed", action="store_true", help="Bypass the local recent-feed cache and refetch from SEC.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    session = create_session()
    recent_rows = load_recent_insider_rows(supabase, days=args.days)
    rows_by_accession: dict[str, list[dict]] = defaultdict(list)
    for row in recent_rows:
        accession = extract_sec_accession(row.get("source_url"))
        if accession:
            rows_by_accession[accession].append(row)

    feed_filings = load_recent_form4_filings(
        session,
        days=args.days,
        limit=args.limit,
        pages=args.pages,
        use_cache=not args.fresh_feed,
    )
    filings = build_filing_inputs(feed_filings, recent_rows, db_accession_limit=RECENT_DB_ACCESSION_LIMIT)

    filings_seen = len(filings)
    filings_with_trades = 0
    filings_without_buy_sell = 0
    rows_replaced = 0
    rows_inserted = 0
    failed_accessions: list[str] = []

    for filing in filings:
        accession = filing["accession"]
        try:
            parsed = parse_form4_filing(session, filing["source_url"], filed_date=filing.get("filed_date"))
        except Exception:
            failed_accessions.append(accession)
            continue

        if not parsed:
            failed_accessions.append(accession)
            continue

        parsed_rows = parsed.get("rows") or []
        existing_rows = rows_by_accession.get(accession, [])
        existing_ids = [row["id"] for row in existing_rows if row.get("id")]

        if existing_ids:
            for index in range(0, len(existing_ids), 200):
                chunk = existing_ids[index : index + 200]
                supabase.table("insider_trades").delete().in_("id", chunk).execute()

        rows_replaced += len(existing_ids)
        rows_by_accession[accession] = []

        if not parsed_rows:
            filings_without_buy_sell += 1
            continue

        upsert_companies(supabase, parsed_rows)
        insert_rows = [{key: value for key, value in row.items() if not key.startswith("_")} for row in parsed_rows]
        for index in range(0, len(insert_rows), 100):
            chunk = insert_rows[index : index + 100]
            supabase.table("insider_trades").insert(chunk).execute()
            rows_inserted += len(chunk)
        filings_with_trades += 1

    emit_summary(
        {
            "filings_seen": filings_seen,
            "filings_with_trades": filings_with_trades,
            "filings_without_buy_sell": filings_without_buy_sell,
            "rows_replaced": rows_replaced,
            "rows_inserted": rows_inserted,
            "failed_accessions": failed_accessions[:50],
            "parse_failures": len(failed_accessions),
        }
    )


if __name__ == "__main__":
    main()
