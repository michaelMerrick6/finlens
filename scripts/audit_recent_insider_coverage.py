import argparse
import os
from collections import Counter, defaultdict
from datetime import timedelta

from pipeline_support import emit_summary, get_supabase_client
from sec_form4_support import (
    create_session,
    extract_sec_accession,
    load_recent_form4_filings,
    parse_form4_filing,
    recent_trade_key,
)
from time_utils import congress_today


AUDIT_DAYS = int(os.environ.get("SEC_RECENT_AUDIT_DAYS", "14"))
AUDIT_LIMIT = int(os.environ.get("SEC_RECENT_AUDIT_LIMIT", "800"))
AUDIT_PAGES = int(os.environ.get("SEC_RECENT_AUDIT_PAGES", "10"))
AUDIT_DB_ACCESSION_LIMIT = int(os.environ.get("SEC_RECENT_AUDIT_DB_ACCESSION_LIMIT", "80"))


def load_recent_insider_rows(supabase, *, days: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days + 2)).isoformat()
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase.table("insider_trades")
            .select("ticker, filer_name, filer_relation, transaction_date, published_date, transaction_code, amount, price, value, source_url")
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


def build_filing_inputs(feed_filings: list[dict], insider_rows: list[dict], *, db_accession_limit: int) -> list[dict]:
    filings_by_accession: dict[str, dict] = {}
    for filing in feed_filings:
        filings_by_accession[filing["accession"]] = filing

    db_added = 0
    for row in insider_rows:
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
    parser = argparse.ArgumentParser(description="Audit recent SEC Form 4 filings against insider_trades.")
    parser.add_argument("--days", type=int, default=AUDIT_DAYS)
    parser.add_argument("--limit", type=int, default=AUDIT_LIMIT)
    parser.add_argument("--pages", type=int, default=AUDIT_PAGES)
    parser.add_argument("--fresh-feed", action="store_true", help="Bypass the local recent-feed cache and refetch from SEC.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    session = create_session()
    insider_rows = load_recent_insider_rows(supabase, days=args.days)
    feed_filings = load_recent_form4_filings(
        session,
        days=args.days,
        limit=args.limit,
        pages=args.pages,
        use_cache=not args.fresh_feed,
    )
    filings = build_filing_inputs(feed_filings, insider_rows, db_accession_limit=AUDIT_DB_ACCESSION_LIMIT)

    rows_by_accession: dict[str, list[dict]] = defaultdict(list)
    for row in insider_rows:
        accession = extract_sec_accession(row.get("source_url"))
        if accession:
            rows_by_accession[accession].append(row)

    matched_filings = 0
    zero_trade_filings = 0
    mismatches: list[dict] = []
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

        official_rows = parsed.get("rows") or []
        db_rows = rows_by_accession.get(accession, [])
        official_counter = Counter(recent_trade_key(row) for row in official_rows)
        db_counter = Counter(recent_trade_key(row) for row in db_rows)

        if not official_rows and not db_rows:
            zero_trade_filings += 1
            matched_filings += 1
            continue

        if official_counter == db_counter:
            matched_filings += 1
            continue

        mismatches.append(
            {
                "accession": accession,
                "filed_date": filing.get("filed_date"),
                "official_rows": len(official_rows),
                "db_rows": len(db_rows),
                "source_url": parsed.get("source_url") or filing.get("source_url"),
            }
        )

    emit_summary(
        {
            "filings_seen": len(filings),
            "feed_filings_seen": len(feed_filings),
            "matched_filings": matched_filings,
            "zero_trade_filings": zero_trade_filings,
            "mismatches": mismatches[:50],
            "failed_accessions": failed_accessions[:50],
            "coverage_mismatches": len(mismatches),
            "parse_failures": len(mismatches) + len(failed_accessions),
        }
    )

    if mismatches or failed_accessions:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
