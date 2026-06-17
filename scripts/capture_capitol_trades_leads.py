import argparse
import os
from datetime import timedelta

from capitol_trades_support import CAPITOL_TRADES_LIST_URL, content_hash, create_session, enrich_with_detail, parse_trade_page
from pipeline_support import emit_summary, get_supabase_client
from time_utils import congress_now


LOOKBACK_DAYS = int(os.environ.get("CAPITOL_TRADES_LOOKBACK_DAYS", "21"))
MAX_PAGES = int(os.environ.get("CAPITOL_TRADES_MAX_PAGES", "20"))
PAGE_SIZE = int(os.environ.get("CAPITOL_TRADES_PAGE_SIZE", "96"))


def load_existing_leads(supabase, *, cutoff_iso: str | None = None) -> dict[str, dict]:
    existing: dict[str, dict] = {}
    offset = 0
    while True:
        query = supabase.table("raw_filings").select("source_document_id, payload, content_hash").eq("source", "capitol_trades")
        if cutoff_iso:
            query = query.gte("filed_at", cutoff_iso)
        response = query.range(offset, offset + 999).execute()
        rows = response.data or []
        if not rows:
            break
        for row in rows:
            existing[str(row["source_document_id"])] = row
        if len(rows) < 1000:
            break
        offset += 1000
    return existing


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture Capitol Trades rows as provisional politician trade leads.")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--pages", type=int, default=MAX_PAGES)
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    args = parser.parse_args()

    supabase = get_supabase_client()
    session = create_session()
    now = congress_now()
    cutoff = (now.date() - timedelta(days=args.lookback_days)).isoformat()
    existing = load_existing_leads(supabase, cutoff_iso=cutoff)

    records_seen = 0
    records_older_than_cutoff_skipped = 0
    records_inserted = 0
    records_updated = 0
    records_skipped = 0
    official_source_hits = 0
    detail_lookup_failures: list[str] = []
    upsert_payload: list[dict] = []
    latest_published_date: str | None = None
    oldest_recent_published_date: str | None = None
    pages_fetched = 0

    for page in range(1, args.pages + 1):
        response = session.get(CAPITOL_TRADES_LIST_URL, params={"page": page, "pageSize": args.page_size}, timeout=30)
        response.raise_for_status()
        trades = parse_trade_page(response.text, now=now)
        if not trades:
            break
        pages_fetched += 1

        fresh_page_rows = []
        for lead in trades:
            published_date = str(lead.get("published_date") or "").strip()
            if published_date and published_date < cutoff:
                records_older_than_cutoff_skipped += 1
                continue
            fresh_page_rows.append(lead)

        if not fresh_page_rows:
            break

        for lead in fresh_page_rows:
            records_seen += 1
            published_date = str(lead.get("published_date") or "").strip()
            if published_date:
                if latest_published_date is None or published_date > latest_published_date:
                    latest_published_date = published_date
                if oldest_recent_published_date is None or published_date < oldest_recent_published_date:
                    oldest_recent_published_date = published_date
            existing_row = existing.get(lead["source_document_id"])
            existing_payload = (existing_row or {}).get("payload") or {}
            official_source_url = existing_payload.get("official_source_url")

            if not official_source_url:
                try:
                    detail_metadata = enrich_with_detail(session, lead["detail_url"])
                    official_source_url = detail_metadata.get("official_source_url")
                except Exception:
                    detail_lookup_failures.append(lead["source_document_id"])

            lead["official_source_url"] = official_source_url
            if official_source_url:
                official_source_hits += 1

            lead_hash = content_hash(lead)
            if existing_row and existing_row.get("content_hash") == lead_hash:
                records_skipped += 1
                continue

            if existing_row:
                records_updated += 1
            else:
                records_inserted += 1

            upsert_payload.append(
                {
                    "source": "capitol_trades",
                    "filing_type": "politician_trade_lead",
                    "source_document_id": lead["source_document_id"],
                    "source_url": lead["detail_url"],
                    "ticker": lead["ticker"],
                    "filer_name": lead["politician_name"],
                    "filed_at": lead["published_date"],
                    "content_hash": lead_hash,
                    "payload": lead,
                }
            )

    for index in range(0, len(upsert_payload), 100):
        chunk = upsert_payload[index : index + 100]
        if chunk:
            supabase.table("raw_filings").upsert(chunk, on_conflict="source,source_document_id").execute()

    emit_summary(
        {
            "cutoff_date": cutoff,
            "lookback_days": args.lookback_days,
            "pages_fetched": pages_fetched,
            "records_seen": records_seen,
            "records_older_than_cutoff_skipped": records_older_than_cutoff_skipped,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_skipped": records_skipped,
            "official_source_hits": official_source_hits,
            "latest_published_date": latest_published_date,
            "oldest_recent_published_date": oldest_recent_published_date,
            "detail_lookup_failures": detail_lookup_failures[:20],
            "parse_failures": len(detail_lookup_failures),
        }
    )


if __name__ == "__main__":
    main()
