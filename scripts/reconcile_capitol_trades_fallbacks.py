import argparse
import csv
import io
import os
from collections import defaultdict
from datetime import datetime, timedelta

from backfill_recent_politician_member_ids import suggest_member_id
from capitol_trades_support import CAPITOL_OFFICIAL_SOURCE_DOMAINS, build_bridge_doc_id
from pipeline_support import emit_summary, get_supabase_client
from politician_schema_support import politician_trades_has_asset_name_column
from time_utils import congress_today


HOUSE_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt"
RECONCILE_DAYS = int(os.environ.get("CAPITOL_TRADES_RECONCILE_DAYS", "14"))
RECONCILE_LIMIT = int(os.environ.get("CAPITOL_TRADES_RECONCILE_LIMIT", "500"))


def load_recent_capitol_leads(supabase, *, days: int, limit: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days)).isoformat()
    response = (
        supabase.table("raw_filings")
        .select("source_document_id, source_url, filed_at, payload")
        .eq("source", "capitol_trades")
        .gte("filed_at", cutoff)
        .order("filed_at", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []


def load_recent_politician_rows(supabase, *, days: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days + 1)).isoformat()
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase.table("politician_trades")
            .select(
                "id, member_id, politician_name, chamber, party, ticker, "
                "transaction_date, published_date, transaction_type, amount_range, source_url, doc_id"
            )
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
    return (
        " ".join((actor_name or "").lower().split()),
        (ticker or "N/A").upper(),
        tx_date or "",
        (tx_type or "").lower(),
    )


def load_house_index_map(session, year: str, cache: dict[str, dict[str, str]]) -> dict[str, str]:
    if year in cache:
        return cache[year]

    mapping: dict[str, str] = {}
    try:
        response = session.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.content.decode("utf-8-sig", errors="replace")), delimiter="\t")
        for row in reader:
            if (row.get("FilingType") or "").strip().upper() != "P":
                continue
            doc_id = (row.get("DocID") or "").strip()
            raw_date = (row.get("FilingDate") or "").strip()
            if not doc_id or not raw_date:
                continue
            try:
                mapping[doc_id] = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        mapping = {}

    cache[year] = mapping
    return mapping


def resolve_published_date(session, official_source_url: str, source_rows: list[dict], lead_rows: list[dict], house_index_cache) -> str:
    existing_dates = sorted({row.get("published_date") for row in source_rows if row.get("published_date")})
    if existing_dates:
        return existing_dates[0]

    bridge_doc_id = build_bridge_doc_id(official_source_url, "probe")
    if bridge_doc_id and bridge_doc_id.startswith("house-"):
        parts = bridge_doc_id.split("-")
        if len(parts) >= 4:
            year = parts[1]
            doc_id = parts[2]
            return load_house_index_map(session, year, house_index_cache).get(doc_id) or (
                (lead_rows[0].get("payload") or {}).get("published_date")
                or lead_rows[0].get("filed_at")
                or congress_today().isoformat()
            )

    return (
        (lead_rows[0].get("payload") or {}).get("published_date")
        or lead_rows[0].get("filed_at")
        or congress_today().isoformat()
    )


def upsert_company_if_needed(supabase, ticker: str, asset_name: str) -> None:
    ticker = (ticker or "").strip().upper()
    if not ticker or ticker == "N/A":
        return
    supabase.table("companies").upsert({"ticker": ticker, "name": (asset_name or ticker)[:255]}, on_conflict="ticker").execute()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replace bad or missing official politician_trades filings with Capitol Trades rows keyed to the official source URL."
    )
    parser.add_argument("--days", type=int, default=RECONCILE_DAYS)
    parser.add_argument("--limit", type=int, default=RECONCILE_LIMIT)
    args = parser.parse_args()

    supabase = get_supabase_client()
    supports_asset_name = politician_trades_has_asset_name_column(supabase)
    members = (
        supabase.table("congress_members")
        .select("id, first_name, last_name, chamber, active, state, party")
        .execute()
        .data
        or []
    )

    leads = load_recent_capitol_leads(supabase, days=args.days, limit=args.limit)
    official_rows = load_recent_politician_rows(supabase, days=args.days)

    leads_by_source: dict[str, list[dict]] = defaultdict(list)
    rows_by_source: dict[str, list[dict]] = defaultdict(list)
    for lead in leads:
        payload = lead.get("payload") or {}
        official_source_url = payload.get("official_source_url") or ""
        if official_source_url and any(domain in official_source_url for domain in CAPITOL_OFFICIAL_SOURCE_DOMAINS):
            leads_by_source[official_source_url].append(lead)
    for row in official_rows:
        source_url = row.get("source_url") or ""
        if source_url:
            rows_by_source[source_url].append(row)

    session = None
    house_index_cache: dict[str, dict[str, str]] = {}

    sources_seen = len(leads_by_source)
    sources_replaced = 0
    rows_inserted = 0
    rows_deleted = 0
    sources_skipped = 0
    sources_skipped_same = 0
    skipped_unrecognized = 0
    errors: list[dict] = []
    replaced_sources: list[dict] = []

    for official_source_url, source_leads in sorted(leads_by_source.items()):
        bridge_doc_ids = []
        lead_rows: list[dict] = []
        lead_exact: set[tuple[str, str, str, str]] = set()
        source_rows = rows_by_source.get(official_source_url, [])
        official_exact = {
            exact_match_key(
                row.get("politician_name") or "",
                row.get("ticker") or "N/A",
                row.get("transaction_date") or "",
                row.get("transaction_type") or "",
            )
            for row in source_rows
        }

        for lead in source_leads:
            payload = lead.get("payload") or {}
            trade_id = payload.get("capitol_trade_id") or str(lead.get("source_document_id") or "").split("-")[-1]
            bridge_doc_id = build_bridge_doc_id(official_source_url, trade_id)
            if not bridge_doc_id:
                skipped_unrecognized += 1
                continue
            bridge_doc_ids.append(bridge_doc_id)
            lead_rows.append(lead)
            lead_exact.add(
                exact_match_key(
                    payload.get("politician_name") or "",
                    payload.get("ticker") or "N/A",
                    payload.get("transaction_date") or "",
                    payload.get("transaction_type") or "",
                )
            )

        if not lead_rows:
            continue

        if len(source_rows) == len(lead_rows) and official_exact == lead_exact:
            sources_skipped_same += 1
            continue

        if session is None:
            from capitol_trades_support import create_session

            session = create_session()

        published_date = resolve_published_date(session, official_source_url, source_rows, lead_rows, house_index_cache)
        first_payload = lead_rows[0].get("payload") or {}
        actor_name = (source_rows[0].get("politician_name") if source_rows else None) or first_payload.get("politician_name") or ""
        chamber = (source_rows[0].get("chamber") if source_rows else None) or first_payload.get("chamber") or "Unknown"
        party = (source_rows[0].get("party") if source_rows else None) or first_payload.get("party") or "Unknown"
        member_id = (source_rows[0].get("member_id") if source_rows else None) or suggest_member_id(actor_name, chamber, members)
        replace_reason = "missing_official_rows" if not source_rows else "official_mismatch"

        prepared_rows: list[dict] = []
        for lead in lead_rows:
            payload = lead.get("payload") or {}
            ticker = (payload.get("ticker") or "N/A").upper()
            transaction_date = payload.get("transaction_date")
            transaction_type = (payload.get("transaction_type") or "").lower()
            bridge_doc_id = build_bridge_doc_id(
                official_source_url,
                payload.get("capitol_trade_id") or str(lead.get("source_document_id") or "").split("-")[-1],
            )
            if not transaction_date or transaction_type not in {"buy", "sell", "exchange"} or not bridge_doc_id:
                continue

            upsert_company_if_needed(supabase, ticker, payload.get("asset_name") or ticker)
            prepared_rows.append(
                {
                    "member_id": member_id,
                    "politician_name": actor_name,
                    "chamber": chamber,
                    "party": party,
                    "ticker": ticker,
                    "transaction_date": transaction_date,
                    "published_date": published_date,
                    "transaction_type": transaction_type,
                    "asset_type": "Stock",
                    "amount_range": payload.get("amount_range") or "Unknown",
                    "source_url": official_source_url,
                    "doc_id": bridge_doc_id,
                    **(
                        {"asset_name": str(payload.get("asset_name") or "").strip()[:255]}
                        if supports_asset_name and str(payload.get("asset_name") or "").strip()
                        else {}
                    ),
                }
            )

        if not prepared_rows:
            sources_skipped += 1
            errors.append({"official_source_url": official_source_url, "error": "no_prepared_rows"})
            continue

        try:
            existing_count = (
                supabase.table("politician_trades")
                .select("id", count="exact")
                .eq("source_url", official_source_url)
                .limit(2000)
                .execute()
                .count
                or 0
            )
            if existing_count:
                supabase.table("politician_trades").delete().eq("source_url", official_source_url).execute()
            for index in range(0, len(prepared_rows), 50):
                chunk = prepared_rows[index : index + 50]
                supabase.table("politician_trades").insert(chunk).execute()
            rows_deleted += existing_count
            rows_inserted += len(prepared_rows)
            sources_replaced += 1
            replaced_sources.append(
                {
                    "official_source_url": official_source_url,
                    "reason": replace_reason,
                    "existing_rows": existing_count,
                    "lead_rows": len(prepared_rows),
                    "published_date": published_date,
                }
            )
        except Exception as exc:
            errors.append({"official_source_url": official_source_url, "error": str(exc)})

    emit_summary(
        {
            "sources_seen": sources_seen,
            "sources_replaced": sources_replaced,
            "sources_skipped": sources_skipped + sources_skipped_same,
            "sources_skipped_same": sources_skipped_same,
            "skipped_unrecognized": skipped_unrecognized,
            "rows_deleted": rows_deleted,
            "rows_inserted": rows_inserted,
            "replaced_sources": replaced_sources[:20],
            "errors": errors[:20],
            "parse_failures": len(errors),
        }
    )

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
