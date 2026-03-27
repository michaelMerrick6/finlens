import argparse
from collections import Counter

from pipeline_support import emit_summary, get_supabase_client
from politician_schema_support import politician_trades_has_asset_name_column


def normalize_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_direction(value: str | None) -> str:
    normalized = normalize_text(value)
    if normalized.startswith("buy") or normalized in {"purchase", "p"}:
        return "buy"
    if normalized.startswith("sell") or normalized in {"sale", "s"}:
        return "sell"
    if normalized.startswith("exchange") or normalized == "e":
        return "exchange"
    return normalized


def asset_match_key(value: dict) -> str:
    return "|".join(
        [
            normalize_text(value.get("source_url")),
            normalize_text(value.get("politician_name")),
            normalize_text(value.get("transaction_date")),
            normalize_direction(value.get("transaction_type")),
            normalize_text(value.get("amount_range")),
        ]
    )


def load_politician_rows(supabase, *, limit: int | None = None) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    page_size = 1000

    while True:
        query = (
            supabase.table("politician_trades")
            .select(
                "id, doc_id, ticker, source_url, politician_name, transaction_date, "
                "transaction_type, amount_range, asset_name"
            )
            .order("published_date", desc=True)
            .order("id", desc=True)
        )
        batch = query.range(offset, offset + page_size - 1).execute().data or []
        if not batch:
            break

        for row in batch:
            if str(row.get("asset_name") or "").strip():
                continue
            rows.append(row)
            if limit and len(rows) >= limit:
                return rows

        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def load_raw_asset_name_maps(supabase, rows: list[dict]) -> tuple[dict[str, str], dict[str, str]]:
    doc_ids = sorted({str(row.get("doc_id") or "").strip() for row in rows if str(row.get("doc_id") or "").strip()})
    source_urls = sorted({str(row.get("source_url") or "").strip() for row in rows if str(row.get("source_url") or "").strip()})

    doc_id_map: dict[str, str] = {}
    signature_map: dict[str, str] = {}

    for index in range(0, len(doc_ids), 100):
        chunk = doc_ids[index : index + 100]
        if not chunk:
            continue
        response = (
            supabase.table("raw_filings")
            .select("source_document_id, payload")
            .eq("source", "congress")
            .in_("source_document_id", chunk)
            .execute()
        )
        for row in response.data or []:
            payload = row.get("payload") or {}
            asset_name = str(payload.get("asset_name") or "").strip()
            source_document_id = str(row.get("source_document_id") or "").strip()
            if source_document_id and asset_name:
                doc_id_map[source_document_id] = asset_name[:255]

    for index in range(0, len(source_urls), 100):
        chunk = source_urls[index : index + 100]
        if not chunk:
            continue
        response = (
            supabase.table("raw_filings")
            .select("payload")
            .eq("source", "congress")
            .in_("source_url", chunk)
            .execute()
        )
        for row in response.data or []:
            payload = row.get("payload") or {}
            asset_name = str(payload.get("asset_name") or "").strip()
            if not asset_name:
                continue
            signature_map[asset_match_key(payload)] = asset_name[:255]

    return doc_id_map, signature_map


def load_company_name_map(supabase, tickers: set[str]) -> dict[str, str]:
    normalized = sorted({ticker.strip().upper() for ticker in tickers if ticker and ticker.strip().upper() not in {"", "N/A", "UNKNOWN", "US-TREAS"}})
    company_map: dict[str, str] = {}

    for index in range(0, len(normalized), 100):
        chunk = normalized[index : index + 100]
        if not chunk:
            continue
        response = supabase.table("companies").select("ticker, name").in_("ticker", chunk).execute()
        for row in response.data or []:
            ticker = str(row.get("ticker") or "").strip().upper()
            name = str(row.get("name") or "").strip()
            if ticker and name:
                company_map[ticker] = name[:255]

    return company_map


def resolve_asset_name(row: dict, doc_id_map: dict[str, str], signature_map: dict[str, str], company_map: dict[str, str]) -> str:
    doc_id = str(row.get("doc_id") or "").strip()
    if doc_id and doc_id in doc_id_map:
        return doc_id_map[doc_id]

    signature = asset_match_key(row)
    if signature in signature_map:
        return signature_map[signature]

    ticker = str(row.get("ticker") or "").strip().upper()
    if ticker == "US-TREAS":
        return "U.S. Treasury"

    return company_map.get(ticker, "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill politician_trades.asset_name from official Congress raw filings.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of politician rows to inspect.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    if not politician_trades_has_asset_name_column(supabase):
        raise RuntimeError("politician_trades.asset_name column does not exist yet. Apply the schema migration first.")

    rows = load_politician_rows(supabase, limit=args.limit)
    doc_id_map, signature_map = load_raw_asset_name_maps(supabase, rows)
    company_map = load_company_name_map(supabase, {str(row.get("ticker") or "") for row in rows})

    updates_by_id: dict[str, str] = {}
    source_counter: Counter[str] = Counter()

    for row in rows:
        asset_name = resolve_asset_name(row, doc_id_map, signature_map, company_map)
        if not asset_name:
            continue
        updates_by_id[str(row["id"])] = asset_name[:255]
        source_counter["raw_doc_id" if str(row.get("doc_id") or "").strip() in doc_id_map else "derived"] += 1

    updated = 0
    for row_id, asset_name in updates_by_id.items():
        supabase.table("politician_trades").update({"asset_name": asset_name}).eq("id", row_id).execute()
        updated += 1

    unresolved = max(len(rows) - updated, 0)
    emit_summary(
        {
            "rows_seen": len(rows),
            "rows_updated": updated,
            "rows_unmatched": unresolved,
            "doc_id_asset_names": len(doc_id_map),
            "signature_asset_names": len(signature_map),
            "company_asset_names": len(company_map),
            "update_sources": dict(source_counter),
        }
    )
    print(f"Backfilled politician_trades.asset_name for {updated} rows; {unresolved} rows remain unresolved.")


if __name__ == "__main__":
    main()
