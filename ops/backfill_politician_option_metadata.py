from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
from collections import Counter

from backfill_politician_asset_names import parse_source_asset_names
from pipeline_support import emit_summary, get_supabase_client


OPTION_METADATA_MARKERS = ("call option", "put option", "strike $", "expires ")


def normalize_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def has_option_metadata(asset_name: str | None) -> bool:
    normalized = normalize_text(asset_name)
    return any(marker in normalized for marker in OPTION_METADATA_MARKERS)


def load_target_rows(supabase, *, limit: int | None = None) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    page_size = 500

    while True:
        batch = (
            supabase.table("politician_trades")
            .select("id, doc_id, source_url, asset_name, asset_type")
            .eq("asset_type", "OP")
            .order("published_date", desc=True)
            .order("id", desc=True)
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        if not batch:
            break

        for row in batch:
            if has_option_metadata(row.get("asset_name")):
                continue
            rows.append(row)
            if limit and len(rows) >= limit:
                return rows

        if len(batch) < page_size:
            break
        offset += page_size

    return rows


def load_raw_row_map(supabase, source_urls: list[str]) -> dict[str, dict]:
    rows_by_url: dict[str, dict] = {}

    for index in range(0, len(source_urls), 100):
        chunk = source_urls[index : index + 100]
        if not chunk:
            continue
        response = (
            supabase.table("raw_filings")
            .select("id, source_document_id, source_url, payload, filed_at")
            .eq("source", "congress")
            .in_("source_url", chunk)
            .execute()
        )
        for row in response.data or []:
            source_url = str(row.get("source_url") or "").strip()
            if source_url and source_url not in rows_by_url:
                rows_by_url[source_url] = row

    return rows_by_url


def update_raw_filing_payloads(supabase, updates_by_doc_id: dict[str, str]) -> int:
    updated = 0
    for index in range(0, len(updates_by_doc_id), 100):
        chunk_doc_ids = list(updates_by_doc_id.keys())[index : index + 100]
        response = (
            supabase.table("raw_filings")
            .select("id, source_document_id, payload")
            .eq("source", "congress")
            .in_("source_document_id", chunk_doc_ids)
            .execute()
        )
        for row in response.data or []:
            doc_id = str(row.get("source_document_id") or "").strip()
            asset_name = updates_by_doc_id.get(doc_id)
            if not asset_name:
                continue
            payload = dict(row.get("payload") or {})
            next_payload = dict(payload)
            next_payload["asset_name"] = asset_name[:255]
            if str(next_payload.get("asset_type") or "").strip().upper() != "OP":
                next_payload["asset_type"] = "OP"
            if next_payload == payload:
                continue
            supabase.table("raw_filings").update({"payload": next_payload}).eq("id", row["id"]).execute()
            updated += 1
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill strike and expiration metadata for congress option trades."
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of option rows to inspect.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    rows = load_target_rows(supabase, limit=args.limit)
    source_urls = sorted({str(row.get("source_url") or "").strip() for row in rows if str(row.get("source_url") or "").strip()})
    raw_rows_by_url = load_raw_row_map(supabase, source_urls)

    parsed_by_url: dict[str, dict[str, str]] = {}
    parse_errors: Counter[str] = Counter()
    for source_url in source_urls:
        raw_row = raw_rows_by_url.get(source_url)
        if not raw_row:
            parse_errors["missing_raw_row"] += 1
            continue
        try:
            parsed_by_url[source_url] = parse_source_asset_names(source_url, raw_row)
        except Exception:
            parse_errors["parse_failed"] += 1

    updates_by_trade_id: dict[str, str] = {}
    updates_by_doc_id: dict[str, str] = {}
    for row in rows:
        doc_id = str(row.get("doc_id") or "").strip()
        source_url = str(row.get("source_url") or "").strip()
        asset_name = parsed_by_url.get(source_url, {}).get(doc_id, "")
        if not has_option_metadata(asset_name):
            continue
        updates_by_trade_id[str(row["id"])] = asset_name[:255]
        updates_by_doc_id[doc_id] = asset_name[:255]

    updated_trades = 0
    for trade_id, asset_name in updates_by_trade_id.items():
        supabase.table("politician_trades").update({"asset_name": asset_name}).eq("id", trade_id).execute()
        updated_trades += 1

    updated_raw_filings = update_raw_filing_payloads(supabase, updates_by_doc_id)
    unresolved = max(len(rows) - len(updates_by_trade_id), 0)

    emit_summary(
        {
            "rows_seen": len(rows),
            "politician_trades_updated": updated_trades,
            "raw_filings_updated": updated_raw_filings,
            "rows_unresolved": unresolved,
            "source_urls_parsed": len(parsed_by_url),
            "parse_errors": dict(parse_errors),
        }
    )
    print(
        f"Backfilled option metadata for {updated_trades} politician trades and {updated_raw_filings} raw filings; "
        f"{unresolved} rows remain unresolved."
    )


if __name__ == "__main__":
    main()
