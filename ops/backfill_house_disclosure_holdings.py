from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import requests
from dotenv import load_dotenv
from supabase import create_client

from audit_member_house_disclosures import FILING_TYPE_LABELS, find_member_filings, load_member
from house_financial_disclosure_parser import parse_house_financial_disclosure

load_dotenv(dotenv_path=".env.local")

SUPPORTED_DISCLOSURE_TYPES = {"A", "C", "H", "O", "W"}


def env_value(key: str) -> str:
    for line in Path(".env.local").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip().strip('"').strip("'")
    return ""


supabase = create_client(
    env_value("SUPABASE_URL") or env_value("NEXT_PUBLIC_SUPABASE_URL"),
    env_value("SUPABASE_SERVICE_KEY") or env_value("SUPABASE_SERVICE_ROLE_KEY"),
)


def normalize_iso_date(raw_value: str) -> str:
    parts = raw_value.strip().split("/")
    month, day, year = parts
    if len(year) == 2:
        year = f"20{year}"
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def upsert_doc_rows(rows: list[dict]) -> None:
    if not rows:
        return
    for start in range(0, len(rows), 200):
        chunk = rows[start : start + 200]
        supabase.table("raw_filings").upsert(chunk, on_conflict="source,source_document_id").execute()


def build_raw_filing_rows(*, filing: dict, payload_rows: list[dict]) -> list[dict]:
    filed_at = normalize_iso_date(filing["filing_date"])
    received_at = datetime.now(timezone.utc).isoformat()
    source_document_prefix = f"house-disclosure-{filing['year']}-{filing['doc_id']}"
    rows: list[dict] = []

    for index, payload in enumerate(payload_rows):
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        content_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        rows.append(
            {
                "source": "house_disclosures",
                "filing_type": "disclosure_holding",
                "source_document_id": f"{source_document_prefix}-{index}",
                "source_url": filing["pdf_url"],
                "ticker": payload.get("ticker"),
                "filer_name": payload["politician_name"],
                "filed_at": filed_at,
                "received_at": received_at,
                "content_hash": content_hash,
                "payload": payload,
            }
        )

    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill House financial disclosure holdings into raw_filings for a single member.")
    parser.add_argument("--member-id", required=True, help="Bioguide / congress_members id, e.g. B001325")
    parser.add_argument("--start-year", type=int, default=2024)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true", help="Parse and print summary without writing raw_filings.")
    args = parser.parse_args()

    identity = load_member(args.member_id)
    years = list(range(args.start_year, args.end_year + 1))
    filings = [
        filing
        for filing in find_member_filings(identity, years)
        if filing["filing_type"] in SUPPORTED_DISCLOSURE_TYPES
    ]

    total_rows = 0
    docs_written = 0

    for filing in filings:
        response = requests.get(filing["pdf_url"], timeout=60)
        response.raise_for_status()
        parsed = parse_house_financial_disclosure(response.content)

        payload_rows: list[dict] = []
        for index, holding in enumerate(parsed.holdings):
            payload_rows.append(
                {
                    "member_id": identity.member_id,
                    "politician_name": f"{identity.first_name} {identity.last_name}",
                    "doc_id": filing["doc_id"],
                    "filing_year": filing["year"],
                    "filing_type": filing["filing_type"],
                    "filing_type_label": FILING_TYPE_LABELS.get(filing["filing_type"], "Unknown"),
                    "filing_date": normalize_iso_date(filing["filing_date"]),
                    "period_covered_start": parsed.period_covered_start,
                    "period_covered_end": parsed.period_covered_end,
                    "state_district": filing["state_district"],
                    "asset_name": holding.asset_name,
                    "asset_type": holding.asset_type_code,
                    "ticker": holding.ticker,
                    "owner": holding.owner,
                    "value_range": holding.value_range,
                    "source_url": filing["pdf_url"],
                    "row_index": index,
                    "section": "A",
                    "product_eligible": bool(holding.ticker and holding.value_range.strip().lower() != "none"),
                }
            )

        total_rows += len(payload_rows)
        filed_at = normalize_iso_date(filing["filing_date"])

        if not args.dry_run:
            upsert_doc_rows(build_raw_filing_rows(filing=filing, payload_rows=payload_rows))
            docs_written += 1

        print(
            json.dumps(
                {
                    "doc_id": filing["doc_id"],
                    "filing_type": filing["filing_type"],
                    "filing_date": filing["filing_date"],
                    "parsed_rows": len(payload_rows),
                    "pdf_url": filing["pdf_url"],
                },
                sort_keys=True,
            )
        )

    summary = {
        "member_id": identity.member_id,
        "member_name": f"{identity.first_name} {identity.last_name}",
        "supported_filings": len(filings),
        "rows_parsed": total_rows,
        "docs_written": docs_written,
        "dry_run": args.dry_run,
    }
    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
