from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
from datetime import datetime
from pathlib import Path

from ingest_house_official import (
    HOUSE_PTR_PDF_URL,
    load_company_lookup,
    prepare_house_trades_for_insert,
    resolve_member_id,
    supabase,
)


def parse_house_date(raw_value: str) -> str:
    return datetime.strptime(raw_value, "%m/%d/%Y").strftime("%Y-%m-%d")


def replace_doc_rows(prefix: str, trades: list[dict]) -> tuple[int, int]:
    prepared = prepare_house_trades_for_insert(trades)
    existing = (
        supabase.table("politician_trades")
        .select("doc_id", count="exact")
        .ilike("doc_id", f"{prefix}%")
        .limit(2000)
        .execute()
    )
    existing_count = existing.count or 0
    supabase.table("politician_trades").delete().ilike("doc_id", f"{prefix}%").execute()

    inserted = 0
    for index in range(0, len(prepared), 50):
        chunk = prepared[index : index + 50]
        supabase.table("politician_trades").insert(chunk).execute()
        inserted += len(chunk)
    return existing_count, inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Import manually reviewed House PTR fixes from JSON.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/house_review_fixes.json"),
        help="Path to manual House review fixes JSON",
    )
    args = parser.parse_args()

    payload = json.loads(args.input.read_text())
    members_db = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    load_company_lookup()

    applied: list[dict] = []
    for filing in payload:
        year = int(filing["year"])
        doc_id = str(filing["doc_id"])
        first_name = str(filing["first_name"])
        last_name = str(filing["last_name"])
        prefix = f"house-{year}-{doc_id}"
        member_id = resolve_member_id(first_name, last_name, members_db)
        published_date = parse_house_date(str(filing["filing_date"]))
        source_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)

        trades: list[dict] = []
        for index, trade in enumerate(filing["trades"]):
            trades.append(
                {
                    "member_id": member_id,
                    "politician_name": f"{first_name} {last_name}"[:100],
                    "chamber": "House",
                    "party": "Unknown",
                    "ticker": str(trade["ticker"]).upper()[:10],
                    "transaction_date": parse_house_date(str(trade["transaction_date"])),
                    "published_date": published_date,
                    "transaction_type": str(trade["transaction_type"]).lower(),
                    "asset_type": "Stock",
                    "amount_range": str(trade["amount_range"])[:255],
                    "source_url": source_url,
                    "doc_id": f"{prefix}-{index}",
                    "_company_name": str(trade["asset_name"])[:255],
                }
            )

        replaced, inserted = replace_doc_rows(prefix, trades)
        applied.append({"doc_id": prefix, "rows_replaced": replaced, "rows_inserted": inserted})
        print(f"APPLIED {prefix}: replaced={replaced} inserted={inserted}")

    print("SUMMARY_JSON:" + json.dumps({"applied": applied}, sort_keys=True))


if __name__ == "__main__":
    main()
