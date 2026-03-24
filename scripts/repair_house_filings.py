import argparse
import csv
import io
import json
from datetime import datetime

import requests

from legacy_congress_guard import require_repair_write_opt_in
from ingest_house_official import (
    HOUSE_INDEX_URL,
    HOUSE_PTR_PDF_URL,
    extract_ocr_lines,
    extract_pdf_lines,
    extract_transactions_from_lines,
    extract_transactions_from_scanned_house_pdf,
    load_company_lookup,
    normalize_line,
    prepare_house_trades_for_insert,
    supabase,
    detect_house_attachment_only_filing,
    detect_house_no_trade_filing,
)


def load_house_index(year: int) -> dict[str, dict]:
    response = requests.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
    response.raise_for_status()
    payload = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    filings: dict[str, dict] = {}
    for row in reader:
        if (row.get("FilingType") or "").strip().upper() != "P":
            continue
        doc_id = (row.get("DocID") or "").strip()
        if not doc_id:
            continue
        filings[doc_id] = {
            "first_name": (row.get("First") or "").strip(),
            "last_name": (row.get("Last") or "").strip(),
            "filing_date": (row.get("FilingDate") or "").strip(),
            "year": year,
        }
    return filings


def parse_house_filing(year: int, doc_id: str, filing: dict, members_db: list[dict], company_lookup: list[dict]) -> list[dict]:
    pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
    pdf_resp = requests.get(pdf_url, timeout=(10, 60))
    pdf_resp.raise_for_status()

    pdf_lines = [normalize_line(line) for line in extract_pdf_lines(pdf_resp.content)]
    pdf_lines = [line for line in pdf_lines if line]
    transactions: list[dict] = []

    if sum(len(line) for line in pdf_lines) >= 80:
        transactions = extract_transactions_from_lines(
            pdf_lines,
            doc_id,
            filing["first_name"],
            filing["last_name"],
            year,
            members_db,
            company_lookup,
        )

    if not transactions:
        no_trade_filing = detect_house_no_trade_filing(pdf_resp.content)
        attachment_only = detect_house_attachment_only_filing(pdf_resp.content) if not no_trade_filing else False

        if not no_trade_filing:
            transactions = extract_transactions_from_scanned_house_pdf(
                pdf_resp.content,
                doc_id,
                filing["first_name"],
                filing["last_name"],
                year,
                members_db,
                company_lookup,
                attachment_hint=attachment_only,
            )

        if not transactions and not attachment_only and not pdf_lines:
            ocr_lines = [normalize_line(line) for line in extract_ocr_lines(pdf_resp.content)]
            ocr_lines = [line for line in ocr_lines if line]
            if ocr_lines:
                transactions = extract_transactions_from_lines(
                    ocr_lines,
                    doc_id,
                    filing["first_name"],
                    filing["last_name"],
                    year,
                    members_db,
                    company_lookup,
                )

    try:
        published_date = datetime.strptime(filing["filing_date"], "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        published_date = datetime.utcnow().strftime("%Y-%m-%d")

    for transaction in transactions:
        transaction["published_date"] = published_date
    return transactions


def replace_house_doc(year: int, doc_id: str, filing: dict, members_db: list[dict], company_lookup: list[dict]) -> tuple[int, int]:
    trades = parse_house_filing(year, doc_id, filing, members_db, company_lookup)
    if not trades:
        raise RuntimeError(f"No trades parsed for {year}:{doc_id}; refusing to replace existing data")
    prepared_trades = prepare_house_trades_for_insert(trades)

    prefix = f"house-{year}-{doc_id}"
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
    for index in range(0, len(prepared_trades), 50):
        chunk = prepared_trades[index : index + 50]
        supabase.table("politician_trades").insert(chunk).execute()
        inserted += len(chunk)

    return existing_count, inserted


def parse_target(value: str) -> tuple[int, str]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("Expected YEAR:DOC_ID")
    year_raw, doc_id = value.split(":", 1)
    return int(year_raw), doc_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Reparse and replace specific House PTR filings.")
    parser.add_argument("targets", nargs="+", type=parse_target, help="Filings in YEAR:DOC_ID form")
    args = parser.parse_args()

    years = sorted({year for year, _ in args.targets})
    index_by_year = {year: load_house_index(year) for year in years}

    members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
    members_db = members_req.data if members_req else []
    company_lookup = load_company_lookup()

    total_existing = 0
    total_inserted = 0
    for year, doc_id in args.targets:
        filing = index_by_year.get(year, {}).get(doc_id)
        if not filing:
            raise RuntimeError(f"Could not find {year}:{doc_id} in the House index")

        existing_count, inserted_count = replace_house_doc(year, doc_id, filing, members_db, company_lookup)
        total_existing += existing_count
        total_inserted += inserted_count
        print(f"Replaced {year}:{doc_id} ({existing_count} old rows -> {inserted_count} reparsed rows)")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "targets": [f"{year}:{doc_id}" for year, doc_id in args.targets],
                "rows_replaced": total_existing,
                "rows_inserted": total_inserted,
            }
        )
    )


if __name__ == "__main__":
    require_repair_write_opt_in("repair_house_filings.py")
    main()
