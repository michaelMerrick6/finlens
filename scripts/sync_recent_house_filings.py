import argparse
import csv
import io
import json
import os
from datetime import datetime, timedelta

import requests

from ingest_house_official import (
    HOUSE_INDEX_URL,
    HOUSE_PTR_PDF_URL,
    detect_house_attachment_only_filing,
    detect_house_no_trade_filing,
    extract_ocr_lines,
    extract_pdf_lines,
    extract_transactions_from_lines,
    extract_transactions_from_scanned_house_pdf,
    load_company_lookup,
    normalize_line,
    prepare_house_trades_for_insert,
    supabase,
)
from time_utils import congress_today


RECENT_DAYS = int(os.environ.get("HOUSE_RECENT_SYNC_DAYS", "3"))
MAX_FILINGS = int(os.environ.get("HOUSE_RECENT_SYNC_LIMIT", "20"))


def load_house_index(year: int) -> list[dict]:
    response = requests.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
    response.raise_for_status()
    payload = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    filings: list[dict] = []
    for row in reader:
        if (row.get("FilingType") or "").strip().upper() != "P":
            continue
        doc_id = (row.get("DocID") or "").strip()
        if not doc_id:
            continue
        filings.append(
            {
                "doc_id": doc_id,
                "first_name": (row.get("First") or "").strip(),
                "last_name": (row.get("Last") or "").strip(),
                "filing_date_raw": (row.get("FilingDate") or "").strip(),
                "year": year,
            }
        )
    filings.sort(
        key=lambda filing: (
            datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y"),
            int("".join(ch for ch in filing["doc_id"] if ch.isdigit()) or "0"),
        ),
        reverse=True,
    )
    return filings


def parse_house_doc(filing: dict, members_db: list[dict], company_lookup: list[dict]) -> tuple[str, list[dict]]:
    year = filing["year"]
    doc_id = filing["doc_id"]
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

    if transactions:
        status = "trades"
    else:
        no_trade_filing = detect_house_no_trade_filing(pdf_resp.content)
        attachment_only = detect_house_attachment_only_filing(pdf_resp.content) if not no_trade_filing else False
        if no_trade_filing:
            status = "no_trade"
        else:
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
            if transactions:
                status = "trades"
            elif attachment_only:
                status = "attachment_unparsed"
            else:
                status = "unparsed"

        if not transactions and status == "unparsed" and not pdf_lines:
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
                if transactions:
                    status = "trades"

    published_date = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").strftime("%Y-%m-%d")
    for transaction in transactions:
        transaction["published_date"] = published_date

    return status, transactions


def replace_doc_rows(filing: dict, trades: list[dict]) -> tuple[int, int]:
    prepared_trades = prepare_house_trades_for_insert(trades)
    prefix = f"house-{filing['year']}-{filing['doc_id']}"
    existing = (
        supabase.table("politician_trades")
        .select("id", count="exact")
        .ilike("doc_id", f"{prefix}%")
        .limit(2000)
        .execute()
    )
    existing_count = existing.count or 0
    supabase.table("politician_trades").delete().ilike("doc_id", f"{prefix}%").execute()

    inserted_count = 0
    if prepared_trades:
        for index in range(0, len(prepared_trades), 50):
            chunk = prepared_trades[index : index + 50]
            supabase.table("politician_trades").insert(chunk).execute()
            inserted_count += len(chunk)
    return existing_count, inserted_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Replace the most recent House filings from the official index.")
    parser.add_argument("--days", type=int, default=RECENT_DAYS)
    parser.add_argument("--limit", type=int, default=MAX_FILINGS)
    args = parser.parse_args()

    today = congress_today()
    cutoff = today - timedelta(days=args.days)
    filings = [filing for filing in load_house_index(today.year) if datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").date() >= cutoff]
    filings = filings[: args.limit]

    members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
    members_db = members_req.data if members_req else []
    company_lookup = load_company_lookup()

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "no_trade_filings": 0,
        "rows_replaced": 0,
        "rows_inserted": 0,
        "failed_doc_ids": [],
    }

    for filing in filings:
        status, trades = parse_house_doc(filing, members_db, company_lookup)
        if status == "trades":
            summary["filings_with_trades"] += 1
        elif status == "no_trade":
            summary["no_trade_filings"] += 1
        else:
            summary["failed_doc_ids"].append(f"{filing['year']}-{filing['doc_id']}")
            continue

        replaced_count, inserted_count = replace_doc_rows(filing, trades)
        summary["rows_replaced"] += replaced_count
        summary["rows_inserted"] += inserted_count
        print(
            f"Synced {filing['year']}:{filing['doc_id']} {filing['filing_date_raw']} "
            f"status={status} replaced={replaced_count} inserted={inserted_count}"
        )

    summary["parse_failures"] = len(summary["failed_doc_ids"])
    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))

    if summary["failed_doc_ids"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
