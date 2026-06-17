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
    detect_house_non_public_only_lines,
    extract_best_text_transactions,
    extract_ocr_lines,
    extract_transactions_from_lines,
    extract_transactions_from_scanned_house_pdf,
    load_company_lookup,
    normalize_line,
    prepare_house_trades_for_insert,
    supabase,
)
from time_utils import congress_today


RECENT_DAYS = int(os.environ.get("HOUSE_RECENT_SYNC_DAYS", "14"))
MAX_FILINGS = int(os.environ.get("HOUSE_RECENT_SYNC_LIMIT", "100"))
MAX_PARSE_FAILURES = int(os.environ.get("HOUSE_RECENT_SYNC_MAX_PARSE_FAILURES", "3"))
CARRYOVER_DAYS = int(os.environ.get("HOUSE_RECENT_SYNC_CARRYOVER_DAYS", "60"))
CARRYOVER_RUN_LIMIT = int(os.environ.get("HOUSE_RECENT_SYNC_CARRYOVER_RUN_LIMIT", "120"))
CARRYOVER_DOC_LIMIT = int(os.environ.get("HOUSE_RECENT_SYNC_CARRYOVER_DOC_LIMIT", "40"))


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


def load_recent_house_filings(*, days: int, limit: int) -> list[dict]:
    today = congress_today()
    cutoff = today - timedelta(days=days)
    years = sorted({today.year, cutoff.year}, reverse=True)
    filings: list[dict] = []

    for year in years:
        filings.extend(
            filing
            for filing in load_house_index(year)
            if datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").date() >= cutoff
        )

    filings.sort(
        key=lambda filing: (
            datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y"),
            int("".join(ch for ch in filing["doc_id"] if ch.isdigit()) or "0"),
        ),
        reverse=True,
    )
    return filings[:limit]


def parse_house_failed_doc_id(value: str) -> tuple[int, str] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    year_part, _, doc_id = raw.partition("-")
    if not year_part.isdigit() or not doc_id:
        return None
    return int(year_part), doc_id


def dedupe_house_filings(*groups: list[dict]) -> list[dict]:
    merged: dict[tuple[int, str], dict] = {}
    for filings in groups:
        for filing in filings:
            key = (int(filing["year"]), str(filing["doc_id"]))
            merged[key] = filing

    deduped = list(merged.values())
    deduped.sort(
        key=lambda filing: (
            datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y"),
            int("".join(ch for ch in filing["doc_id"] if ch.isdigit()) or "0"),
        ),
        reverse=True,
    )
    return deduped


def extract_failed_house_doc_ids(run_rows: list[dict], *, doc_limit: int) -> list[tuple[int, str]]:
    seen: set[tuple[int, str]] = set()
    failed_docs: list[tuple[int, str]] = []
    for row in run_rows:
        metadata = row.get("run_metadata") or {}
        for key in ("failed_doc_ids", "carryover_failed_doc_ids"):
            for raw_doc_id in metadata.get(key) or []:
                parsed = parse_house_failed_doc_id(raw_doc_id)
                if not parsed or parsed in seen:
                    continue
                seen.add(parsed)
                failed_docs.append(parsed)
                if len(failed_docs) >= doc_limit:
                    return failed_docs
    return failed_docs


def load_house_filings_by_doc_ids(targets: list[tuple[int, str]]) -> list[dict]:
    if not targets:
        return []

    docs_by_year: dict[int, set[str]] = {}
    for year, doc_id in targets:
        docs_by_year.setdefault(year, set()).add(doc_id)

    filings: list[dict] = []
    for year, doc_ids in docs_by_year.items():
        for filing in load_house_index(year):
            if filing["doc_id"] in doc_ids:
                filings.append(filing)
    return filings


def load_recent_house_carryover_filings(*, days: int, run_limit: int, doc_limit: int) -> list[dict]:
    cutoff = (congress_today() - timedelta(days=days)).isoformat()
    rows = (
        supabase.table("scraper_runs")
        .select("started_at, run_metadata")
        .eq("scraper_name", "house_official_daily")
        .gte("started_at", cutoff)
        .order("started_at", desc=True)
        .limit(run_limit)
        .execute()
        .data
        or []
    )
    failed_docs = extract_failed_house_doc_ids(rows, doc_limit=doc_limit)
    return load_house_filings_by_doc_ids(failed_docs)


def parse_house_doc(filing: dict, members_db: list[dict], company_lookup: list[dict]) -> tuple[str, list[dict]]:
    year = filing["year"]
    doc_id = filing["doc_id"]
    pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
    pdf_resp = requests.get(pdf_url, timeout=(10, 60))
    pdf_resp.raise_for_status()

    transactions, pdf_lines = extract_best_text_transactions(
        pdf_resp.content,
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

        if not transactions and status == "unparsed":
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
                elif detect_house_non_public_only_lines(ocr_lines):
                    status = "no_trade"

    published_date = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").strftime("%Y-%m-%d")
    for transaction in transactions:
        # The House index filing date is the authoritative "filed" date. Parsed PDFs
        # often expose transaction/notification dates, which must not replace it.
        transaction["published_date"] = published_date

    return status, transactions


def ensure_referenced_companies(prepared_trades: list[dict]) -> None:
    """Protect ingestion from unknown tickers that would violate the FK on politician_trades."""
    tickers = sorted(
        {
            str(trade.get("ticker") or "").strip().upper()[:10]
            for trade in prepared_trades
            if str(trade.get("ticker") or "").strip().upper() not in {"", "N/A"}
        }
    )
    if not tickers:
        return

    existing: set[str] = set()
    for index in range(0, len(tickers), 100):
        chunk = tickers[index : index + 100]
        response = supabase.table("companies").select("ticker").in_("ticker", chunk).execute()
        existing.update(str(row.get("ticker") or "").upper() for row in (response.data or []))

    missing = [ticker for ticker in tickers if ticker not in existing]
    if not missing:
        return

    asset_names_by_ticker: dict[str, str] = {}
    for trade in prepared_trades:
        ticker = str(trade.get("ticker") or "").strip().upper()[:10]
        if ticker not in missing or ticker in asset_names_by_ticker:
            continue
        asset_name = str(trade.get("asset_name") or "").strip()
        asset_names_by_ticker[ticker] = asset_name[:255] if asset_name else ticker

    rows = [
        {
            "ticker": ticker,
            "name": asset_names_by_ticker.get(ticker) or ticker,
            "sector": "Unknown",
            "industry": "Unknown",
        }
        for ticker in missing
    ]
    supabase.table("companies").upsert(rows, on_conflict="ticker").execute()
    print(f"Registered {len(rows)} missing company ticker(s): {', '.join(missing[:10])}")


def replace_doc_rows(filing: dict, trades: list[dict]) -> tuple[int, int]:
    prepared_trades = prepare_house_trades_for_insert(trades)
    ensure_referenced_companies(prepared_trades)
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
    parser.add_argument("--max-parse-failures", type=int, default=MAX_PARSE_FAILURES)
    parser.add_argument("--carryover-days", type=int, default=CARRYOVER_DAYS)
    parser.add_argument("--carryover-run-limit", type=int, default=CARRYOVER_RUN_LIMIT)
    parser.add_argument("--carryover-doc-limit", type=int, default=CARRYOVER_DOC_LIMIT)
    args = parser.parse_args()

    recent_filings = load_recent_house_filings(days=args.days, limit=args.limit)
    carryover_filings = load_recent_house_carryover_filings(
        days=args.carryover_days,
        run_limit=args.carryover_run_limit,
        doc_limit=args.carryover_doc_limit,
    )
    filings = dedupe_house_filings(recent_filings, carryover_filings)

    members_req = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute()
    members_db = members_req.data if members_req else []
    company_lookup = load_company_lookup()

    summary = {
        "filings_seen": len(filings),
        "recent_filings_seen": len(recent_filings),
        "carryover_filings_seen": len(carryover_filings),
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

    if summary["failed_doc_ids"] and (
        summary["parse_failures"] > args.max_parse_failures or summary["rows_inserted"] == 0
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
