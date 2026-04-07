import argparse
import json
import os
from datetime import datetime, timedelta

from ingest_senate_official import SENATE_REPORT_DATA_URL, SENATE_SEARCH_URL, load_valid_tickers, parse_filed_date
from repair_senate_filings import (
    create_senate_session,
    load_members_lookup,
    replace_senate_doc,
)
from time_utils import congress_today


RECENT_DAYS = int(os.environ.get("SENATE_RECENT_SYNC_DAYS", "30"))
MAX_FILINGS = int(os.environ.get("SENATE_RECENT_SYNC_LIMIT", "100"))


def load_recent_senate_filings(session, *, days: int, limit: int) -> list[dict]:
    cutoff = congress_today() - timedelta(days=days)
    filings: list[dict] = []
    seen: set[str] = set()

    for start_offset in range(0, 500, 100):
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": cutoff.strftime("%m/%d/%Y 00:00:00"),
            "submitted_end_date": "",
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
            "csrfmiddlewaretoken": session.cookies.get("csrftoken") or session.cookies.get("csrf") or "",
        }
        response = session.post(
            SENATE_REPORT_DATA_URL,
            data=payload,
            headers={"Referer": SENATE_SEARCH_URL},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        rows = data.get("data", [])
        if not rows:
            break

        for row in rows:
            first_name = str(row[0]).strip()
            last_name = str(row[1]).strip()
            filed_date = parse_filed_date(row[4])
            if not filed_date:
                continue
            filed_dt = datetime.strptime(filed_date, "%Y-%m-%d").date()
            if filed_dt < cutoff:
                continue
            link_html = str(row[3])
            href_start = link_html.find('href="')
            if href_start == -1:
                continue
            href_start += len('href="')
            href_end = link_html.find('"', href_start)
            detail_path = link_html[href_start:href_end]
            doc_key = detail_path.rstrip("/").split("/")[-1]
            if doc_key in seen:
                continue
            seen.add(doc_key)
            filings.append(
                {
                    "doc_key": doc_key,
                    "politician_name": f"{first_name} {last_name}".strip(),
                    "source_url": f"https://efdsearch.senate.gov{detail_path}",
                    "published_date": filed_date,
                }
            )
            if len(filings) >= limit:
                return filings

    return filings


def main() -> None:
    parser = argparse.ArgumentParser(description="Replace the most recent Senate PTR filings from the official feed.")
    parser.add_argument("--days", type=int, default=RECENT_DAYS)
    parser.add_argument("--limit", type=int, default=MAX_FILINGS)
    args = parser.parse_args()

    session = create_senate_session()
    members_db = load_members_lookup()
    valid_tickers = load_valid_tickers()
    filings = load_recent_senate_filings(session, days=args.days, limit=args.limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "paper_unmapped_filings": 0,
        "rows_replaced": 0,
        "rows_inserted": 0,
        "failed_doc_ids": [],
    }

    for filing in filings:
        try:
            existing_count, inserted_count = replace_senate_doc(
                session,
                filing["doc_key"],
                filing,
                members_db,
                valid_tickers,
            )
        except Exception as exc:
            if "/search/view/paper/" in filing["source_url"] and "No Senate trades parsed" in str(exc):
                summary["paper_unmapped_filings"] += 1
                print(f"Synced senate:{filing['doc_key']} {filing['published_date']} status=paper-unmapped")
                continue
            print(f"Failed senate:{filing['doc_key']} {filing['published_date']} error={exc}")
            summary["failed_doc_ids"].append(filing["doc_key"])
            continue

        summary["filings_with_trades"] += 1
        summary["rows_replaced"] += existing_count
        summary["rows_inserted"] += inserted_count
        print(
            f"Synced senate:{filing['doc_key']} {filing['published_date']} "
            f"replaced={existing_count} inserted={inserted_count}"
        )

    summary["parse_failures"] = len(summary["failed_doc_ids"])
    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))

    if summary["failed_doc_ids"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
