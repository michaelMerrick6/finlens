import argparse
import json
import re
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from ingest_senate_official import SENATE_REPORT_DATA_URL, SENATE_SEARCH_URL, load_valid_tickers, parse_filed_date

ROOT_DIR = Path(__file__).resolve().parent.parent
OPS_DIR = ROOT_DIR / "ops"
if str(OPS_DIR) not in sys.path:
    sys.path.insert(0, str(OPS_DIR))

from repair_senate_filings import (
    create_senate_session,
    load_members_lookup,
    replace_senate_doc,
)
from time_utils import congress_today


RECENT_DAYS = int(os.environ.get("SENATE_RECENT_SYNC_DAYS", "30"))
MAX_FILINGS = int(os.environ.get("SENATE_RECENT_SYNC_LIMIT", "100"))


class SenatePaginationError(ValueError):
    pass


def load_recent_senate_filings(session, *, days: int, limit: int | None, end_date=None) -> list[dict]:
    cutoff = congress_today() - timedelta(days=days)
    filings: list[dict] = []
    seen: set[str] = set()

    start_offset = 0
    expected_total = None
    while True:
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": cutoff.strftime("%m/%d/%Y 00:00:00"),
            "submitted_end_date": end_date.strftime("%m/%d/%Y 23:59:59") if end_date else "",
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
        total = data.get("recordsFiltered")
        if not isinstance(total, int) or total < 0:
            raise RuntimeError("Senate feed omitted its filing total")
        if expected_total is not None and total != expected_total:
            raise RuntimeError("Senate inventory changed during pagination; retry discovery")
        expected_total = total
        rows = data.get("data", [])
        if not rows:
            if len(seen) != total:
                raise RuntimeError("Senate feed ended before its reported total")
            break

        for row in rows:
            first_name = str(row[0]).strip()
            last_name = str(row[1]).strip()
            filed_date = parse_filed_date(row[4])
            if not filed_date:
                raise ValueError("Invalid Senate filing date")
            filed_dt = datetime.strptime(filed_date, "%Y-%m-%d").date()
            if filed_dt < cutoff or (end_date and filed_dt > end_date):
                raise ValueError("Senate filing outside requested window")
            link_html = str(row[3])
            match = re.search(r'href=[\'"]([^\'"]+)[\'"]', link_html)
            if not match:
                raise ValueError("Senate filing has no document link")
            detail_path = match.group(1)
            if not re.fullmatch(r"/search/view/(ptr|paper)/[a-fA-F0-9-]+/", detail_path):
                raise ValueError("Unrecognized Senate document URL")
            doc_key = detail_path.rstrip("/").split("/")[-1].lower()
            if doc_key in seen:
                raise SenatePaginationError("Duplicate Senate filing during pagination")
            seen.add(doc_key)
            filings.append(
                {
                    "doc_key": doc_key,
                    "politician_name": f"{first_name} {last_name}".strip(),
                    "source_url": f"https://efdsearch.senate.gov{detail_path}",
                    "published_date": filed_date,
                }
            )
            if limit is not None and len(filings) >= limit:
                return filings
        start_offset += len(rows)
        if len(seen) == expected_total:
            break
        if len(seen) > expected_total:
            raise RuntimeError("Senate filing count exceeds reported total")

    return filings


def load_senate_interval(session, start_date, end_date):
    """Split unstable page boundaries without silently accepting duplicate/missing rows."""
    try:
        return load_recent_senate_filings(session, days=(congress_today() - start_date).days,
                                         limit=None, end_date=end_date)
    except SenatePaginationError:
        if start_date >= end_date:
            raise
        midpoint = start_date + (end_date - start_date) // 2
        return (load_senate_interval(session, start_date, midpoint)
                + load_senate_interval(session, midpoint + timedelta(days=1), end_date))


def main() -> None:
    from capture_congress import main as capture
    capture(chamber="Senate")


if __name__ == "__main__":
    main()
