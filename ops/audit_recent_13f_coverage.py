from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import os

from pipeline_support import emit_summary, get_supabase_client
from ingest_sec_13f import apply_qoq
from sec_13f_support import (
    TRACKED_13F_FUNDS,
    SecTickerResolver,
    create_13f_session,
    is_unbounded_limit,
    load_company_reference,
    load_available_13f_filings,
    parse_13f_filing,
)

MAX_FILINGS_PER_FUND = int(os.environ.get("SEC_13F_MAX_FILINGS_PER_FUND", "12"))
MAX_DISTINCT_PERIODS_PER_FUND = int(os.environ.get("SEC_13F_MAX_PERIODS_PER_FUND", "8"))
MIN_RESOLUTION_RATIO = float(os.environ.get("SEC_13F_MIN_RESOLUTION_RATIO", "0.55"))


def select_recent_distinct_periods(parsed_filings: list[dict], max_distinct_periods: int) -> list[dict]:
    latest_by_period: dict[str, dict] = {}
    for parsed in parsed_filings:
        report_period = str(parsed.get("report_period") or "")
        existing = latest_by_period.get(report_period)
        if not existing or str(parsed.get("published_date") or "") > str(existing.get("published_date") or ""):
            latest_by_period[report_period] = parsed
    periods = sorted(latest_by_period.values(), key=lambda parsed: str(parsed.get("report_period") or ""))
    if is_unbounded_limit(max_distinct_periods):
        return periods
    return periods[-max_distinct_periods:]


def db_row_key(row: dict) -> tuple[str, int, int]:
    return (
        str(row.get("ticker") or "").upper(),
        int(row.get("shares_held") or 0),
        int(row.get("value_held") or 0),
    )


def load_db_holdings(supabase, fund_name: str, report_period: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase.table("institutional_holdings")
            .select("ticker, shares_held, value_held")
            .eq("fund_name", fund_name)
            .eq("report_period", report_period)
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


def main() -> None:
    print("Auditing tracked SEC 13F coverage...")
    supabase = get_supabase_client()
    session = create_13f_session()
    resolver = SecTickerResolver(load_company_reference(session))

    funds_seen = 0
    filings_seen = 0
    matched_periods = 0
    unresolved_rows = 0
    parse_failures = 0
    mismatches: list[dict] = []

    for fund in TRACKED_13F_FUNDS:
        funds_seen += 1
        parsed_by_period: dict[str, dict] = {}
        for filing in load_available_13f_filings(session, fund, max_filings=MAX_FILINGS_PER_FUND):
            filings_seen += 1
            parsed = parse_13f_filing(session, filing, resolver)
            if not parsed:
                parse_failures += 1
                continue
            supported = int(parsed.get("rows_supported") or 0)
            resolved = int(parsed.get("rows_resolved") or 0)
            resolution_ratio = (resolved / supported) if supported else 0.0
            if supported <= 0 or resolution_ratio < MIN_RESOLUTION_RATIO:
                parse_failures += 1
                continue
            report_period = str(parsed.get("report_period") or "")
            existing = parsed_by_period.get(report_period)
            if not existing or str(parsed.get("published_date") or "") > str(existing.get("published_date") or ""):
                parsed_by_period[report_period] = parsed
            if not is_unbounded_limit(MAX_DISTINCT_PERIODS_PER_FUND) and len(parsed_by_period) >= MAX_DISTINCT_PERIODS_PER_FUND:
                break

        parsed_periods = select_recent_distinct_periods(list(parsed_by_period.values()), MAX_DISTINCT_PERIODS_PER_FUND)
        for index, parsed in enumerate(parsed_periods):
            current_holdings = parsed.get("holdings") or []
            previous_holdings = parsed_periods[index - 1].get("holdings") or [] if index > 0 else []
            compared_holdings = apply_qoq(current_holdings, previous_holdings)
            resolved_holdings = [holding for holding in compared_holdings if holding.get("ticker")]
            unresolved_rows += int(parsed.get("rows_unresolved") or 0)
            expected = {db_row_key(row) for row in resolved_holdings}
            actual = {db_row_key(row) for row in load_db_holdings(supabase, fund["name"], parsed["report_period"])}
            if expected == actual and actual:
                matched_periods += 1
                continue
            mismatches.append(
                {
                    "fund_name": fund["name"],
                    "report_period": parsed["report_period"],
                    "expected_rows": len(expected),
                    "db_rows": len(actual),
                }
            )

    emit_summary(
        {
            "funds_seen": funds_seen,
            "filings_seen": filings_seen,
            "matched_periods": matched_periods,
            "mismatches": mismatches[:20],
            "unresolved_rows": unresolved_rows,
            "parse_failures": parse_failures + len(mismatches),
        }
    )


if __name__ == "__main__":
    main()
