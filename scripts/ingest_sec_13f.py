import os

from pipeline_support import emit_summary, get_supabase_client
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


def _holding_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def apply_qoq(current_holdings: list[dict], previous_holdings: list[dict]) -> list[dict]:
    previous_by_ticker = {
        str(row.get("ticker") or "").upper(): row
        for row in previous_holdings
        if row.get("ticker")
    }
    if not previous_by_ticker:
        for holding in current_holdings:
            holding["qoq_change_shares"] = None
            holding["qoq_change_percent"] = None
        return current_holdings

    current_by_ticker = {
        str(row.get("ticker") or "").upper(): row
        for row in current_holdings
        if row.get("ticker")
    }

    for holding in current_holdings:
        ticker = str(holding.get("ticker") or "").upper()
        if not ticker:
            holding["qoq_change_shares"] = None
            holding["qoq_change_percent"] = None
            continue
        previous = previous_by_ticker.get(ticker)
        if not previous:
            holding["qoq_change_shares"] = _holding_int(holding.get("shares_held"))
            holding["qoq_change_percent"] = None
            continue
        previous_shares = _holding_int(previous.get("shares_held"))
        current_shares = _holding_int(holding.get("shares_held"))
        change = current_shares - previous_shares
        holding["qoq_change_shares"] = change
        holding["qoq_change_percent"] = round((change / previous_shares) * 100, 2) if previous_shares else None

    if not current_holdings:
        return current_holdings

    current_template = current_holdings[0]
    compared_holdings = list(current_holdings)
    for ticker, previous in previous_by_ticker.items():
        if ticker in current_by_ticker:
            continue
        previous_shares = _holding_int(previous.get("shares_held"))
        if previous_shares <= 0:
            continue
        compared_holdings.append(
            {
                "fund_name": current_template["fund_name"],
                "ticker": previous.get("ticker"),
                "report_period": current_template["report_period"],
                "published_date": current_template["published_date"],
                "shares_held": 0,
                "value_held": 0,
                "qoq_change_shares": -previous_shares,
                "qoq_change_percent": -100.0,
                "source_url": current_template["source_url"],
                "_issuer_name": previous.get("_issuer_name") or previous.get("ticker"),
                "_title_of_class": previous.get("_title_of_class"),
                "_cusip": previous.get("_cusip"),
                "_share_class": previous.get("_share_class"),
                "_asset_key": previous.get("_asset_key") or ticker,
                "_accession": current_template.get("_accession"),
            }
        )
    return compared_holdings


def upsert_companies(supabase, holdings: list[dict]) -> None:
    company_rows: dict[str, dict] = {}
    for holding in holdings:
        ticker = str(holding.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        company_rows[ticker] = {
            "ticker": ticker[:10],
            "name": str(holding.get("_issuer_name") or ticker).strip()[:255] or ticker,
            "sector": "Institutional Holding",
            "industry": "13F Filing",
        }
    if company_rows:
        supabase.table("companies").upsert(list(company_rows.values()), on_conflict="ticker").execute()


def replace_holdings_for_period(supabase, fund_name: str, report_period: str, holdings: list[dict]) -> int:
    supabase.table("institutional_holdings").delete().eq("fund_name", fund_name).eq("report_period", report_period).execute()
    published_date = str(holdings[0].get("published_date") or "").strip() if holdings else ""
    if published_date:
        supabase.table("institutional_holdings").delete().eq("fund_name", fund_name).eq("published_date", published_date).execute()
    insert_rows = [
        {
            "fund_name": holding["fund_name"],
            "ticker": holding.get("ticker"),
            "report_period": holding["report_period"],
            "published_date": holding["published_date"],
            "shares_held": holding["shares_held"],
            "value_held": holding["value_held"],
            "qoq_change_shares": holding.get("qoq_change_shares"),
            "qoq_change_percent": holding.get("qoq_change_percent"),
            "source_url": holding["source_url"],
        }
        for holding in holdings
        if holding.get("ticker")
    ]
    if not insert_rows:
        return 0
    for index in range(0, len(insert_rows), 200):
        supabase.table("institutional_holdings").insert(insert_rows[index : index + 200]).execute()
    return len(insert_rows)


def main() -> None:
    print("Starting SEC 13F-HR sync...")
    supabase = get_supabase_client()
    session = create_13f_session()
    resolver = SecTickerResolver(load_company_reference(session))

    funds_seen = 0
    filings_seen = 0
    records_seen = 0
    records_inserted = 0
    unresolved_rows = 0
    unsupported_rows = 0
    parse_failures = 0
    failed_funds: list[str] = []

    for fund in TRACKED_13F_FUNDS:
        funds_seen += 1
        fund_name = fund["name"]
        fund_filings = load_available_13f_filings(session, fund, max_filings=MAX_FILINGS_PER_FUND)
        parsed_by_period: dict[str, dict] = {}

        for filing in fund_filings:
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
        if not parsed_periods:
            failed_funds.append(fund_name)
            continue

        for index, parsed in enumerate(parsed_periods):
            current_holdings = parsed.get("holdings") or []
            previous_holdings = parsed_periods[index - 1].get("holdings") or [] if index > 0 else []
            compared_holdings = apply_qoq(current_holdings, previous_holdings)

            records_seen += len(compared_holdings)
            unresolved_rows += int(parsed.get("rows_unresolved") or 0)
            unsupported_rows += int(parsed.get("rows_skipped") or 0)

            resolved_holdings = [holding for holding in compared_holdings if holding.get("ticker")]
            if not resolved_holdings:
                continue
            upsert_companies(supabase, resolved_holdings)
            records_inserted += replace_holdings_for_period(
                supabase,
                fund_name,
                parsed["report_period"],
                resolved_holdings,
            )

    summary = {
        "funds_seen": funds_seen,
        "filings_seen": filings_seen,
        "records_seen": records_seen,
        "records_inserted": records_inserted,
        "records_skipped": unresolved_rows + unsupported_rows,
        "unresolved_rows": unresolved_rows,
        "unsupported_rows": unsupported_rows,
        "parse_failures": parse_failures,
        "failed_funds": failed_funds,
    }
    emit_summary(summary)


if __name__ == "__main__":
    main()
