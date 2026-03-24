import argparse
import os
from datetime import datetime, timedelta

from ingest_house_official import load_company_lookup
from ingest_senate_official import load_valid_tickers
from pipeline_support import emit_summary, get_supabase_client
from repair_senate_filings import create_senate_session, load_members_lookup, parse_senate_filing
from sync_recent_house_filings import load_house_index, parse_house_doc
from sync_recent_senate_filings import load_recent_senate_filings
from time_utils import congress_today


HOUSE_AUDIT_DAYS = int(os.environ.get("HOUSE_AUDIT_DAYS", "3"))
HOUSE_AUDIT_LIMIT = int(os.environ.get("HOUSE_AUDIT_LIMIT", "20"))
SENATE_AUDIT_DAYS = int(os.environ.get("SENATE_AUDIT_DAYS", "3"))
SENATE_AUDIT_LIMIT = int(os.environ.get("SENATE_AUDIT_LIMIT", "20"))


def summarize_doc_prefix(doc_id: str) -> str:
    if doc_id.startswith("house-"):
        return "-".join(doc_id.split("-")[:3])
    if doc_id.startswith("senate-") and doc_id.count("-") >= 5:
        return "-".join(doc_id.split("-")[:-1])
    return doc_id


def load_recent_house_filings(*, days: int, limit: int) -> list[dict]:
    today = congress_today()
    cutoff = today - timedelta(days=days)
    years = sorted({today.year, cutoff.year}, reverse=True)
    filings: list[dict] = []

    for year in years:
        for filing in load_house_index(year):
            filed_date = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").date()
            if cutoff <= filed_date <= today:
                filings.append(filing)

    filings.sort(
        key=lambda filing: (
            datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y"),
            int("".join(ch for ch in filing["doc_id"] if ch.isdigit()) or "0"),
        ),
        reverse=True,
    )
    return filings[:limit]


def fetch_doc_rows(supabase, prefix: str) -> tuple[int, list[dict]]:
    response = (
        supabase.table("politician_trades")
        .select("doc_id, member_id, published_date", count="exact")
        .ilike("doc_id", f"{prefix}%")
        .limit(2000)
        .execute()
    )
    return response.count or 0, response.data or []


def audit_house(supabase, *, days: int, limit: int) -> dict:
    members_db = (
        supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    )
    company_lookup = load_company_lookup()
    filings = load_recent_house_filings(days=days, limit=limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "no_trade_filings": 0,
        "source_parse_failures": [],
        "row_count_mismatches": [],
        "published_date_mismatches": [],
        "unexpected_rows_for_no_trade_filings": [],
        "unknown_member_docs": [],
    }

    for filing in filings:
        status, trades = parse_house_doc(filing, members_db, company_lookup)
        prefix = f"house-{filing['year']}-{filing['doc_id']}"
        expected_published_date = datetime.strptime(filing["filing_date_raw"], "%m/%d/%Y").strftime("%Y-%m-%d")
        db_count, db_rows = fetch_doc_rows(supabase, prefix)
        db_dates = sorted({row.get("published_date") for row in db_rows if row.get("published_date")})
        unknown_count = sum(1 for row in db_rows if str(row.get("member_id") or "").startswith("unknown-"))

        if status == "trades":
            summary["filings_with_trades"] += 1
            if db_count != len(trades):
                summary["row_count_mismatches"].append(
                    {"doc_id": prefix, "expected_rows": len(trades), "actual_rows": db_count}
                )
            if db_dates != [expected_published_date]:
                summary["published_date_mismatches"].append(
                    {
                        "doc_id": prefix,
                        "expected_published_date": expected_published_date,
                        "actual_published_dates": db_dates,
                    }
                )
            if unknown_count:
                summary["unknown_member_docs"].append({"doc_id": prefix, "unknown_rows": unknown_count})
        elif status == "no_trade":
            summary["no_trade_filings"] += 1
            if db_count:
                summary["unexpected_rows_for_no_trade_filings"].append({"doc_id": prefix, "actual_rows": db_count})
        else:
            summary["source_parse_failures"].append({"doc_id": prefix, "status": status})

    return summary


def audit_senate(supabase, *, days: int, limit: int) -> dict:
    session = create_senate_session()
    members_db = load_members_lookup()
    valid_tickers = load_valid_tickers()
    filings = load_recent_senate_filings(session, days=days, limit=limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "paper_unmapped_filings": [],
        "source_parse_failures": [],
        "row_count_mismatches": [],
        "published_date_mismatches": [],
        "unknown_member_docs": [],
    }

    for filing in filings:
        prefix = f"senate-{filing['doc_key']}"
        try:
            trades = parse_senate_filing(session, filing["doc_key"], filing, members_db, valid_tickers)
        except Exception as exc:
            if "/search/view/paper/" in filing["source_url"] and "No Senate trades parsed" in str(exc):
                summary["paper_unmapped_filings"].append(prefix)
                continue
            summary["source_parse_failures"].append({"doc_id": prefix, "error": str(exc)})
            continue

        db_count, db_rows = fetch_doc_rows(supabase, prefix)
        db_dates = sorted({row.get("published_date") for row in db_rows if row.get("published_date")})
        unknown_count = sum(1 for row in db_rows if str(row.get("member_id") or "").startswith("unknown-"))
        summary["filings_with_trades"] += 1

        if db_count != len(trades):
            summary["row_count_mismatches"].append(
                {"doc_id": prefix, "expected_rows": len(trades), "actual_rows": db_count}
            )
        if db_dates != [filing["published_date"]]:
            summary["published_date_mismatches"].append(
                {
                    "doc_id": prefix,
                    "expected_published_date": filing["published_date"],
                    "actual_published_dates": db_dates,
                }
            )
        if unknown_count:
            summary["unknown_member_docs"].append({"doc_id": prefix, "unknown_rows": unknown_count})

    return summary


def fetch_recent_unknown_rows(supabase, cutoff_iso: str) -> list[dict]:
    rows = (
        supabase.table("politician_trades")
        .select("doc_id, politician_name, member_id, published_date, chamber")
        .gte("published_date", cutoff_iso)
        .like("member_id", "unknown-%")
        .order("published_date", desc=True)
        .limit(200)
        .execute()
        .data
        or []
    )

    seen_prefixes: set[str] = set()
    summarized: list[dict] = []
    for row in rows:
        prefix = summarize_doc_prefix(row.get("doc_id") or "")
        if prefix in seen_prefixes:
            continue
        seen_prefixes.add(prefix)
        summarized.append(
            {
                "doc_id_prefix": prefix,
                "politician_name": row.get("politician_name"),
                "published_date": row.get("published_date"),
                "chamber": row.get("chamber"),
            }
        )
    return summarized


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit recent official Congress filings against stored rows.")
    parser.add_argument("--house-days", type=int, default=HOUSE_AUDIT_DAYS)
    parser.add_argument("--house-limit", type=int, default=HOUSE_AUDIT_LIMIT)
    parser.add_argument("--senate-days", type=int, default=SENATE_AUDIT_DAYS)
    parser.add_argument("--senate-limit", type=int, default=SENATE_AUDIT_LIMIT)
    args = parser.parse_args()

    supabase = get_supabase_client()
    cutoff = (congress_today() - timedelta(days=max(args.house_days, args.senate_days))).isoformat()

    house_summary = audit_house(supabase, days=args.house_days, limit=args.house_limit)
    senate_summary = audit_senate(supabase, days=args.senate_days, limit=args.senate_limit)
    recent_unknown_rows = fetch_recent_unknown_rows(supabase, cutoff)

    parse_failures = (
        len(house_summary["source_parse_failures"])
        + len(house_summary["row_count_mismatches"])
        + len(house_summary["published_date_mismatches"])
        + len(house_summary["unexpected_rows_for_no_trade_filings"])
        + len(house_summary["unknown_member_docs"])
        + len(senate_summary["source_parse_failures"])
        + len(senate_summary["row_count_mismatches"])
        + len(senate_summary["published_date_mismatches"])
        + len(senate_summary["unknown_member_docs"])
        + len(recent_unknown_rows)
    )

    emit_summary(
        {
            "house": house_summary,
            "senate": senate_summary,
            "recent_unknown_rows": recent_unknown_rows,
            "parse_failures": parse_failures,
        }
    )

    if parse_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
