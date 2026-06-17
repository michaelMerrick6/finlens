from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
from pathlib import Path

from ingest_house_official import load_company_lookup, supabase
from repair_house_filings import load_house_index, replace_house_doc
from sync_recent_house_filings import parse_house_doc
from time_utils import congress_now


MANUAL_FIXES_PATH = Path("data/house_review_fixes.json")


def build_signature(row: dict) -> str:
    return "|".join(
        (
            str(row.get("ticker") or "").upper(),
            str(row.get("transaction_type") or "").lower(),
            str(row.get("transaction_date") or ""),
            str(row.get("published_date") or ""),
            str(row.get("amount_range") or ""),
        )
    )


def fetch_db_rows(prefix: str) -> list[dict]:
    response = (
        supabase.table("politician_trades")
        .select("ticker, transaction_type, transaction_date, published_date, amount_range, doc_id")
        .ilike("doc_id", f"{prefix}%")
        .limit(2000)
        .execute()
    )
    return response.data or []


def load_manual_fix_counts() -> dict[str, int]:
    if not MANUAL_FIXES_PATH.exists():
        return {}
    payload = json.loads(MANUAL_FIXES_PATH.read_text())
    counts: dict[str, int] = {}
    for filing in payload:
        prefix = f"house-{int(filing['year'])}-{filing['doc_id']}"
        counts[prefix] = len(filing.get("trades") or [])
    return counts


def iter_house_filings(start_year: int, end_year: int) -> list[tuple[int, str, dict]]:
    filings: list[tuple[int, str, dict]] = []
    for year in range(end_year, start_year - 1, -1):
        year_index = load_house_index(year)
        for doc_id, filing in sorted(year_index.items(), key=lambda item: item[0], reverse=True):
            filings.append((year, doc_id, filing))
    return filings


def parse_target(value: str) -> tuple[int, str]:
    year_raw, _, doc_id = value.partition(":")
    if not year_raw or not doc_id:
        raise argparse.ArgumentTypeError("Expected YEAR:DOC_ID")
    return int(year_raw), doc_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit historical official House PTR filings against politician_trades.")
    parser.add_argument("--start-year", type=int, default=2013)
    parser.add_argument("--end-year", type=int, default=congress_now().year)
    parser.add_argument("--limit", type=int, default=0, help="Optional filing limit for staged runs")
    parser.add_argument("--target", action="append", type=parse_target, default=[], help="Specific YEAR:DOC_ID filing")
    parser.add_argument("--apply", action="store_true", help="Replace mismatched filings with the official parse")
    args = parser.parse_args()

    members_db = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    company_lookup = load_company_lookup()
    if args.target:
        index_cache: dict[int, dict[str, dict]] = {}
        filings = []
        for year, doc_id in args.target:
            if year not in index_cache:
                index_cache[year] = load_house_index(year)
            filing = index_cache[year].get(doc_id)
            if not filing:
                raise RuntimeError(f"Could not find {year}:{doc_id} in the House index")
            filings.append((year, doc_id, filing))
    else:
        filings = iter_house_filings(args.start_year, args.end_year)
    if args.limit > 0:
        filings = filings[: args.limit]

    summary = {
        "filings_seen": len(filings),
        "no_trade_filings": 0,
        "manual_fix_filings": 0,
        "parse_failures": [],
        "mismatched_filings": [],
        "applied_repairs": [],
    }
    manual_fix_counts = load_manual_fix_counts()

    for year, doc_id, filing in filings:
        prefix = f"house-{year}-{doc_id}"
        try:
            status, source_rows = parse_house_doc(
                {
                    "year": year,
                    "doc_id": doc_id,
                    "first_name": filing["first_name"],
                    "last_name": filing["last_name"],
                    "filing_date_raw": filing["filing_date"],
                },
                members_db,
                company_lookup,
            )
        except Exception as exc:
            summary["parse_failures"].append({"doc_id": prefix, "error": str(exc)})
            continue

        if status == "no_trade":
            summary["no_trade_filings"] += 1
            db_rows = fetch_db_rows(prefix)
            if db_rows:
                summary["mismatched_filings"].append(
                    {
                        "doc_id": prefix,
                        "source_rows": 0,
                        "db_rows": len(db_rows),
                        "missing_signatures": 0,
                        "extra_signatures": len(db_rows),
                    }
                )
            continue

        if not source_rows:
            if prefix in manual_fix_counts:
                db_rows = fetch_db_rows(prefix)
                if len(db_rows) == manual_fix_counts[prefix]:
                    summary["manual_fix_filings"] += 1
                    continue
                summary["mismatched_filings"].append(
                    {
                        "doc_id": prefix,
                        "source_rows": 0,
                        "db_rows": len(db_rows),
                        "missing_signatures": manual_fix_counts[prefix],
                        "extra_signatures": max(len(db_rows) - manual_fix_counts[prefix], 0),
                    }
                )
                continue
            summary["parse_failures"].append({"doc_id": prefix, "error": status})
            continue

        db_rows = fetch_db_rows(prefix)
        source_signatures = {build_signature(row) for row in source_rows}
        db_signatures = {build_signature(row) for row in db_rows}
        missing_signatures = sorted(source_signatures - db_signatures)
        extra_signatures = sorted(db_signatures - source_signatures)

        if len(source_rows) == len(db_rows) and not missing_signatures and not extra_signatures:
            continue

        mismatch = {
            "doc_id": prefix,
            "source_rows": len(source_rows),
            "db_rows": len(db_rows),
            "missing_signatures": len(missing_signatures),
            "extra_signatures": len(extra_signatures),
        }
        summary["mismatched_filings"].append(mismatch)
        print(
            f"MISMATCH {prefix}: source_rows={len(source_rows)} db_rows={len(db_rows)} "
            f"missing={len(missing_signatures)} extra={len(extra_signatures)}"
        )

        if args.apply:
            replaced, inserted = replace_house_doc(year, doc_id, filing, members_db, company_lookup)
            repair_summary = {"doc_id": prefix, "rows_replaced": replaced, "rows_inserted": inserted}
            summary["applied_repairs"].append(repair_summary)
            print(f"APPLIED {prefix}: replaced={replaced} inserted={inserted}")

    summary["mismatch_count"] = len(summary["mismatched_filings"])
    summary["parse_failure_count"] = len(summary["parse_failures"])
    summary["repair_count"] = len(summary["applied_repairs"])
    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))

    if summary["parse_failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
