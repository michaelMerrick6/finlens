from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from ingest_house_official import (
    HOUSE_PTR_PDF_URL,
    load_company_lookup,
    prepare_house_trades_for_insert,
    resolve_member_id,
    supabase,
)
from ingest_senate_official import (
    SENATE_REPORT_DATA_URL,
    SENATE_SEARCH_URL,
    load_valid_tickers,
    parse_filed_date,
)
from legacy_congress_guard import require_repair_write_opt_in
from repair_house_filings import load_house_index, replace_house_doc
from repair_senate_filings import (
    create_senate_session,
    load_members_lookup,
    parse_senate_filing,
    replace_senate_doc,
)
from sync_recent_house_filings import parse_house_doc
from time_utils import congress_now


MANUAL_FIXES_PATH = Path("data/house_review_fixes.json")
HOUSE_START_YEAR = 2012
SENATE_START_DATE = "01/01/2012 00:00:00"
DB_RETRY_DELAYS = (2, 5, 10)


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def normalize_ticker(value: object) -> str:
    return normalize_text(value).upper()


def normalize_kind(value: object) -> str:
    return normalize_text(value).lower()


def normalize_date(value: object) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def extract_asset_name(row: dict) -> str:
    for key in ("asset_name", "_asset_name", "_company_name"):
        value = normalize_text(row.get(key))
        if value:
            return value
    return ""


def build_signature(row: dict) -> str:
    return "|".join(
        (
            normalize_ticker(row.get("ticker")),
            normalize_text(extract_asset_name(row)).lower(),
            normalize_kind(row.get("transaction_type")),
            normalize_date(row.get("transaction_date")),
            normalize_date(row.get("published_date")),
            normalize_text(row.get("amount_range")),
        )
    )


def build_counter(rows: list[dict]) -> Counter:
    counter: Counter = Counter()
    for row in rows:
        counter[build_signature(row)] += 1
    return counter


def summarize_counter_diff(source_counter: Counter, db_counter: Counter) -> tuple[int, int, list[dict], list[dict]]:
    missing_rows = 0
    extra_rows = 0
    missing_examples: list[dict] = []
    extra_examples: list[dict] = []

    for signature in sorted(source_counter):
        source_count = source_counter[signature]
        db_count = db_counter.get(signature, 0)
        if source_count > db_count:
            delta = source_count - db_count
            missing_rows += delta
            if len(missing_examples) < 5:
                missing_examples.append({"signature": signature, "count": delta})

    for signature in sorted(db_counter):
        db_count = db_counter[signature]
        source_count = source_counter.get(signature, 0)
        if db_count > source_count:
            delta = db_count - source_count
            extra_rows += delta
            if len(extra_examples) < 5:
                extra_examples.append({"signature": signature, "count": delta})

    return missing_rows, extra_rows, missing_examples, extra_examples


def doc_prefix(doc_id: str) -> str:
    if doc_id.startswith("house-"):
        return "-".join(doc_id.split("-")[:3])
    if doc_id.startswith("senate-"):
        parts = doc_id.split("-")
        if len(parts) > 2 and parts[-1].isdigit():
            return "-".join(parts[:-1])
        return doc_id
    return doc_id


def execute_with_retry(operation, *, label: str):
    attempts = len(DB_RETRY_DELAYS) + 1
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= attempts:
                raise
            delay = DB_RETRY_DELAYS[attempt - 1]
            print(f"[db] RETRY {label}: attempt={attempt} delay={delay}s error={exc}")
            time.sleep(delay)


def fetch_db_rows(prefix: str) -> list[dict]:
    response = execute_with_retry(
        lambda: (
            supabase.table("politician_trades")
            .select("id, doc_id, ticker, asset_name, transaction_type, transaction_date, published_date, amount_range")
            .ilike("doc_id", f"{prefix}%")
            .limit(5000)
            .execute()
        ),
        label=f"fetch_db_rows {prefix}",
    )
    return response.data or []


def load_house_manual_fixes() -> dict[str, dict]:
    if not MANUAL_FIXES_PATH.exists():
        return {}

    payload = json.loads(MANUAL_FIXES_PATH.read_text())
    fixes: dict[str, dict] = {}
    for filing in payload:
        year = int(filing["year"])
        doc_id = str(filing["doc_id"])
        prefix = f"house-{year}-{doc_id}"
        published_date = normalize_date(filing["filing_date"])
        rows = []
        for trade in filing.get("trades") or []:
            rows.append(
                {
                    "ticker": normalize_ticker(trade.get("ticker")),
                    "asset_name": normalize_text(trade.get("asset_name"))[:255],
                    "transaction_type": normalize_kind(trade.get("transaction_type")),
                    "transaction_date": normalize_date(trade.get("transaction_date")),
                    "published_date": published_date,
                    "amount_range": normalize_text(trade.get("amount_range"))[:255],
                }
            )
        fixes[prefix] = {"filing": filing, "rows": rows}
    return fixes


def replace_house_manual_fix(filing: dict, manual_fix: dict, members_db: list[dict]) -> tuple[int, int]:
    year = int(filing["year"])
    doc_id = str(manual_fix["filing"]["doc_id"])
    first_name = str(manual_fix["filing"]["first_name"])
    last_name = str(manual_fix["filing"]["last_name"])
    prefix = f"house-{year}-{doc_id}"
    published_date = normalize_date(manual_fix["filing"]["filing_date"])
    member_id = resolve_member_id(first_name, last_name, members_db)

    trades: list[dict] = []
    for index, trade in enumerate(manual_fix["rows"]):
        trades.append(
            {
                "member_id": member_id,
                "politician_name": f"{first_name} {last_name}"[:100],
                "chamber": "House",
                "party": "Unknown",
                "ticker": normalize_ticker(trade.get("ticker")),
                "transaction_date": normalize_date(trade.get("transaction_date")),
                "published_date": published_date,
                "transaction_type": normalize_kind(trade.get("transaction_type")),
                "asset_type": "Stock",
                "amount_range": normalize_text(trade.get("amount_range"))[:255],
                "source_url": HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id),
                "doc_id": f"{prefix}-{index}",
                "asset_name": normalize_text(trade.get("asset_name"))[:255],
                "_company_name": normalize_text(trade.get("asset_name"))[:255],
            }
        )

    prepared = prepare_house_trades_for_insert(trades)
    existing = execute_with_retry(
        lambda: (
            supabase.table("politician_trades")
            .select("id", count="exact")
            .ilike("doc_id", f"{prefix}%")
            .limit(5000)
            .execute()
        ),
        label=f"count_house_rows {prefix}",
    )
    existing_count = existing.count or 0
    execute_with_retry(
        lambda: supabase.table("politician_trades").delete().ilike("doc_id", f"{prefix}%").execute(),
        label=f"delete_house_rows {prefix}",
    )

    inserted = 0
    for index in range(0, len(prepared), 50):
        chunk = prepared[index : index + 50]
        execute_with_retry(
            lambda chunk=chunk: supabase.table("politician_trades").insert(chunk).execute(),
            label=f"insert_house_rows {prefix} chunk={index//50 + 1}",
        )
        inserted += len(chunk)
    return existing_count, inserted


def iter_house_filings(start_year: int, end_year: int) -> list[tuple[int, str, dict]]:
    filings: list[tuple[int, str, dict]] = []
    for year in range(end_year, start_year - 1, -1):
        year_index = load_house_index(year)
        for source_doc_id, filing in sorted(year_index.items(), key=lambda item: item[0], reverse=True):
            filings.append((year, source_doc_id, filing))
    return filings


def extract_detail_path(link_html: str) -> str:
    href_start = link_html.find('href="')
    if href_start == -1:
        return ""
    href_start += len('href="')
    href_end = link_html.find('"', href_start)
    if href_end == -1:
        return ""
    return link_html[href_start:href_end]


def load_historical_senate_filings(session, *, start_date: str, limit: int = 0) -> list[dict]:
    return load_historical_senate_filings_in_range(session, start_date=start_date, end_date=None, limit=limit)


def load_historical_senate_filings_in_range(
    session,
    *,
    start_date: str,
    end_date: str | None,
    limit: int = 0,
) -> list[dict]:
    filings: list[dict] = []
    seen: set[str] = set()
    end_dt = datetime.strptime(end_date, "%m/%d/%Y %H:%M:%S").date() if end_date else None

    for start_offset in range(0, 10000, 100):
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": start_date,
            "submitted_end_date": end_date or "",
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
        rows = response.json().get("data", [])
        if not rows:
            break

        for row in rows:
            filed_date = parse_filed_date(row[4])
            detail_path = extract_detail_path(str(row[3]))
            if not filed_date or not detail_path:
                continue
            filed_dt = datetime.strptime(filed_date, "%Y-%m-%d").date()
            if end_dt and filed_dt > end_dt:
                continue

            doc_key = detail_path.rstrip("/").split("/")[-1]
            if doc_key in seen:
                continue
            seen.add(doc_key)
            filings.append(
                {
                    "doc_key": doc_key,
                    "politician_name": f"{normalize_text(row[0])} {normalize_text(row[1])}".strip(),
                    "source_url": f"https://efdsearch.senate.gov{detail_path}",
                    "published_date": filed_date,
                }
            )
            if limit > 0 and len(filings) >= limit:
                return filings

        time.sleep(0.5)

    filings.sort(
        key=lambda filing: (
            filing.get("published_date") or "",
            filing.get("doc_key") or "",
        ),
        reverse=True,
    )
    return filings


def audit_house_backlog(
    *,
    start_year: int,
    end_year: int,
    limit: int,
    apply_repairs: bool,
    members_db: list[dict],
    company_lookup: list[dict],
) -> tuple[dict, set[str]]:
    manual_fixes = load_house_manual_fixes()
    filings = iter_house_filings(start_year, end_year)
    if limit > 0:
        filings = filings[:limit]

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "no_trade_filings": 0,
        "manual_fix_filings": 0,
        "parse_failures": [],
        "mismatched_filings": [],
        "applied_repairs": [],
    }
    expected_prefixes: set[str] = set()

    for index, (year, source_doc_id, filing) in enumerate(filings, start=1):
        prefix = f"house-{year}-{source_doc_id}"
        expected_prefixes.add(prefix)
        if index == 1 or index % 50 == 0:
            print(f"[house] {index}/{len(filings)} {prefix}")

        if prefix in manual_fixes:
            source_rows = manual_fixes[prefix]["rows"]
            status = "manual_fix"
            summary["manual_fix_filings"] += 1
        else:
            try:
                status, source_rows = parse_house_doc(
                    {
                        "year": year,
                        "doc_id": source_doc_id,
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
                        "missing_rows": 0,
                        "extra_rows": len(db_rows),
                        "missing_examples": [],
                        "extra_examples": [],
                    }
                )
            continue

        if not source_rows:
            summary["parse_failures"].append({"doc_id": prefix, "error": status})
            continue

        summary["filings_with_trades"] += 1
        db_rows = fetch_db_rows(prefix)
        source_counter = build_counter(source_rows)
        db_counter = build_counter(db_rows)
        missing_rows, extra_rows, missing_examples, extra_examples = summarize_counter_diff(source_counter, db_counter)

        if sum(source_counter.values()) == sum(db_counter.values()) and not missing_rows and not extra_rows:
            continue

        mismatch = {
            "doc_id": prefix,
            "source_rows": sum(source_counter.values()),
            "db_rows": sum(db_counter.values()),
            "missing_rows": missing_rows,
            "extra_rows": extra_rows,
            "missing_examples": missing_examples,
            "extra_examples": extra_examples,
        }
        summary["mismatched_filings"].append(mismatch)
        print(
            f"[house] MISMATCH {prefix}: source_rows={mismatch['source_rows']} "
            f"db_rows={mismatch['db_rows']} missing={missing_rows} extra={extra_rows}"
        )

        if apply_repairs:
            if prefix in manual_fixes:
                replaced, inserted = replace_house_manual_fix(filing, manual_fixes[prefix], members_db)
            else:
                replaced, inserted = replace_house_doc(year, source_doc_id, filing, members_db, company_lookup)
            summary["applied_repairs"].append(
                {"doc_id": prefix, "rows_replaced": replaced, "rows_inserted": inserted}
            )
            print(f"[house] APPLIED {prefix}: replaced={replaced} inserted={inserted}")

    summary["mismatch_count"] = len(summary["mismatched_filings"])
    summary["parse_failure_count"] = len(summary["parse_failures"])
    summary["repair_count"] = len(summary["applied_repairs"])
    return summary, expected_prefixes


def audit_senate_backlog(
    *,
    start_date: str,
    end_date: str | None,
    limit: int,
    apply_repairs: bool,
    members_db: list[dict],
    valid_tickers: set[str],
) -> tuple[dict, set[str]]:
    session = create_senate_session()
    filings = load_historical_senate_filings_in_range(session, start_date=start_date, end_date=end_date, limit=limit)

    summary = {
        "filings_seen": len(filings),
        "filings_with_trades": 0,
        "parse_failures": [],
        "mismatched_filings": [],
        "applied_repairs": [],
    }
    expected_prefixes: set[str] = set()

    for index, filing in enumerate(filings, start=1):
        prefix = f"senate-{filing['doc_key']}"
        expected_prefixes.add(prefix)
        if index == 1 or index % 50 == 0:
            print(f"[senate] {index}/{len(filings)} {prefix}")

        try:
            source_rows = parse_senate_filing(session, filing["doc_key"], filing, members_db, valid_tickers)
        except Exception as exc:
            summary["parse_failures"].append({"doc_id": prefix, "error": str(exc)})
            continue

        if not source_rows:
            summary["parse_failures"].append({"doc_id": prefix, "error": "empty_parse"})
            continue

        summary["filings_with_trades"] += 1
        db_rows = fetch_db_rows(prefix)
        source_counter = build_counter(source_rows)
        db_counter = build_counter(db_rows)
        missing_rows, extra_rows, missing_examples, extra_examples = summarize_counter_diff(source_counter, db_counter)

        if sum(source_counter.values()) == sum(db_counter.values()) and not missing_rows and not extra_rows:
            continue

        mismatch = {
            "doc_id": prefix,
            "source_rows": sum(source_counter.values()),
            "db_rows": sum(db_counter.values()),
            "missing_rows": missing_rows,
            "extra_rows": extra_rows,
            "missing_examples": missing_examples,
            "extra_examples": extra_examples,
        }
        summary["mismatched_filings"].append(mismatch)
        print(
            f"[senate] MISMATCH {prefix}: source_rows={mismatch['source_rows']} "
            f"db_rows={mismatch['db_rows']} missing={missing_rows} extra={extra_rows}"
        )

        if apply_repairs:
            replaced, inserted = replace_senate_doc(session, filing["doc_key"], filing, members_db, valid_tickers)
            summary["applied_repairs"].append(
                {"doc_id": prefix, "rows_replaced": replaced, "rows_inserted": inserted}
            )
            print(f"[senate] APPLIED {prefix}: replaced={replaced} inserted={inserted}")

    summary["mismatch_count"] = len(summary["mismatched_filings"])
    summary["parse_failure_count"] = len(summary["parse_failures"])
    summary["repair_count"] = len(summary["applied_repairs"])
    return summary, expected_prefixes


def collect_db_doc_stats() -> tuple[Counter, Counter]:
    doc_id_counts: Counter = Counter()
    prefix_counts: Counter = Counter()
    offset = 0
    while True:
        response = execute_with_retry(
            lambda: supabase.table("politician_trades").select("doc_id").range(offset, offset + 999).execute(),
            label=f"collect_db_doc_stats offset={offset}",
        )
        rows = response.data or []
        if not rows:
            break

        for row in rows:
            source_doc_id = normalize_text(row.get("doc_id"))
            if not source_doc_id:
                continue
            doc_id_counts[source_doc_id] += 1
            prefix_counts[doc_prefix(source_doc_id)] += 1

        if len(rows) < 1000:
            break
        offset += 1000
    return doc_id_counts, prefix_counts


def summarize_duplicates_and_orphans(
    *,
    house_expected_prefixes: set[str],
    senate_expected_prefixes: set[str],
    house_full_scan: bool,
    senate_full_scan: bool,
) -> dict:
    doc_id_counts, prefix_counts = collect_db_doc_stats()

    exact_duplicate_doc_ids = [
        {"doc_id": source_doc_id, "row_count": count}
        for source_doc_id, count in sorted(doc_id_counts.items())
        if count > 1
    ]

    orphan_house_prefixes: list[dict] = []
    orphan_senate_prefixes: list[dict] = []

    for prefix, count in sorted(prefix_counts.items()):
        if prefix.startswith("house-") and house_full_scan and prefix not in house_expected_prefixes:
            orphan_house_prefixes.append({"doc_id_prefix": prefix, "row_count": count})
        if prefix.startswith("senate-") and senate_full_scan and prefix not in senate_expected_prefixes:
            orphan_senate_prefixes.append({"doc_id_prefix": prefix, "row_count": count})

    return {
        "exact_duplicate_doc_id_count": len(exact_duplicate_doc_ids),
        "exact_duplicate_doc_ids": exact_duplicate_doc_ids[:200],
        "orphan_house_prefix_count": len(orphan_house_prefixes),
        "orphan_house_prefixes": orphan_house_prefixes[:200],
        "orphan_senate_prefix_count": len(orphan_senate_prefixes),
        "orphan_senate_prefixes": orphan_senate_prefixes[:200],
    }


def load_expected_prefixes_only(
    *,
    chamber: str,
    house_start_year: int,
    house_end_year: int,
    senate_start_date: str,
    senate_end_date: str | None,
) -> tuple[set[str], set[str]]:
    house_expected_prefixes: set[str] = set()
    senate_expected_prefixes: set[str] = set()

    if chamber in ("both", "house"):
        for year, source_doc_id, _ in iter_house_filings(house_start_year, house_end_year):
            house_expected_prefixes.add(f"house-{year}-{source_doc_id}")

    if chamber in ("both", "senate"):
        session = create_senate_session()
        filings = load_historical_senate_filings_in_range(
            session,
            start_date=senate_start_date,
            end_date=senate_end_date,
            limit=0,
        )
        for filing in filings:
            senate_expected_prefixes.add(f"senate-{filing['doc_key']}")

    return house_expected_prefixes, senate_expected_prefixes


def main() -> None:
    parser = argparse.ArgumentParser(description="Official-source backlog audit for historical House and Senate politician trades.")
    parser.add_argument("--chamber", choices=("both", "house", "senate"), default="both")
    parser.add_argument("--house-start-year", type=int, default=HOUSE_START_YEAR)
    parser.add_argument("--house-end-year", type=int, default=congress_now().year)
    parser.add_argument("--house-limit", type=int, default=0, help="Optional filing limit for staged House runs")
    parser.add_argument("--senate-start-date", default=SENATE_START_DATE)
    parser.add_argument("--senate-end-date", default=None)
    parser.add_argument("--senate-limit", type=int, default=0, help="Optional filing limit for staged Senate runs")
    parser.add_argument("--apply", action="store_true", help="Repair mismatched filings from the official source")
    parser.add_argument("--skip-duplicate-scan", action="store_true", help="Skip exact duplicate/orphan prefix scan")
    parser.add_argument("--prefix-scan-only", action="store_true", help="Only enumerate expected filing prefixes and scan DB duplicates/orphans")
    parser.add_argument("--artifact", type=Path, default=None, help="Optional JSON artifact path")
    args = parser.parse_args()

    if args.apply:
        require_repair_write_opt_in("audit_historical_congress_backlog.py")

    house_summary = None
    senate_summary = None
    house_expected_prefixes: set[str] = set()
    senate_expected_prefixes: set[str] = set()

    if args.prefix_scan_only:
        house_expected_prefixes, senate_expected_prefixes = load_expected_prefixes_only(
            chamber=args.chamber,
            house_start_year=args.house_start_year,
            house_end_year=args.house_end_year,
            senate_start_date=args.senate_start_date,
            senate_end_date=args.senate_end_date,
        )
    else:
        members_db = load_members_lookup()
        company_lookup = load_company_lookup()
        valid_tickers = load_valid_tickers()

        if args.chamber in ("both", "house"):
            house_summary, house_expected_prefixes = audit_house_backlog(
                start_year=args.house_start_year,
                end_year=args.house_end_year,
                limit=args.house_limit,
                apply_repairs=args.apply,
                members_db=members_db,
                company_lookup=company_lookup,
            )

        if args.chamber in ("both", "senate"):
            senate_summary, senate_expected_prefixes = audit_senate_backlog(
                start_date=args.senate_start_date,
                end_date=args.senate_end_date,
                limit=args.senate_limit,
                apply_repairs=args.apply,
                members_db=members_db,
                valid_tickers=valid_tickers,
            )

    if args.skip_duplicate_scan:
        duplicate_summary = {
            "exact_duplicate_doc_id_count": 0,
            "exact_duplicate_doc_ids": [],
            "orphan_house_prefix_count": 0,
            "orphan_house_prefixes": [],
            "orphan_senate_prefix_count": 0,
            "orphan_senate_prefixes": [],
        }
    else:
        duplicate_summary = summarize_duplicates_and_orphans(
            house_expected_prefixes=house_expected_prefixes,
            senate_expected_prefixes=senate_expected_prefixes,
            house_full_scan=args.chamber in ("both", "house") and args.house_limit == 0,
            senate_full_scan=args.chamber in ("both", "senate") and args.senate_limit == 0,
        )

    parse_failure_count = 0
    mismatch_count = 0
    repair_count = 0
    if house_summary:
        parse_failure_count += house_summary["parse_failure_count"]
        mismatch_count += house_summary["mismatch_count"]
        repair_count += house_summary["repair_count"]
    if senate_summary:
        parse_failure_count += senate_summary["parse_failure_count"]
        mismatch_count += senate_summary["mismatch_count"]
        repair_count += senate_summary["repair_count"]

    summary = {
        "house": house_summary,
        "senate": senate_summary,
        "duplicates": duplicate_summary,
        "parse_failure_count": parse_failure_count,
        "mismatch_count": mismatch_count,
        "repair_count": repair_count,
        "orphan_prefix_count": duplicate_summary["orphan_house_prefix_count"] + duplicate_summary["orphan_senate_prefix_count"],
        "exact_duplicate_doc_id_count": duplicate_summary["exact_duplicate_doc_id_count"],
    }

    if args.artifact:
        args.artifact.parent.mkdir(parents=True, exist_ok=True)
        args.artifact.write_text(json.dumps(summary, indent=2, sort_keys=True))
        print(f"ARTIFACT {args.artifact}")

    print("SUMMARY_JSON:" + json.dumps(summary, sort_keys=True))

    has_fatal_findings = (
        summary["parse_failure_count"]
        or summary["orphan_prefix_count"]
        or summary["exact_duplicate_doc_id_count"]
        or (not args.apply and summary["mismatch_count"])
    )
    if has_fatal_findings:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
