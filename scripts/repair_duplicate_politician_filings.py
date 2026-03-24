import argparse
import json
from collections import Counter, defaultdict

from legacy_congress_guard import require_repair_write_opt_in
from repair_house_filings import load_house_index, replace_house_doc
from repair_senate_filings import (
    create_senate_session,
    load_existing_senate_filing,
    load_members_lookup,
    replace_senate_doc,
)
from ingest_house_official import load_company_lookup, supabase as house_supabase
from ingest_senate_official import load_valid_tickers


def load_doc_id_counts() -> Counter:
    counts: Counter = Counter()
    offset = 0
    while True:
        rows = (
            house_supabase.table("politician_trades")
            .select("doc_id")
            .range(offset, offset + 999)
            .execute()
            .data
            or []
        )
        for row in rows:
            doc_id = row.get("doc_id")
            if doc_id:
                counts[doc_id] += 1
        if len(rows) < 1000:
            break
        offset += 1000
    return counts


def find_duplicate_prefixes(counts: Counter) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    house_prefix_dups: defaultdict[str, int] = defaultdict(int)
    senate_prefix_dups: defaultdict[str, int] = defaultdict(int)

    for doc_id, count in counts.items():
        if count <= 1:
            continue
        if doc_id.startswith("house-"):
            parts = doc_id.split("-")
            prefix = "-".join(parts[:3]) if len(parts) >= 3 else doc_id
            house_prefix_dups[prefix] += count - 1
        elif doc_id.startswith("senate-"):
            parts = doc_id.split("-")
            prefix = "-".join(parts[:6]) if len(parts) >= 6 else doc_id
            senate_prefix_dups[prefix] += count - 1

    house_items = sorted(house_prefix_dups.items(), key=lambda item: item[1], reverse=True)
    senate_items = sorted(senate_prefix_dups.items(), key=lambda item: item[1], reverse=True)
    return house_items, senate_items


def parse_house_prefix(prefix: str) -> tuple[int, str]:
    _, year_raw, doc_id = prefix.split("-", 2)
    return int(year_raw), doc_id


def parse_senate_prefix(prefix: str) -> str:
    return prefix.removeprefix("senate-")


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair duplicate politician filing prefixes from source.")
    parser.add_argument("--house-limit", type=int, default=0, help="Repair top N duplicate House filing prefixes")
    parser.add_argument("--senate-limit", type=int, default=0, help="Repair top N duplicate Senate filing prefixes")
    parser.add_argument("--house-year-min", type=int, default=None, help="Only repair House prefixes for years >= this value")
    parser.add_argument("--senate-prefix", action="append", default=[], help="Specific Senate prefixes/doc keys to repair")
    parser.add_argument("--house-prefix", action="append", default=[], help="Specific House prefixes in house-YYYY-DOC format")
    args = parser.parse_args()

    counts = load_doc_id_counts()
    house_dups, senate_dups = find_duplicate_prefixes(counts)
    house_dup_map = dict(house_dups)
    senate_dup_map = dict(senate_dups)

    selected_house = []
    if args.house_prefix:
        selected_house.extend((prefix, house_dup_map.get(prefix, 0)) for prefix in args.house_prefix)
    if args.house_limit:
        for prefix, extra_rows in house_dups:
            year, _ = parse_house_prefix(prefix)
            if args.house_year_min is not None and year < args.house_year_min:
                continue
            if prefix not in {item[0] for item in selected_house}:
                selected_house.append((prefix, extra_rows))
            if len(selected_house) >= len(args.house_prefix) + args.house_limit:
                break

    selected_senate = []
    if args.senate_prefix:
        selected_senate.extend(
            (
                prefix if prefix.startswith("senate-") else f"senate-{prefix}",
                senate_dup_map.get(prefix if prefix.startswith("senate-") else f"senate-{prefix}", 0),
            )
            for prefix in args.senate_prefix
        )
    if args.senate_limit:
        for prefix, extra_rows in senate_dups:
            if prefix not in {item[0] for item in selected_senate}:
                selected_senate.append((prefix, extra_rows))
            if len(selected_senate) >= len(args.senate_prefix) + args.senate_limit:
                break

    house_summary: list[dict] = []
    senate_summary: list[dict] = []

    if selected_house:
        years = sorted({parse_house_prefix(prefix)[0] for prefix, _ in selected_house})
        house_index_cache = {year: load_house_index(year) for year in years}
        members_db = load_members_lookup()
        company_lookup = load_company_lookup()
        for prefix, extra_rows in selected_house:
            year, doc_id = parse_house_prefix(prefix)
            filing = house_index_cache.get(year, {}).get(doc_id)
            if not filing:
                house_summary.append({"prefix": prefix, "status": "missing_index", "extra_rows": extra_rows})
                continue
            try:
                existing_count, inserted_count = replace_house_doc(year, doc_id, filing, members_db, company_lookup)
                house_summary.append(
                    {
                        "prefix": prefix,
                        "status": "repaired",
                        "extra_rows": extra_rows,
                        "rows_replaced": existing_count,
                        "rows_inserted": inserted_count,
                    }
                )
                print(f"Repaired {prefix} ({existing_count} old rows -> {inserted_count} reparsed rows)")
            except Exception as exc:
                house_summary.append({"prefix": prefix, "status": "error", "extra_rows": extra_rows, "error": str(exc)})
                print(f"Failed repairing {prefix}: {exc}")

    if selected_senate:
        session = create_senate_session()
        members_db = load_members_lookup()
        valid_tickers = load_valid_tickers()
        for prefix, extra_rows in selected_senate:
            doc_key = parse_senate_prefix(prefix)
            try:
                filing = load_existing_senate_filing(doc_key)
                existing_count, inserted_count = replace_senate_doc(session, doc_key, filing, members_db, valid_tickers)
                senate_summary.append(
                    {
                        "prefix": prefix,
                        "status": "repaired",
                        "extra_rows": extra_rows,
                        "rows_replaced": existing_count,
                        "rows_inserted": inserted_count,
                    }
                )
                print(f"Repaired {prefix} ({existing_count} old rows -> {inserted_count} reparsed rows)")
            except Exception as exc:
                senate_summary.append({"prefix": prefix, "status": "error", "extra_rows": extra_rows, "error": str(exc)})
                print(f"Failed repairing {prefix}: {exc}")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "house_repaired": house_summary,
                "senate_repaired": senate_summary,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    require_repair_write_opt_in("repair_duplicate_politician_filings.py")
    main()
