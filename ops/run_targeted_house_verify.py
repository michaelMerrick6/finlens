from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

from audit_historical_congress_backlog import build_counter, summarize_counter_diff
from ingest_house_official import load_company_lookup, supabase
from repair_house_filings import load_house_index
from sync_recent_house_filings import parse_house_doc

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a targeted House verify pass from a doc-id manifest.")
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="JSON manifest containing a bucket key with House doc-id list values.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Bucket key inside the manifest.",
    )
    parser.add_argument("--start", type=int, default=0, help="Start offset within the bucket.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum docs to process (0 = all).")
    parser.add_argument(
        "--artifact",
        type=Path,
        default=None,
        help="Optional JSON artifact path for results.",
    )
    return parser.parse_args()


def load_targets(manifest_path: Path, bucket: str, start: int, limit: int) -> list[str]:
    payload = json.loads(manifest_path.read_text())
    targets = payload.get(bucket)
    if not isinstance(targets, list) or not targets:
        raise SystemExit(f"No targets found for bucket '{bucket}' in {manifest_path}")
    sliced = targets[start:]
    if limit > 0:
        sliced = sliced[:limit]
    return sliced


def fetch_db_rows(prefix: str) -> list[dict]:
    last_exc = None
    for attempt in range(5):
        try:
            response = (
                supabase.table("politician_trades")
                .select("doc_id, ticker, asset_name, transaction_type, transaction_date, published_date, amount_range")
                .gte("doc_id", prefix)
                .lt("doc_id", prefix + "~")
                .limit(5000)
                .execute()
            )
            return response.data or []
        except Exception as exc:
            last_exc = exc
            if "statement timeout" not in str(exc).lower() or attempt == 4:
                raise
            time.sleep(2.0 * (attempt + 1))
    raise last_exc


def main() -> None:
    args = parse_args()
    targets = load_targets(args.manifest, args.bucket, args.start, args.limit)

    years = sorted({int(doc_id.split("-")[1]) for doc_id in targets})
    house_index_by_year = {year: load_house_index(year) for year in years}
    members_db = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    company_lookup = load_company_lookup()

    results: list[dict] = []
    year_counts = Counter()

    for index, full_doc_id in enumerate(targets, start=1):
        _, year_raw, source_doc_id = full_doc_id.split("-", 2)
        year = int(year_raw)
        filing = house_index_by_year[year].get(source_doc_id)
        prefix = f"house-{year}-{source_doc_id}"

        if not filing:
            results.append({"doc_id": prefix, "status": "missing_in_index"})
            print(f"[{index}/{len(targets)}] MISSING_INDEX {prefix}")
            continue

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
            results.append({"doc_id": prefix, "status": "parse_failed", "error": str(exc)})
            print(f"[{index}/{len(targets)}] PARSE_FAILED {prefix}: {exc}")
            continue

        if status == "no_trade":
            db_rows = fetch_db_rows(prefix)
            if db_rows:
                results.append(
                    {
                        "doc_id": prefix,
                        "status": "mismatch",
                        "source_rows": 0,
                        "db_rows": len(db_rows),
                        "missing_rows": 0,
                        "extra_rows": len(db_rows),
                    }
                )
                print(f"[{index}/{len(targets)}] MISMATCH {prefix}: source_rows=0 db_rows={len(db_rows)}")
            else:
                results.append({"doc_id": prefix, "status": "ok", "source_rows": 0, "db_rows": 0})
                year_counts[year] += 1
            continue

        if not source_rows:
            results.append({"doc_id": prefix, "status": "parse_failed", "error": status})
            print(f"[{index}/{len(targets)}] PARSE_FAILED {prefix}: {status}")
            continue

        db_rows = fetch_db_rows(prefix)
        source_counter = build_counter(source_rows)
        db_counter = build_counter(db_rows)
        missing_rows, extra_rows, missing_examples, extra_examples = summarize_counter_diff(source_counter, db_counter)

        if len(source_rows) == len(db_rows) and missing_rows == 0 and extra_rows == 0:
            results.append({"doc_id": prefix, "status": "ok", "source_rows": len(source_rows), "db_rows": len(db_rows)})
            year_counts[year] += 1
            continue

        results.append(
            {
                "doc_id": prefix,
                "status": "mismatch",
                "source_rows": len(source_rows),
                "db_rows": len(db_rows),
                "missing_rows": missing_rows,
                "extra_rows": extra_rows,
                "missing_examples": missing_examples,
                "extra_examples": extra_examples,
            }
        )
        print(
            f"[{index}/{len(targets)}] MISMATCH {prefix}: "
            f"source_rows={len(source_rows)} db_rows={len(db_rows)} missing={missing_rows} extra={extra_rows}"
        )

    summary = {
        "bucket": args.bucket,
        "target_count": len(targets),
        "ok_count": sum(1 for item in results if item["status"] == "ok"),
        "mismatch_count": sum(1 for item in results if item["status"] == "mismatch"),
        "parse_failure_count": sum(1 for item in results if item["status"] == "parse_failed"),
        "missing_in_index_count": sum(1 for item in results if item["status"] == "missing_in_index"),
        "year_counts": dict(sorted(year_counts.items())),
        "results": results,
    }

    if args.artifact:
        args.artifact.parent.mkdir(parents=True, exist_ok=True)
        args.artifact.write_text(json.dumps(summary, indent=2, sort_keys=True))
        print(f"ARTIFACT {args.artifact}")

    print("SUMMARY_JSON:" + json.dumps({k: v for k, v in summary.items() if k != "results"}, sort_keys=True))


if __name__ == "__main__":
    main()
