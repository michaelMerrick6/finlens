from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from legacy_congress_guard import require_repair_write_opt_in
from repair_house_filings import load_house_index, replace_house_doc
from repair_senate_filings import load_members_lookup
from ingest_house_official import load_company_lookup

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a targeted House repair pass from a doc-id manifest.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/overnight/house_targeted_apply_lists_20260331.json"),
        help="JSON manifest containing named doc-id lists.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Bucket key inside the manifest, e.g. recent_house_2024_2026.",
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


def main() -> None:
    args = parse_args()
    targets = load_targets(args.manifest, args.bucket, args.start, args.limit)

    years = sorted({int(doc_id.split("-")[1]) for doc_id in targets})
    house_index_by_year = {year: load_house_index(year) for year in years}
    members_db = load_members_lookup()
    company_lookup = load_company_lookup()

    results: list[dict] = []
    year_counts = Counter()

    for index, full_doc_id in enumerate(targets, start=1):
        _, year_raw, source_doc_id = full_doc_id.split("-", 2)
        year = int(year_raw)
        filing = house_index_by_year[year].get(source_doc_id)
        if not filing:
            results.append({"doc_id": full_doc_id, "status": "missing_in_index"})
            continue

        try:
            existing_count, inserted_count = replace_house_doc(year, source_doc_id, filing, members_db, company_lookup)
            results.append(
                {
                    "doc_id": full_doc_id,
                    "status": "repaired",
                    "existing_count": existing_count,
                    "inserted_count": inserted_count,
                }
            )
            year_counts[year] += 1
            print(f"[{index}/{len(targets)}] repaired {full_doc_id} ({existing_count} -> {inserted_count})")
        except Exception as exc:
            results.append({"doc_id": full_doc_id, "status": "failed", "error": str(exc)})
            print(f"[{index}/{len(targets)}] FAILED {full_doc_id}: {exc}")

    summary = {
        "bucket": args.bucket,
        "target_count": len(targets),
        "repaired_count": sum(1 for item in results if item["status"] == "repaired"),
        "failed_count": sum(1 for item in results if item["status"] == "failed"),
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
    require_repair_write_opt_in("run_targeted_house_apply.py")
    main()
