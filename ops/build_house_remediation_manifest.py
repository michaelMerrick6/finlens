from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path


def classify_mismatch(item: dict) -> str:
    source_rows = int(item.get("source_rows") or 0)
    db_rows = int(item.get("db_rows") or 0)
    missing_rows = int(item.get("missing_rows") or 0)
    extra_rows = int(item.get("extra_rows") or 0)

    if db_rows == 0:
        return "db_zero_missing_filing"
    if source_rows == db_rows and (missing_rows or extra_rows):
        return "same_count_wrong_content"
    if source_rows > db_rows:
        return "underfilled_db"
    if source_rows < db_rows:
        return "overfilled_db"
    return "count_diff_mixed"


def doc_year(doc_id: str) -> int | None:
    parts = str(doc_id or "").split("-")
    if len(parts) < 3:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


def score_mismatch(item: dict) -> tuple[int, int, int]:
    return (
        int(item.get("missing_rows") or 0) + int(item.get("extra_rows") or 0),
        int(item.get("source_rows") or 0),
        int(item.get("db_rows") or 0),
    )


def top_items(items: list[dict], limit: int = 25) -> list[dict]:
    return sorted(items, key=score_mismatch, reverse=True)[:limit]


def load_verify_artifact(path: Path) -> tuple[list[dict], list[dict]]:
    payload = json.loads(path.read_text())
    house = payload.get("house") or {}
    mismatches = house.get("mismatched_filings") or []
    parse_failures = house.get("parse_failures") or []
    return mismatches, parse_failures


def build_manifest(artifacts: list[Path]) -> dict:
    mismatches: list[dict] = []
    parse_failures: list[dict] = []

    for artifact in artifacts:
        artifact_mismatches, artifact_parse_failures = load_verify_artifact(artifact)
        mismatches.extend(artifact_mismatches)
        parse_failures.extend(artifact_parse_failures)

    mismatch_kinds = Counter()
    mismatch_years = Counter()
    parse_failure_years = Counter()
    mismatches_by_kind: dict[str, list[dict]] = defaultdict(list)
    mismatches_by_year: dict[int, list[dict]] = defaultdict(list)
    parse_failures_by_year: dict[int, list[dict]] = defaultdict(list)

    for item in mismatches:
        kind = classify_mismatch(item)
        year = doc_year(item.get("doc_id", ""))
        mismatch_kinds[kind] += 1
        if year is not None:
            mismatch_years[year] += 1
            mismatches_by_year[year].append(item)
        mismatches_by_kind[kind].append(item)

    for item in parse_failures:
        year = doc_year(item.get("doc_id", ""))
        if year is not None:
            parse_failure_years[year] += 1
            parse_failures_by_year[year].append(item)

    recent_modern = [item for item in mismatches if (doc_year(item.get("doc_id", "")) or 0) >= 2024]
    recent_mid = [item for item in mismatches if 2020 <= (doc_year(item.get("doc_id", "")) or 0) <= 2023]
    historical_mid = [item for item in mismatches if 2016 <= (doc_year(item.get("doc_id", "")) or 0) <= 2019]
    oldest = [item for item in mismatches if (doc_year(item.get("doc_id", "")) or 0) <= 2015]

    manifest = {
        "summary": {
            "artifact_count": len(artifacts),
            "mismatch_count": len(mismatches),
            "parse_failure_count": len(parse_failures),
            "mismatch_kinds": dict(mismatch_kinds),
            "mismatch_years": dict(sorted(mismatch_years.items(), reverse=True)),
            "parse_failure_years": dict(sorted(parse_failure_years.items(), reverse=True)),
        },
        "priority_buckets": [
            {
                "name": "recent_house_content_mismatches",
                "description": "Recent House filings (2024-2026) that still differ from source. Highest product priority.",
                "filing_count": len(recent_modern),
                "top_examples": top_items(recent_modern, 20),
            },
            {
                "name": "recentish_house_content_mismatches",
                "description": "House filings from 2020-2023 that still differ from source.",
                "filing_count": len(recent_mid),
                "top_examples": top_items(recent_mid, 20),
            },
            {
                "name": "historical_missing_house_filings",
                "description": "Older House filings with db_rows=0 and clear official rows available.",
                "filing_count": len(mismatches_by_kind["db_zero_missing_filing"]),
                "top_examples": top_items(mismatches_by_kind["db_zero_missing_filing"], 20),
            },
            {
                "name": "historical_house_content_mismatches",
                "description": "2016-2019 House filings with same-count but wrong-content issues.",
                "filing_count": len(historical_mid),
                "top_examples": top_items(historical_mid, 20),
            },
            {
                "name": "legacy_house_content_mismatches",
                "description": "2015 and older House filings with the heaviest legacy mismatch burden.",
                "filing_count": len(oldest),
                "top_examples": top_items(oldest, 20),
            },
            {
                "name": "house_parse_failures",
                "description": "House filings that still fail parsing and need parser work or manual review.",
                "filing_count": len(parse_failures),
                "top_examples": parse_failures[:40],
            },
        ],
        "by_kind": {
            kind: {
                "count": len(items),
                "top_examples": top_items(items, 25),
            }
            for kind, items in sorted(mismatches_by_kind.items())
        },
        "by_year": {
            str(year): {
                "mismatch_count": len(mismatches_by_year[year]),
                "parse_failure_count": len(parse_failures_by_year.get(year, [])),
                "top_mismatches": top_items(mismatches_by_year[year], 20),
                "top_parse_failures": parse_failures_by_year.get(year, [])[:20],
            }
            for year in sorted(set(mismatches_by_year) | set(parse_failures_by_year), reverse=True)
        },
    }
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a targeted House remediation manifest from verify artifacts.")
    parser.add_argument(
        "--glob",
        default="artifacts/overnight/congress_backtest_verify_*_house_*.json",
        help="Glob for finished House verify artifacts.",
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        default=Path("artifacts/overnight/house_remediation_manifest.json"),
        help="Output JSON artifact path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifacts = sorted(Path(".").glob(args.glob))
    if not artifacts:
        raise SystemExit(f"No verify artifacts matched: {args.glob}")

    manifest = build_manifest(artifacts)
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    args.artifact.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"ARTIFACT {args.artifact}")
    print(
        json.dumps(
            {
                "artifact_count": manifest["summary"]["artifact_count"],
                "mismatch_count": manifest["summary"]["mismatch_count"],
                "parse_failure_count": manifest["summary"]["parse_failure_count"],
                "top_kind_counts": manifest["summary"]["mismatch_kinds"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
