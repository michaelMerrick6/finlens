from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import argparse
import csv
import io
import json
from pathlib import Path

import requests
from pdf2image import convert_from_bytes
from pypdf import PdfReader

from ingest_house_official import (
    HOUSE_INDEX_URL,
    HOUSE_PTR_PDF_URL,
    extract_ocr_lines,
    extract_pdf_lines,
    extract_pdftotext_layout_lines,
    load_company_lookup,
    normalize_line,
    supabase,
)
from sync_recent_house_filings import parse_house_doc
from time_utils import congress_now

MANUAL_FIXES_PATH = Path("data/house_review_fixes.json")


def parse_target(value: str) -> tuple[int, str]:
    year_raw, _, doc_id = value.partition(":")
    if not year_raw or not doc_id:
        raise argparse.ArgumentTypeError("Expected YEAR:DOC_ID")
    return int(year_raw), doc_id


def load_house_index(year: int) -> dict[str, dict]:
    response = requests.get(HOUSE_INDEX_URL.format(year=year), timeout=30)
    response.raise_for_status()
    payload = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    filings: dict[str, dict] = {}
    for row in reader:
        if (row.get("FilingType") or "").strip().upper() != "P":
            continue
        doc_id = (row.get("DocID") or "").strip()
        if not doc_id:
            continue
        filings[doc_id] = {
            "year": year,
            "doc_id": doc_id,
            "first_name": (row.get("First") or "").strip(),
            "last_name": (row.get("Last") or "").strip(),
            "filing_date_raw": (row.get("FilingDate") or "").strip(),
        }
    return filings


def iter_targets(start_year: int, end_year: int, explicit_targets: list[tuple[int, str]]) -> list[dict]:
    if explicit_targets:
        filings: list[dict] = []
        cache: dict[int, dict[str, dict]] = {}
        for year, doc_id in explicit_targets:
            if year not in cache:
                cache[year] = load_house_index(year)
            filing = cache[year].get(doc_id)
            if not filing:
                raise RuntimeError(f"Could not find {year}:{doc_id} in House index")
            filings.append(filing)
        return filings

    filings: list[dict] = []
    for year in range(end_year, start_year - 1, -1):
        year_filings = list(load_house_index(year).values())
        year_filings.sort(key=lambda filing: filing["doc_id"], reverse=True)
        filings.extend(year_filings)
    return filings


def page_count(pdf_bytes: bytes) -> int:
    try:
        return len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception:
        return 0


def load_manual_fix_ids() -> set[str]:
    if not MANUAL_FIXES_PATH.exists():
        return set()
    payload = json.loads(MANUAL_FIXES_PATH.read_text())
    return {f"house-{int(filing['year'])}-{filing['doc_id']}" for filing in payload}


def save_previews(pdf_bytes: bytes, image_dir: Path, stem: str) -> list[str]:
    image_dir.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    try:
        images = convert_from_bytes(pdf_bytes, dpi=180, first_page=1, last_page=2)
    except Exception:
        return saved

    for index, image in enumerate(images, 1):
        path = image_dir / f"{stem}-page{index}.png"
        image.save(path)
        saved.append(str(path))
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a manual review queue for unparsed House PTR filings.")
    parser.add_argument("--start-year", type=int, default=2013)
    parser.add_argument("--end-year", type=int, default=congress_now().year)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--target", action="append", type=parse_target, default=[])
    parser.add_argument("--output", type=Path, default=Path("artifacts/house-review-queue.json"))
    parser.add_argument("--image-dir", type=Path, default=None, help="Optional directory for first-page preview PNGs")
    args = parser.parse_args()

    members_db = supabase.table("congress_members").select("id, first_name, last_name, chamber, active").execute().data or []
    company_lookup = load_company_lookup()
    filings = iter_targets(args.start_year, args.end_year, args.target)
    if args.limit > 0:
        filings = filings[: args.limit]
    manual_fix_ids = load_manual_fix_ids()

    entries: list[dict] = []
    for filing in filings:
        prefix = f"house-{filing['year']}-{filing['doc_id']}"
        if prefix in manual_fix_ids:
            continue
        status, trades = parse_house_doc(filing, members_db, company_lookup)
        if status in {"trades", "no_trade"}:
            continue

        pdf_url = HOUSE_PTR_PDF_URL.format(year=filing["year"], doc_id=filing["doc_id"])
        pdf_resp = requests.get(pdf_url, timeout=(10, 60))
        pdf_resp.raise_for_status()
        pdf_bytes = pdf_resp.content

        pdf_lines = [normalize_line(line) for line in extract_pdf_lines(pdf_bytes) if normalize_line(line)]
        layout_lines = [normalize_line(line) for line in extract_pdftotext_layout_lines(pdf_bytes) if normalize_line(line)]
        ocr_lines = [normalize_line(line) for line in extract_ocr_lines(pdf_bytes) if normalize_line(line)]

        stem = prefix
        preview_paths = save_previews(pdf_bytes, args.image_dir, stem) if args.image_dir else []

        entries.append(
            {
                "doc_id": stem,
                "source_url": pdf_url,
                "status": status,
                "politician_name": f"{filing['first_name']} {filing['last_name']}".strip(),
                "filing_date_raw": filing["filing_date_raw"],
                "page_count": page_count(pdf_bytes),
                "pdf_text_line_count": len(pdf_lines),
                "layout_line_count": len(layout_lines),
                "ocr_line_count": len(ocr_lines),
                "image_only_pdf": not pdf_lines and not layout_lines,
                "ocr_preview": ocr_lines[:20],
                "preview_images": preview_paths,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(entries, indent=2))
    print(f"WROTE {len(entries)} review entries to {args.output}")
    print("SUMMARY_JSON:" + json.dumps({"entries": len(entries), "output": str(args.output)}))


if __name__ == "__main__":
    main()
