"""Offline source-image regression. No API calls or database access.

Usage: python ops/verify_senate_table_ocr.py --dataset artifacts/ocr-benchmark/dataset-v1 --output artifacts/ocr-benchmark/table-verification.json
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from senate_table_ocr import extract_document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = ROOT / 'config/reviewed_filings/senate-929216d5-5dbd-429c-858c-1e9332924627.json'
    truth = json.loads(manifest.read_text())
    images = []
    for page, digest in enumerate(truth['image_rgb_sha256'], 1):
        image = Image.open(args.dataset / f'{manifest.stem}-{page:02}.png').convert('RGB')
        if hashlib.sha256(image.tobytes()).hexdigest() != digest:
            raise ValueError(f'Source image changed: {page}')
        images.append(image)
    # Extraction sees only original pixels and filing date; never expected rows.
    rows = extract_document(images, filed_date=truth['published_date'])
    expected = {(r['page'], r['row']): r for r in truth['rows']}
    actual = {(r['page'], r['row']): r for r in rows}
    normalize = lambda text: re.sub(r'[^a-z0-9]', '', text.lower())
    errors = []
    for key, row in expected.items():
        found = actual.get(key, {})
        for field in ('transaction_date','transaction_type','amount_range','asset_name'):
            value = found.get(field, '')
            if field == 'asset_name':
                value = re.sub(r'^\(S\)\s*', '', value)
            if normalize(value) != normalize(row[field]):
                errors.append(dict(page=key[0], row=key[1], field=field))
    report = dict(filing=manifest.stem, source_images_verified=len(images),
                  expected_rows=len(expected), extracted_rows=len(rows),
                  duplicate_positions=len(rows)-len(actual),
                  missing_positions=sorted(expected.keys()-actual.keys()),
                  extra_positions=sorted(actual.keys()-expected.keys()), field_errors=errors,
                  scope='One development filing, not a held-out accuracy estimate',
                  extraction='Tesseract cells and OpenCV grid/mark detection; no reviewed override')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
    return int(bool(errors or report['duplicate_positions'] or report['missing_positions'] or report['extra_positions']))


if __name__ == '__main__':
    raise SystemExit(main())
