#!/usr/bin/env python3
"""Extract report rows, not inferred stock positions, from the certified 2026 PDF.

Usage: python scripts/strategies/import_trump_annual.py INPUT.pdf OUTPUT.json
Requires Poppler pdftotext and pdfplumber (scripts/requirements.txt).
Only the reviewed document checksum is accepted.
The original government document is available from the URL in SOURCE.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import pdfplumber

SOURCE = 'https://oge.box.com/shared/static/zycb5i2ny8kssm51uzqm8ygyq2zkpkqq.pdf'
ROW = re.compile(r'^\s*(\d+)\s+(.+?)\s{2,}(N/A|Yes|No)\s+(.+?)\s{2,}')
RANGE = re.compile(r'^\$([\d,]+) - \$([\d,]+)$')
ACCOUNT = re.compile(r'^(?:INVESTMENT ACCOUNT #\d+|FAMILY TRUST \d+\*?|DONALD J\.? TRUMP(?: REVOCABLE TRUST)?)$')


def parse_value(value):
    if value == 'None (or less than $1,001)':
        return 0, 1000
    match = RANGE.fullmatch(value)
    if match:
        return tuple(int(n.replace(',', '')) for n in match.groups())
    if value == 'Over $50,000,000':
        return 50000001, None
    raise ValueError(f'Unrecognized value range: {value}')


def parse_pages(text):
    rows = []
    part_pages = []
    for page, content in enumerate(text.split('\f'), 1):
        if 'Part 6: Other Assets and Income' not in content:
            continue
        part_pages.append(page)
        ordinal = 0
        # Some pages restart line numbering for another account. Retain every
        # row and use its page-local ordinal as the unique key.
        for line in content.splitlines():
            if not re.match(r'^\s*\d+\s+\S', line):
                continue
            match = ROW.match(line)
            if not match:
                raise ValueError(f'Unparsed asset row on page {page}: {line}')
            number, name, _, value = match.groups()
            low, high = parse_value(value)
            ordinal += 1
            rows.append({'id': f'{page}-{ordinal}', 'name': name.strip(),
                         'page': page, 'line': int(number), 'low': low, 'high': high})
    if len(set(r['id'] for r in rows)) != len(rows):
        raise ValueError('Duplicate page/line identifiers')
    return rows, part_pages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pdf', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    digest = hashlib.sha256(args.pdf.read_bytes()).hexdigest()
    # A changed source requires review, not silently applying an old parser.
    expected = '84b5987e4c8a418188600bea1e1ba6b44e0d5735cdd757cee5aba551977ca402'
    if digest != expected:
        raise ValueError(f'Unreviewed document checksum: {digest}')
    text = subprocess.check_output(['pdftotext', '-layout', str(args.pdf), '-'], text=True)
    rows, pages = parse_pages(text)
    if pages != list(range(7, 159)) or len(rows) != 6181:
        raise ValueError(f'Unexpected coverage: {len(pages)} pages, {len(rows)} rows')
    # Table cells retain wrapped descriptions. Independently compare every line
    # and range with Poppler before accepting the extraction.
    cell_rows = []
    account = None
    with pdfplumber.open(args.pdf) as document:
        for page in pages:
            ordinal = 0
            for table in document.pages[page - 1].extract_tables():
                for cells in table:
                    description = ' '.join((cells[1] or '').split()) if len(cells) > 1 else ''
                    if not cells[0] and ACCOUNT.fullmatch(description):
                        account = description
                    if not cells[0] or not cells[0].strip().isdigit():
                        continue
                    if len(cells) < 4 or cells[2] not in ('N/A', 'Yes', 'No'):
                        raise ValueError(f'Unrecognized table row on page {page}: {cells}')
                    low, high = parse_value(cells[3])
                    if not account:
                        raise ValueError(f'Asset has no account context on page {page}')
                    ordinal += 1
                    cell_rows.append({'id': f'{page}-{ordinal}',
                                      'name': description, 'account': account,
                                      'incomeType': ' '.join((cells[4] or '').split()),
                                      'eif': cells[2],
                                      'page': page, 'line': int(cells[0]),
                                      'low': low, 'high': high})
            document.pages[page - 1].close()
    if len(rows) != len(cell_rows) or any(
        any(left[key] != right[key] for key in ('id', 'line', 'low', 'high'))
        for left, right in zip(rows, cell_rows)
    ):
        raise ValueError('Independent PDF extractors disagree on asset rows')
    rows = cell_rows
    output = {'sourceUrl': SOURCE, 'sha256': digest, 'reportYear': 2025,
              'publishedOn': '2026-06-30', 'holdingsAsOf': '2025-12-31',
              'part6RowCount': len(rows), 'part6Pages': [7, 158],
              'rows': rows,
              'djt': {'ticker': 'DJT', 'name': 'Trump Media & Technology Group',
                      'shares': 114750000, 'page': 865, 'line': 349,
                      'note': 'Common stock subject to restrictions; held through the Donald J. Trump Revocable Trust.'}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, separators=(',', ':')) + '\n')
    print(f'Wrote {len(rows)} source rows; {sum(r["low"] > 0 for r in rows)} report a nonzero value range.')


if __name__ == '__main__':
    main()
