"""Stage House annuals locally; never promote machine-parsed assets to current holdings."""
import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import unicodedata
import requests
from pypdf import PdfReader
from house_financial_disclosure_parser import iter_section_a_lines, build_asset_blocks, parse_asset_block, normalize_line, clean_pdf_text

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / 'artifacts/congress-holdings/sources'


def normalized(value):
    return ' '.join(''.join(c for c in unicodedata.normalize('NFKD', value) if not unicodedata.combining(c)).upper().split())


def name_match_basis(source, expected):
    if normalized(source) == normalized(expected):
        return 'exact'
    # Only omit a terminal generational suffix; never fuzzy-match names or initials.
    def split(value):
        match = re.fullmatch(r'(.*?)\s+(JR\.?|SR\.?|II|III|IV)', normalized(value))
        return (match[1].rstrip(','), match[2].rstrip('.')) if match else (normalized(value), None)
    source_base, source_suffix = split(source)
    expected_base, expected_suffix = split(expected)
    if source_base == expected_base and (not source_suffix or not expected_suffix or source_suffix == expected_suffix):
        return 'generational-suffix-normalized'
    return None


def explicitly_no_assets(full):
    # Accept only an otherwise empty Schedule A bounded by the next schedule.
    lines = [normalize_line(line) for line in clean_pdf_text(full).splitlines()]
    sections = []
    start = None
    for index, line in enumerate(lines):
        if re.match(r'^(?:S|SCHEDULE|SECTION) A:', line, re.I):
            start = index + 1
        elif start is not None and re.match(r'^(?:S|SCHEDULE|SECTION) [B-J]:', line, re.I):
            sections.append([item for item in lines[start:index] if item])
            start = None
    return len(sections) == 1 and sections[0] == ['None disclosed.']


def validate_document(content, member, candidate):
    reader = PdfReader(io.BytesIO(content))
    first = reader.pages[0].extract_text() or ''
    issues = []
    name = re.search(r'Name:\s*(?:Hon\.\s*)?([^\n]+)', first)
    match_basis = name_match_basis(name[1], member['name']) if name else None
    if not match_basis: issues.append('member-name-mismatch')
    if not re.search(r'Status:\s*Member\b', first): issues.append('member-status-unconfirmed')
    if not re.search(r'Filing Type:\s*Annual Report\s*(?:\n|$)', first): issues.append('not-unamended-annual-report')
    year = re.search(r'Filing Year:\s*(\d{4})', first)
    filed = re.search(r'Filing Date:\s*(\d{2}/\d{2}/\d{4})', first)
    if not year or int(year[1]) != candidate['year']: issues.append('reporting-year-mismatch')
    filing_date = datetime.strptime(filed[1], '%m/%d/%Y').date().isoformat() if filed else None
    if not filed or datetime.strptime(candidate['filed'], '%m/%d/%Y').date().isoformat() != filing_date: issues.append('filing-date-mismatch')
    valuation_date = f'{year[1]}-12-31' if year else None
    if valuation_date and filing_date and valuation_date >= filing_date: issues.append('invalid-annual-date-order')
    if not re.search(r'Filing ID #\s*'+re.escape(candidate['doc_id'])+r'\b', first): issues.append('filing-id-mismatch')
    if any(len((p.extract_text() or '').strip()) < 30 for p in reader.pages): issues.append('image-or-empty-page-needs-review')
    lines, full = iter_section_a_lines(content)
    # Blank owners stay unspecified. Unknown tickers and non-stock assets stay in the inventory.
    blocks = build_asset_blocks(lines)
    parsed = [parse_asset_block(b) for b in blocks]
    typed_rows = sum(len(re.findall(r'\[[A-Z]{2}\]', line)) for line in lines)
    no_assets = explicitly_no_assets(full) and not lines and not blocks
    if (not blocks and not no_assets) or typed_rows != len(blocks) or any(p is None for p in parsed): issues.append('incomplete-asset-blocks')
    # An explicit period overrides the usual calendar-year assumption only after review.
    if re.search(r'Period Covered:', full, re.I): issues.append('explicit-reporting-period-needs-review')
    holdings = [dict(asdict(p), row_index=index) for index, p in enumerate(parsed) if p is not None]
    rejected_blocks = [dict(row_index=index, extracted_text=blocks[index]) for index, p in enumerate(parsed) if p is None]
    return dict(member_id=member['member_id'], name=member['name'], chamber='House', source=candidate['source'],
                source_member_name=name[1].strip() if name else None, name_match_basis=match_basis,
                explicit_no_assets=no_assets, doc_id=candidate['doc_id'], filing_date=filing_date, reporting_year=int(year[1]) if year else None,
                valuation_date=valuation_date, valuation_date_basis='Calendar-year annual report; House annual disclosure instructions',
                source_sha256=hashlib.sha256(content).hexdigest(), pages=len(reader.pages), asset_blocks=len(blocks),
                holdings=holdings, rejected_blocks=rejected_blocks, typed_asset_markers=typed_rows, issues=issues, status='review-required' if issues else 'machine-checked-awaiting-review',
                current_holdings_eligible=False)


def select_latest_annual(candidates):
    # Filing time is not the valuation period: a late older-year annual must
    # not displace a newer reporting-year baseline. Retain all candidates in
    # the source inventory for later amendment/transaction reconciliation.
    year = max(c['year'] for c in candidates)
    current = sorted((c for c in candidates if c['year'] == year),
                     key=lambda c: (datetime.strptime(c['filed'], '%m/%d/%Y'), c['doc_id']), reverse=True)
    latest = current[0]
    same_day = [c for c in current if datetime.strptime(c['filed'], '%m/%d/%Y') == datetime.strptime(latest['filed'], '%m/%d/%Y')]
    if len(same_day) > 1 or latest['type'] == 'W':
        raise ValueError('competing-or-amended-latest-filing')
    return latest


def prepare(member):
    try:
        latest = select_latest_annual(member['candidates'])
        path=CACHE / f'{latest["year"]}-{latest["doc_id"]}.pdf'
        if not path.exists():
            r=requests.get(latest['source'],timeout=30);r.raise_for_status()
            if not r.content.startswith(b'%PDF'):raise ValueError('Invalid PDF response')
            path.write_bytes(r.content)
        return validate_document(path.read_bytes(),member,latest)
    except Exception as error:
        return dict(member_id=member['member_id'],status='review-required',issues=[str(error)[:200]],current_holdings_eligible=False)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int);args=parser.parse_args()
    CACHE.mkdir(parents=True,exist_ok=True)
    members=[m for m in json.loads((ROOT/'docs/research/congress-holdings-coverage.json').read_text())['members'] if m['candidates'] and m['member_id']!='P000197']
    if args.limit: members=members[:args.limit]
    rows=[]
    with ProcessPoolExecutor(max_workers=4) as executor:
        for i,result in enumerate(executor.map(prepare,members),1):
            rows.append(result)
            if i%25==0:print(f'Checked {i}/{len(members)} documents',flush=True)
    summary=dict(documents=len(rows),machine_checked=sum(r['status']=='machine-checked-awaiting-review' for r in rows),review_required=sum(r['status']=='review-required' for r in rows),parsed_assets=sum(len(r.get('holdings',[])) for r in rows))
    target=ROOT/'docs/research/congress-house-baselines.json'
    target.write_text(json.dumps(dict(checked_at=datetime.now(timezone.utc).isoformat(),summary=summary,members=rows),indent=2)+'\n')
    print(json.dumps(summary))
if __name__=='__main__': main()
