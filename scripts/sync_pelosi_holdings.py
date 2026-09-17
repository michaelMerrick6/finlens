"""Rebuild a disclosure-derived overlay. Never append, guess share counts, or modify raw trades."""
import csv
import hashlib
import io
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REVIEW = json.loads((ROOT / 'artifacts/pelosi-audit/2026-position-changes.json').read_text())
BASE_DOCS = {'2025': {'10075701', '20026590', '20030630', '20033337'},
             '2026': {e['doc_id'].split('-')[2] for e in REVIEW['events']}}
# New records predating this transaction watermark need historical reconciliation.
WATERMARK = REVIEW['latest_transaction_reviewed']


def parse_filing(pages, year, doc, filed):
    events = []
    all_text = ' '.join(pages)
    if not re.search(r'Name:\s*Hon\.\s*Nancy Pelosi', all_text, re.I):
        raise ValueError('Member identity not confirmed')
    # Every typed asset row must have a recognized owner and complete transaction.
    for page_no, raw in enumerate(pages, 1):
        text = re.sub(r'\s+', ' ', raw)
        starts = list(re.finditer(r'\b(SP|JT|DC)\s+(.+?)\[([A-Z]{2})\]', text))
        if len(starts) != len(re.findall(r'\[[A-Z]{2}\]', text)):
            raise ValueError('Unrecognized asset row or owner')
        for index, start in enumerate(starts):
            body = text[start.end(): starts[index+1].start() if index+1 < len(starts) else len(text)]
            owner, asset, kind = start.groups()
            ticker = re.search(r'\(([A-Z][A-Z0-9.\-]{0,9})\)', asset)
            tx = re.match(r'\s*(P|S)(?:\s*\((?:partial|full)\))?\s+(\d{2}/\d{2}/\d{4})\s*(\d{2}/\d{2}/\d{4})', body, re.I)
            if owner != 'SP' or not ticker or not tx or kind not in ('ST', 'OP'):
                raise ValueError('Unsupported owner, asset, or transaction type')
            if re.search(r'\b(amend|correct|exercise|donat|gift|transfer|split|spin.off|exchange)', body, re.I):
                raise ValueError('Corporate action, exercise, gift or amendment needs review')
            when = datetime.strptime(tx[2], '%m/%d/%Y').date().isoformat()
            if when <= WATERMARK or when > date.today().isoformat():
                raise ValueError('Late historical or future-dated event needs review')
            action = 'Purchased' if tx[1].upper() == 'P' else 'Sold'
            share_delta = contract_delta = 0
            strike = expiration = None
            if kind == 'ST':
                quantity = re.findall(rf'\b{action}\s+([\d,]+)\s+shares\.', body, re.I)
                if len(quantity) != 1 or len(re.findall(r'\bshares\b', body, re.I)) != 1:
                    raise ValueError('Explicit unambiguous share quantity required')
                share_delta = int(quantity[0].replace(',', '')) * (1 if action == 'Purchased' else -1)
            else:
                option = re.search(rf'{action}\s+([\d,]+)\s+call options with a strike price of \$([\d,.]+) and an expiration date of (\d{{1,2}}/\d{{1,2}}/\d{{2,4}})\.', body, re.I)
                if not option:
                    raise ValueError('Explicit option terms required')
                contract_delta = int(option[1].replace(',', '')) * (1 if action == 'Purchased' else -1)
                strike = float(option[2].replace(',', ''))
                expiration = datetime.strptime(option[3], '%m/%d/%y' if len(option[3].split('/')[-1]) == 2 else '%m/%d/%Y').date().isoformat()
            if not (share_delta or contract_delta):
                raise ValueError('Zero quantity')
            events.append(dict(doc_id=f'house-{year}-{doc}-{len(events)}', ticker=ticker[1], owner=owner,
                date=when, kind='purchase' if action == 'Purchased' else 'sale', share_delta=share_delta,
                contract_delta=contract_delta, strike=strike, expiration=expiration, page=page_no,
                source=f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf',
                published_date=filed, review='Automatically parsed explicit quantity; not manually reviewed'))
    if not events:
        raise ValueError('No complete supported rows')
    return events


def main():
    import os
    import requests
    from dotenv import load_dotenv
    from pypdf import PdfReader
    from supabase import create_client
    load_dotenv(ROOT / '.env.local')
    snapshot = dict(version=1, checked_at=datetime.now(timezone.utc).isoformat(), events=[], documents=[], issues=[])
    try:
        for year in range(2025, date.today().year + 1):
            response = requests.get(f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt', timeout=30)
            response.raise_for_status()
            reader = csv.DictReader(io.StringIO(response.text.lstrip('\ufeff')), delimiter='\t')
            if not {'Last','First','DocID','FilingType'}.issubset(reader.fieldnames or []):
                raise ValueError('Invalid official filing index')
            rows = [r for r in reader if r['Last'].strip().upper() == 'PELOSI' and r['First'].strip().upper() == 'NANCY' and r['FilingType'] in ('P','A','O')]
            known = BASE_DOCS.get(str(year), set())
            if not known.issubset({r['DocID'] for r in rows}):
                raise ValueError('Previously reviewed document missing from index')
            for row in rows:
                doc = row['DocID']
                if doc in known:
                    continue
                if row['FilingType'] != 'P':
                    raise ValueError(f'{year}/{doc}: annual filing or amendment needs baseline review')
                url = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf'
                pdf = requests.get(url, timeout=30); pdf.raise_for_status()
                pages = [p.extract_text() or '' for p in PdfReader(io.BytesIO(pdf.content)).pages]
                events = parse_filing(pages, year, doc, row.get('FilingDate', ''))
                snapshot['events'].extend(events)
                snapshot['documents'].append(dict(year=year, doc_id=doc, sha256=hashlib.sha256(pdf.content).hexdigest(), source=url))
    except Exception as error:
        # Do not publish a partial chain as a complete automatic update.
        snapshot['events'] = []
        snapshot['issues'] = [str(error)[:300]]
    import sys
    if '--dry-run' in sys.argv:
        print(json.dumps({'events':len(snapshot['events']), 'issues':snapshot['issues'], 'checked_at':snapshot['checked_at']}))
        return 1 if snapshot['issues'] else 0
    client = create_client(os.environ['SUPABASE_URL'], os.environ.get('SUPABASE_SERVICE_KEY') or os.environ['SUPABASE_SERVICE_ROLE_KEY'])
    client.table('politician_holdings_sync').upsert(dict(member_id='P000197', snapshot=snapshot, updated_at=snapshot['checked_at'])).execute()
    print(json.dumps({'events':len(snapshot['events']), 'issues':snapshot['issues']}))
    return 1 if snapshot['issues'] else 0

if __name__ == '__main__':
    raise SystemExit(main())
