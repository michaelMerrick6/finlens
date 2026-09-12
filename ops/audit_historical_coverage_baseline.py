"""Read-only stored-history and official congressional PTR inventory baseline; never parses OCR."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))

HOUSE_URL = 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt'


def filing_key(doc_id):
    match = re.fullmatch(r'(house-\d{4}-\d+|senate-[0-9a-fA-F-]{36})-\d+', str(doc_id or ''))
    return match.group(1) if match else None


def compare_inventory(official, stored):
    missing = sorted(set(official) - set(stored))
    return {'official_ptr_filings': len(official), 'filings_with_stored_rows': len(set(official) & set(stored)),
            'filings_without_stored_rows': len(missing), 'without_stored_rows_ids': missing,
            'stored_keys_not_in_official_index': sorted(set(stored) - set(official)),
            'meaning': 'Presence only: absent rows may mean no transactions, excluded filers, capture gaps or review-required documents; present rows are not transaction verification.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--connection-options', type=Path, help='Optional ignored JSON of psycopg connection overrides')
    args = parser.parse_args()
    load_dotenv('.env.local')
    now = datetime.now(timezone.utc)
    if not 2012 <= args.start_year <= now.year:parser.error('Invalid start year')
    options = json.loads(args.connection_options.read_text()) if args.connection_options else {}
    result = {'checked_at': now.isoformat(), 'scope': 'All stored congressional transactions; House and Senate official PTR inventories from start_year through current year',
              'start_year': args.start_year, 'transaction_verification': 'Not performed; OCR deferred',
              'senate_inventory': [], 'stored_history': [], 'house_inventory': []}
    with psycopg.connect(os.environ['DATABASE_URL'], **options, row_factory=dict_row, connect_timeout=15) as db:
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        db.execute("SET LOCAL statement_timeout='90s'")
        rows = db.execute('SELECT chamber,published_date,transaction_date,doc_id,member_id FROM politician_trades').fetchall()
        groups = {}
        for row in rows:
            year = row['published_date'].year if row['published_date'] else None
            groups.setdefault((row['chamber'], year), []).append(row)
        for (chamber, year), group in sorted(groups.items(), key=lambda x: str(x[0])):
            keys = [filing_key(r['doc_id']) for r in group]
            docs = Counter(r['doc_id'] for r in group if r['doc_id'])
            result['stored_history'].append({'chamber': chamber, 'published_year': year, 'transaction_rows': len(group),
                'recognizable_official_filing_keys': len(set(k for k in keys if k)),
                'rows_without_recognizable_official_key': sum(k is None for k in keys),
                'duplicate_doc_id_extra_rows': sum(n-1 for n in docs.values()),
                'unresolved_member_rows': sum(not r['member_id'] or r['member_id'].startswith('unknown-') for r in group),
                'transaction_after_filing_rows': sum(bool(r['published_date'] and r['transaction_date'] and r['transaction_date'] > r['published_date']) for r in group),
                'earliest_transaction': str(min(r['transaction_date'] for r in group if r['transaction_date'])),
                'latest_transaction': str(max(r['transaction_date'] for r in group if r['transaction_date']))})
        result['other_stored_sources'] = {}
        for table in ['insider_trades', 'institutional_holdings']:
            result['other_stored_sources'][table] = db.execute(f'SELECT extract(year from published_date)::int AS published_year, count(*) AS stored_rows, count(distinct source_url) AS distinct_source_urls, count(*) FILTER (WHERE source_url IS NULL) AS rows_without_source_url FROM {table} GROUP BY 1 ORDER BY 1').fetchall()
        result['database_snapshot_at'] = str(db.execute('SELECT transaction_timestamp() AS ts').fetchone()['ts'])
    for year in range(args.start_year, now.year + 1):
        url = HOUSE_URL.format(year=year)
        response = requests.get(url, timeout=45);response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.content.decode('utf-8-sig')), delimiter='\t')
        if not {'DocID','FilingType'}.issubset(reader.fieldnames or []):raise ValueError('Unrecognized House index')
        official = {f"house-{year}-{r['DocID'].strip()}" for r in reader if r['FilingType'].strip().upper() == 'P' and r['DocID'].strip()}
        if not official:raise ValueError(f'Empty official inventory for {year}')
        stored = {filing_key(r['doc_id']) for r in rows if str(r['doc_id'] or '').startswith(f'house-{year}-')}
        stored.discard(None)
        result['house_inventory'].append({'year':year,'source_url':url,'source_sha256':hashlib.sha256(response.content).hexdigest(), **compare_inventory(official, stored)})
        print(f'{year}: {len(official)} official PTRs, {len(official & stored)} have stored rows', flush=True)
    from ingest_senate_official import establish_senate_session, SENATE_REPORT_DATA_URL, SENATE_SEARCH_URL
    session = requests.Session()
    csrf = establish_senate_session(session)
    for year in range(args.start_year, now.year + 1):
        official = set()
        offset = 0
        expected = None
        digest = hashlib.sha256()
        while expected is None or offset < expected:
            response = session.post(SENATE_REPORT_DATA_URL, data={
                'start': str(offset), 'length': '100', 'report_types': '[11]', 'filer_types': '[]',
                'submitted_start_date': f'01/01/{year} 00:00:00', 'submitted_end_date': f'12/31/{year} 23:59:59',
                'candidate_state': '', 'senator_state': '', 'office_id': '', 'first_name': '', 'last_name': '',
                'csrfmiddlewaretoken': csrf}, headers={'Referer': SENATE_SEARCH_URL}, timeout=45)
            response.raise_for_status()
            payload = response.json()
            total = payload.get('recordsFiltered')
            if not isinstance(total,int) or total < 0:raise ValueError('Missing Senate inventory count')
            if expected is not None and total != expected:raise ValueError('Senate inventory changed during pagination; rerun')
            expected = total
            batch = payload.get('data')
            if not isinstance(batch,list) or (not batch and offset < expected):raise ValueError('Incomplete Senate inventory page')
            digest.update(response.content)
            for filing in batch:
                matches = re.findall(r'/search/view/(?:ptr|paper)/([0-9a-fA-F-]{36})/',str(filing[3]))
                if len(matches)!=1:raise ValueError('Unrecognized Senate document link')
                if datetime.strptime(filing[4],'%m/%d/%Y').year != year:raise ValueError('Senate returned filing outside requested year')
                key = 'senate-' + matches[0]
                if key in official:raise ValueError('Duplicate Senate inventory key during pagination')
                official.add(key)
            offset += len(batch)
        if len(official)!=expected:raise ValueError('Incomplete Senate inventory')
        stored = {filing_key(r['doc_id']) for r in rows if r['chamber']=='Senate' and r['published_date'] and r['published_date'].year==year}
        stored.discard(None)
        result['senate_inventory'].append({'year':year,'source_url':SENATE_REPORT_DATA_URL,
            'response_pages_sha256':digest.hexdigest(), **compare_inventory(official,stored)})
        print(f'{year}: {len(official)} official Senate PTRs, {len(official & stored)} have stored rows',flush=True)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n')


if __name__ == '__main__':main()
