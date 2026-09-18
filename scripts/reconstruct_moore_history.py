"""Audit Moore's full indexed PTR history from his first year in office.

Research only: an earliest reported purchase never establishes zero ownership.
Keep all transactions and correction flags, including pre-baseline activity.
"""
import csv, hashlib, io, json, re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import requests
from house_ptr_columns import parse_ptr_columns
from house_financial_disclosure_parser import extract_ticker
from reconcile_priority_holdings import dollar_range, iso
from model_unanchored_holdings import model_unanchored_flow
from model_zero_start_scenario import model_zero_start_scenario
from stage_priority_models import prices, CACHE as PRICE_CACHE

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('HY', 'T', 'SMPL', 'IHG', 'RYCEY')
CACHE = ROOT / 'artifacts/congress-holdings/moore-full-history'


def document(task):
    year, row = task
    url = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{row["DocID"]}.pdf'
    path = CACHE / f'{year}-{row["DocID"]}.pdf'
    response = requests.get(url, timeout=45)
    response.raise_for_status()
    raw = response.content
    if not raw.startswith(b'%PDF'): raise ValueError(f'Not a PDF: {url}')
    path.write_bytes(raw)
    parsed = parse_ptr_columns(path)
    events = []
    for i, e in enumerate(parsed['events'], 1):
        c = e['cells']; metadata = ' '.join(e['metadata'])
        status = re.search(r'(?:F\s*S:|Filing Status:)\s*(New|Amended|Deleted)', metadata)
        events.append(dict(id=f'{url}#row={i}', ticker=extract_ticker(c['Asset']),
                           date=iso(c['Date']), action=c['Transaction Type'],
                           name=c['Asset'], value_bounds=dollar_range(c['Amount']),
                           range=c['Amount'], owner=c['Owner'], account_metadata=e['metadata'],
                           filing_status=status[1] if status else 'Unknown',
                           source=url, page=e['page'], source_sha256=hashlib.sha256(raw).hexdigest()))
    return dict(source=url, filed=iso(row['FilingDate']), sha256=hashlib.sha256(raw).hexdigest(),
                issues=parsed['issues'], asset_markers=parsed['asset_markers'], events=events)


def summarize(documents):
    events = [e for d in documents for e in d['events']]
    results = []
    for ticker in TARGETS:
        rows = sorted((e for e in events if e['ticker'] == ticker), key=lambda e:(e['date'], e['id']))
        results.append(dict(ticker=ticker, first_reported_transaction=rows[0] if rows else None,
                            transaction_count=len(rows), events=rows,
                            opening_balance='unknown', current_holdings_eligible=False,
                            corrections=[e['id'] for e in rows if e['filing_status']!='New']))
    return results


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    indexes=[];tasks=[]
    for year in (2025, 2026):
        url=f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt'
        r=requests.get(url,timeout=45);r.raise_for_status()
        rows=[row for row in csv.DictReader(io.StringIO(r.content.decode('utf-8-sig')),delimiter='\t')
              if row.get('Last','').lower()=='moore' and row.get('First','').lower()=='tim']
        indexes.append(dict(year=year,source=url,sha256=hashlib.sha256(r.content).hexdigest(),documents=rows))
        tasks.extend((year,row) for row in rows if row['FilingType']=='P')
    with ThreadPoolExecutor(max_workers=4) as pool:documents=list(pool.map(document,tasks))
    documents.sort(key=lambda d:(d['filed'],d['source']))
    results=summarize(documents)
    if any(d['issues'] for d in documents): raise ValueError('Resolve PDF extraction issues before reconstruction')
    PRICE_CACHE.mkdir(parents=True, exist_ok=True)
    for target in results:
        _, market = prices(target['ticker'])
        if market['error']: raise ValueError(market['error'])
        target['price_source'] = market['source']
        target['price_cache_sha256'] = hashlib.sha256((PRICE_CACHE / (target['ticker']+'.json')).read_bytes()).hexdigest()
        target['net_flow'] = model_unanchored_flow(target['events'], market['prices'], '2025-01-01', '2026-09-17')
        target['zero_start_scenario'] = model_zero_start_scenario(target['events'], market['prices'], '2025-01-01', '2026-09-17')
        if target['ticker'] == 'HY':
            target['year_end_scenario'] = model_zero_start_scenario(target['events'], market['prices'], '2025-01-01', '2025-12-31')
            target['annual_crosscheck'] = dict(source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf',
                page=4, reported_value='None', disposition='Conflicts with positive year-end quantity implied by December 31 purchase under the long-only scenario. Do not override either filing.')
    out=dict(member_id='M001236',checked_at=datetime.now(timezone.utc).isoformat(),
             scope='All PTRs indexed in 2025 and 2026; target-security history, not confirmed current holdings.',
             publication_enabled=False,indexes=indexes,documents=documents,targets=results,
             extraction_issues=[dict(source=d['source'],issues=d['issues']) for d in documents if d['issues']])
    (ROOT/'docs/research/moore-full-transaction-history.json').write_text(json.dumps(out,indent=2)+'\n')
    print('Documents:',len(documents),'Transactions:',sum(len(d['events']) for d in documents),'Issue documents:',len(out['extraction_issues']))
    for t in results:
        print(t['ticker'],len(t['events']),'corrections:',t['corrections'])
        for e in t['events']:print(e['date'],e['action'],e['range'],e['source'].split('/')[-1],e['page'])

if __name__=='__main__': main()
