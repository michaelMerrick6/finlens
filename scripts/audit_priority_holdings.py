"""Read-only source inventory for the user's priority list; never publishes estimates."""
import csv
import hashlib
import io
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from prepare_congress_holdings_baselines import prepare, normalized
from senate_annual_holdings import parse_senate_annual

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ROOT / 'scripts/priority_holdings_members.json'
CACHE = ROOT / 'artifacts/congress-holdings/sources'
SENATE = 'https://efdsearch.senate.gov'


def matches_house(row, member):
    return (normalized(row['First']) == normalized(member['first']) and
            normalized(row['Last']) == normalized(member['last']) and
            row['StateDst'] == member['district'])


def senate_session():
    session = requests.Session()
    home = SENATE + '/search/home/'
    response = session.get(home, timeout=25); response.raise_for_status()
    token = BeautifulSoup(response.text, 'html.parser').find('input', {'name':'csrfmiddlewaretoken'})
    if not token: raise ValueError('Senate session unavailable')
    response = session.post(home, data={'csrfmiddlewaretoken':token['value'], 'prohibition_agreement':'1'}, headers={'Referer':home}, timeout=25)
    response.raise_for_status()
    return session


def senate_reports(session, member):
    reports, seen, offset, expected = [], set(), 0, None
    while True:
        payload = dict(start=str(offset), length='100', report_types='[]', filer_types='[]',
                       submitted_start_date='01/01/2025 00:00:00', submitted_end_date='',
                       first_name='', last_name=member['last'],
                       csrfmiddlewaretoken=session.cookies.get('csrftoken') or session.cookies.get('csrf'))
        response = session.post(SENATE+'/search/report/data/', data=payload, headers={'Referer':SENATE+'/search/'}, timeout=30)
        response.raise_for_status(); data=response.json()
        total = data.get('recordsFiltered')
        if not isinstance(total, int) or total < 0 or (expected is not None and expected != total):
            raise ValueError('Invalid or changing Senate pagination total')
        expected=total
        rows=data.get('data', [])
        if not rows:
            if offset != total: raise ValueError('Incomplete Senate inventory')
            break
        for row in rows:
            link=BeautifulSoup(row[3], 'html.parser').find('a')
            if not link or not link.get('href','').startswith('/search/view/'): raise ValueError('Invalid Senate document link')
            href=link['href']
            if href in seen: raise ValueError('Duplicate Senate pagination row')
            seen.add(href)
            if normalized(row[0]) != normalized(member['first']) or normalized(row[1]) != normalized(member['last']):
                raise ValueError('Senate filer identity needs review')
            reports.append(dict(source=urljoin(SENATE, href), title=link.get_text(' ',strip=True), filed=row[4],
                                kind='annual' if '/annual/' in href else 'ptr' if '/ptr/' in href else 'other'))
        offset += len(rows)
        if offset > total: raise ValueError('Senate inventory exceeded reported total')
        if offset == total: break
    return reports


def stage_senate_annual(session, report, member):
    response=session.get(report['source'],timeout=30);response.raise_for_status()
    soup=BeautifulSoup(response.text,'html.parser')
    # Store original tables and their headers. A table count is NOT a holdings count.
    tables=[]
    for table in soup.find_all('table'):
        rows=[[cell.get_text(' ',strip=True) for cell in tr.find_all(['th','td'])] for tr in table.find_all('tr')]
        tables.append(rows)
    doc=report['source'].rstrip('/').split('/')[-1]
    (CACHE/f'senate-{doc}.html').write_text(response.text)
    return dict(**report,sha256=hashlib.sha256(response.content).hexdigest(),tables=tables,
                parsed=parse_senate_annual(response.text,member['first'],member['last']),current_holdings_eligible=False)


def main():
    CACHE.mkdir(parents=True,exist_ok=True)
    members=json.loads(TARGETS.read_text()); indexes={}; result=[]
    for year in range(2024,date.today().year+1):
        response=requests.get(f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt',timeout=30)
        response.raise_for_status()
        reader=csv.DictReader(io.StringIO(response.text.lstrip('\ufeff')),delimiter='\t')
        if not {'First','Last','StateDst','FilingType','DocID','FilingDate'}.issubset(reader.fieldnames or []): raise ValueError('Invalid House index')
        indexes[year]=list(reader)
    session=None
    for member in members:
        entry={**member,'current_holdings_eligible':False}
        try:
            if member['chamber']=='House':
                documents=[]
                for year,rows in indexes.items():
                    for row in rows:
                        if not matches_house(row,member): continue
                        kind=row['FilingType']
                        folder='ptr-pdfs' if kind=='P' else 'financial-pdfs'
                        documents.append(dict(year=year,doc_id=row['DocID'],type=kind,filed=row['FilingDate'],
                                              source=f'https://disclosures-clerk.house.gov/public_disc/{folder}/{year}/{row["DocID"]}.pdf'))
                annuals=[d for d in documents if d['type'] in ('A','O','W')]
                entry['documents']=documents
                if member['member_id']=='P000197':
                    entry['status']='existing-reviewed-ledger';entry['next_step']='Retain Pelosi ledger; reconcile new documents with existing updater'
                elif annuals:
                    baseline=prepare(dict(member_id=member['member_id'],name=member['first']+' '+member['last'],candidates=annuals))
                    entry['baseline']=baseline
                    baseline_year=baseline.get('reporting_year') or max(d['year'] for d in annuals)
                    cutoff=f'{baseline_year}-12-31'
                    entry['transaction_documents_to_review']=[d for d in documents if d['type']=='P' and datetime.strptime(d['filed'],'%m/%d/%Y').date().isoformat()>cutoff]
                    entry['status']='baseline-and-transactions-review-required'
                    entry['next_step']='Visually verify annual, reconcile transaction dates/owners/amendments and corporate actions'
                else: entry['status']='annual-source-missing'
            else:
                if session is None: session=senate_session()
                reports=senate_reports(session,member)
                entry['documents']=reports
                annuals=[r for r in reports if r['kind']=='annual']
                entry['annual_sources']=[stage_senate_annual(session,r,member) for r in annuals]
                entry['status']='senate-baseline-and-transactions-review-required'
                entry['next_step']='Review annual/new-filer valuation dates and all post-baseline transactions; do not equate filing date to valuation date'
        except Exception as error:
            entry['status']='source-review-required';entry['error']=str(error)
        result.append(entry)
        print(member['name'],entry['status'],len(entry.get('documents',[])),flush=True)
    output={'checked_at':datetime.now(timezone.utc).isoformat(),'scope':'12 fully visible names in user screenshot','members':result}
    (ROOT/'docs/research/priority-holdings-inventory.json').write_text(json.dumps(output,indent=2)+'\n')

if __name__=='__main__': main()
