"""Account-aware evidence ledger. Unmatched events and amendments cannot become balances.

This is a review artifact, not a publication job. Dollar ranges remain dollar ranges;
no share count, starting zero balance, or current holding is inferred here.
"""
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from house_financial_disclosure_parser import ASSET_TYPE_RE, extract_ticker
ROOT=Path(__file__).resolve().parents[1]

def key(value):return re.sub(r'[^a-z0-9]', '', (value or '').lower())
def iso(value):return datetime.strptime(value,'%m/%d/%Y').date().isoformat()
def charitable(value):return bool(re.search(r'\b(?:foundation|charitable|donor.advised)\b',value,re.I))
def dollar_range(value):
    m=re.fullmatch(r'\$([\d,]+)\s*-\s*\$([\d,]+)',value.strip())
    if m:return [int(x.replace(',','')) for x in m.groups()]
    # None is retained as the literal disclosure; do not silently assert zero shares.
    return None

def match_position(event,positions):
    candidates=[p for p in positions if p['ticker']==event['ticker'] and p['owner']==event['owner']]
    if event.get('account') is not None:
        candidates=[p for p in candidates if key(p['account'])==key(event['account'])]
    # Missing Senate account cannot disambiguate repeated holdings in multiple accounts.
    return candidates[0]['id'] if len(candidates)==1 else None

def house_account(metadata, accounts):
    text=' '.join(metadata)
    found=re.search(r'\bS\s*O:\s*(.*)',text)
    if not found:return None
    tail=key(found[1])
    matches=[a for a in accounts if a and tail.startswith(key(a))]
    if not matches:return 'UNRESOLVED: '+found[1][:180]
    return max(matches,key=lambda a:len(key(a)))

def house_member(member,columns,documents):
    rows=[d for d in columns if d['member_id']==member['member_id']]
    annual=next((r for r in rows if r['kind']=='annual'),None)
    if not annual:return dict(status='scanned-form-review-required',positions=[],events=[],blockers=['Annual checkbox matrix and scanned transactions need visual reconciliation.'])
    date=member['baseline']['valuation_date'] if 'valuation_date' in member['baseline'] else str(member['baseline']['reporting_year'])+'-12-31'
    if annual['sha256']!=member['baseline']['source_sha256']:raise ValueError('Annual changed after inventory; refresh and review required')
    expected_hashes={d['source']:d['sha256'] for d in documents if d['member_id']==member['member_id']}
    for report in rows:
        if report['kind']=='ptr' and expected_hashes.get(report['source'])!=report['sha256']:raise ValueError('PTR changed after inventory; refresh and review required')
    positions=[]
    for h in annual['holdings']:
        account=' > '.join(h['asset_name'].split('⇒')[:-1]).strip()
        positions.append(dict(id=f"annual:{h['row_index']}",name=h['asset_name'].split('⇒')[-1].strip(),ticker=h['ticker'],owner=h['owner'] or 'not-stated',account=account,
            asset_type=h['asset_type_code'],range=h['value_range'],value_bounds=[0,0] if h['value_range']=='None' else dollar_range(h['value_range']),zero_basis='explicit-house-none' if h['value_range']=='None' else None,zero_basis_source='https://ethics.house.gov/financial-disclosure/' if h['value_range']=='None' else None,source=annual['source'],source_sha256=annual['sha256'],page=h['source_page'],charitable=charitable(account),events=[]))
    accounts=set(p['account'] for p in positions)
    events=[];blockers=[str(x) for r in rows for x in r['issues']]
    for report in rows:
        if report['kind']!='ptr':continue
        for i,e in enumerate(report['events']):
            c=e['cells'];dt=iso(c['Date']);asset_type=ASSET_TYPE_RE.search(c['Asset'])
            event=dict(id=report['source']+f'#row={i+1}',source=report['source'],source_sha256=report['sha256'],page=e['page'],date=dt,owner=c['Owner'] or 'not-stated',ticker=extract_ticker(c['Asset']),name=c['Asset'],
                account=house_account(e['metadata'],accounts),asset_type=asset_type[1] if asset_type else None,action=c['Transaction Type'],range=c['Amount'],value_bounds=dollar_range(c['Amount']),
                filing_status=(re.search(r'F\s*S:\s*(New|Amended|Deleted)', ' '.join(e['metadata'])) or [None,'Unknown'])[1],
                amendment='Amended' in ' '.join(e['metadata']),original_transaction_id=c['ID'] or None,metadata=e['metadata'])
            event['after_baseline']=dt>date
            event['position_id']=match_position(event,positions) if event['ticker'] else None
            event['charitable']=charitable(event['account'] or '')
            events.append(event)
    return finish(member,date,positions,events,blockers)

def senate_member(member,documents):
    annuals=[a for a in member['annual_sources'] if a['parsed'].get('valuation_date')]
    if not annuals:return dict(status='valuation-date-review-required',positions=[],events=[],blockers=['New-filer valuation date is not an annual year-end; no roll-forward date is assumed.'])
    annual=max(annuals,key=lambda a:a['parsed']['valuation_date']);date=annual['parsed']['valuation_date']
    positions=[]
    for h in annual['parsed']['holdings']:
        if h['is_container']:continue
        account=' > '.join(h['account_path'])
        positions.append(dict(id='annual:'+h['row_id'],name=h['asset_name'],ticker=h['ticker'],owner=h['owner'],account=account,asset_type=h['asset_type'],range=h['value_range'],value_bounds=dollar_range(h['value_range']),source=annual['source'],source_sha256=annual['sha256'],page=None,charitable=charitable(account),events=[]))
    events=[];blockers=[]
    for doc in documents:
        if doc['member_id']!=member['member_id']:continue
        for table in doc.get('tables',[]):
            if not table or table[0]!=['#','Transaction Date','Owner','Ticker','Asset Name','Asset Type','Type','Amount','Comment']:continue
            for row in table[1:]:
                if len(row)!=9:raise ValueError('Incomplete Senate transaction')
                n,dt,owner,ticker,name,asset_type,action,amount,comment=row
                event=dict(id=doc['source']+'#row='+n,source=doc['source'],source_sha256=doc['sha256'],page=None,date=iso(dt),owner=owner,ticker=None if ticker=='--' else ticker,name=name,asset_type=asset_type,action=action,range=amount,value_bounds=dollar_range(amount),account=None,amendment=False,metadata=[comment])
                event['after_baseline']=event['date']>date;event['position_id']=match_position(event,positions) if event['ticker'] else None
                event['charitable']=False;events.append(event)
    # Filing title, not URL, determines amendments. Do not overwrite older same-year annuals.
    if any('Amendment' in a['title'] and a['parsed'].get('valuation_date')==date for a in annuals):blockers.append('Annual amendments require source reconciliation.')
    return finish(member,date,positions,events,blockers)

def finish(member,date,positions,events,blockers):
    by_id={p['id']:p for p in positions}
    fingerprints={}
    for e in events:
        if not e['after_baseline']:e['review_status']='already-in-baseline-period';continue
        if e.get('filing_status')=='Deleted':e['review_status']='deletion-requires-original-row';continue
        if e['amendment']:e['review_status']='amendment-requires-original-row';continue
        fingerprint=(e['date'],e['owner'],e['account'],e['name'],e['action'],e['range'])
        if fingerprint in fingerprints and fingerprints[fingerprint]['source']!=e['source']:
            e['review_status']='possible-cross-filing-duplicate'
            fingerprints[fingerprint]['review_status']='possible-cross-filing-duplicate'
        else:
            e['review_status']='matched-account' if e['position_id'] else 'unmatched-baseline-or-account'
            fingerprints[fingerprint]=e
        if e['position_id']:by_id[e['position_id']]['events'].append(e['id'])
    # Corrections are control records, not a second purchase/sale. Until an original
    # is positively linked, also quarantine plausible originals from balance use.
    for correction in events:
        if correction.get('filing_status') not in ['Amended','Deleted']:continue
        candidates=[e for e in events if e['source']!=correction['source'] and e.get('filing_status')=='New' and all(e.get(k)==correction.get(k) for k in ['ticker','date','action','range','owner'])]
        correction['candidate_original_ids']=[e['id'] for e in candidates]
        for e in candidates:e['review_status']='possible-original-of-correction'
    counts=Counter(e['review_status'] for e in events)
    for p in positions:
        p['current_holdings_eligible']=False
        p['review_status']='charitable-account-excluded' if p['charitable'] else 'balance-and-corporate-actions-review-required'
    return dict(status='ledger-staged',baseline_date=date,positions=positions,events=events,blockers=blockers,counts=dict(counts),current_holdings_eligible=False)

def main():
    inventory=json.loads((ROOT/'docs/research/priority-holdings-inventory.json').read_text())['members']
    columns=json.loads((ROOT/'docs/research/priority-holdings-columns.json').read_text())
    docs=json.loads((ROOT/'docs/research/priority-holdings-transactions.json').read_text())['documents']
    members=[]
    for m in inventory:
        if m['member_id']=='P000197':result=dict(status='existing-reviewed-pelosi-ledger',positions=[],events=[],blockers=[])
        elif m['chamber']=='House':result=house_member(m,columns,docs)
        else:result=senate_member(m,docs)
        result.update(member_id=m['member_id'],name=m['name'],current_holdings_eligible=False)
        members.append(result)
        print(m['name'],result['status'],len(result['positions']),len(result['events']),result.get('counts',{}))
    output=dict(created_at=datetime.now(timezone.utc).isoformat(),publication_enabled=False,members=members)
    (ROOT/'docs/research/priority-holdings-ledger.json').write_text(json.dumps(output,indent=2)+'\n')
if __name__=='__main__':main()
