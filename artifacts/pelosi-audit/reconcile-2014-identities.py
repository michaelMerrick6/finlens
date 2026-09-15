"""Source-reviewed 2014 identities. Emits a proposal, never modifies production."""
import hashlib,json,re
from pathlib import Path
p=Path(__file__).resolve().parent
stored=json.loads((p/'stored-transactions.json').read_text())
annual=json.loads((p/'verified-2014-schedule.json').read_text())
rows=[]
for row in annual['rows']:
    ticker=re.search(r'\(([A-Z]+)\)',row['asset']).group(1)
    dbticker='AVGO' if ticker=='BRCM' else ticker
    candidates=[r for r in stored if r['ticker']==dbticker and r['transaction_date'] in row['dates'] and r['transaction_type']==('sell' if row['activity']=='partial_sale' else row['activity']) and int(re.sub(r'[^0-9]','',r['amount_range'].split(' - ')[0]))==row['amount_min']]
    assert len(candidates)==(2 if ticker in ('HTZ','DIS') else 1),(ticker,candidates)
    amendment=[r for r in candidates if '20003320' in r['doc_id']]
    canonical=amendment or candidates
    assert len(canonical)==1
    rows.append({**row,'candidate_db_ids':[r['id'] for r in candidates], 'canonical_db_id':canonical[0]['id'],'superseded_db_ids':[r['id'] for r in candidates if r!=canonical[0]],'historical_ticker':ticker,'entity_mapping_review_required':ticker=='BRCM','reconciliation':'amended option supersedes original' if amendment else 'asset/date/direction/range identity matched'})
result={'status':'reviewed proposal; not applied to production; not portfolio completeness certification','annual_source':annual['source'],'amendment_source':'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2015/20003320.pdf','amendment_sha256':hashlib.sha256((p/'sources/2015-20003320.pdf').read_bytes()).hexdigest(),'amendment_visual_review':'page 1: all six rows expressly Amended; dates, tickers, ranges, call quantities and terms verified','raw_db_rows':sum(len(r['candidate_db_ids']) for r in rows),'canonical_source_events':len(rows),'superseded_duplicates':sum(len(r['superseded_db_ids']) for r in rows),'rows':rows}
assert result['raw_db_rows']==24 and result['superseded_duplicates']==6
(p/'2014-identity-reconciliation.json').write_text(json.dumps(result,indent=2)+'\n')
print({k:v for k,v in result.items() if k!='rows'})
