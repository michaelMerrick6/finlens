"""Read-only Congress holdings coverage and exact-name House source inventory."""
import csv
import io
import json
import os
import unicodedata
from collections import defaultdict, Counter
from datetime import date, datetime, timezone
from pathlib import Path
import requests
from dotenv import load_dotenv
from supabase import create_client
from congress_member_lookup import load_congress_members

ROOT = Path(__file__).resolve().parents[1]
def norm(s):
    return ' '.join(''.join(c for c in unicodedata.normalize('NFKD', s or '') if not unicodedata.combining(c)).upper().split())

def main():
    load_dotenv(ROOT / '.env.local')
    client = create_client(os.getenv('SUPABASE_URL') or os.environ['NEXT_PUBLIC_SUPABASE_URL'], os.getenv('SUPABASE_SERVICE_KEY') or os.environ['SUPABASE_SERVICE_ROLE_KEY'])
    members = [m for m in load_congress_members(client) if m.get('active')]
    stored = defaultdict(list)
    offset = 0
    while True:
        batch = client.table('raw_filings').select('source_document_id,payload').eq('source','house_disclosures').order('id').range(offset,offset+499).execute().data
        for row in batch:
            stored[row['payload'].get('member_id')].append(row)
        if len(batch)<500: break
        offset += len(batch)
    names = Counter((norm(m['last_name']),norm(m['first_name'])) for m in members if m['chamber']=='House')
    candidates = defaultdict(list)
    for year in range(2024,date.today().year+1):
        r=requests.get(f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt',timeout=30);r.raise_for_status()
        reader=csv.DictReader(io.StringIO(r.text.lstrip('\ufeff')),delimiter='\t')
        if not {'Last','First','DocID','FilingType'}.issubset(reader.fieldnames or []):raise ValueError('Invalid House index')
        for row in reader:
            if row['FilingType'] not in ('A','O','W'):continue
            name=(norm(row['Last']),norm(row['First']))
            if names[name]!=1:continue
            candidates[name].append({'year':year,'doc_id':row['DocID'],'type':row['FilingType'],'filed':row.get('FilingDate'),'source':f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}/{row["DocID"]}.pdf'})
    report=[]
    for m in members:
        rows=stored[m['id']]
        report.append({'member_id':m['id'],'name':m['first_name']+' '+m['last_name'],'chamber':m['chamber'],'stored_annual_rows':sum(r['payload'].get('filing_type') in ('A','O') for r in rows),'stored_rows_with_valuation_date':sum(bool(r['payload'].get('period_covered_end')) for r in rows),'reviewed_estimate':m['id']=='P000197','candidates':candidates.get((norm(m['last_name']),norm(m['first_name'])),[]) if m['chamber']=='House' else [],'status':'reviewed-pelosi-baseline' if m['id']=='P000197' else 'baseline-review-required' if rows else 'baseline-import-required'})
    result={'checked_at':datetime.now(timezone.utc).isoformat(),'notes':['Candidates are exact-name source leads, not verified member attribution or complete baselines.','Senate annual holdings require a separate source adapter.','No raw data or estimates were changed.'],'summary':{'active_members':len(report),'by_chamber':dict(Counter(r['chamber'] for r in report)),'members_with_stored_rows':sum(bool(stored[m['id']]) for m in members),'members_with_house_candidates':sum(bool(r['candidates']) for r in report)},'members':report}
    target=ROOT/'docs/research/congress-holdings-coverage.json';target.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result['summary']))
if __name__=='__main__':main()
