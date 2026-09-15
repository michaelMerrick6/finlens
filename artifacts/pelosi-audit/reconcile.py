import json,collections
from pathlib import Path
from pypdf import PdfReader
from ingest_house_official import extract_best_text_transactions
root=Path('artifacts/pelosi-audit');manifest=json.loads((root/'manifest.json').read_text());stored=json.loads((root/'stored-transactions.json').read_text());out=[]
for f in manifest:
 p=root/'sources'/f"{f['index_year']}-{f['DocID']}.pdf"
 text='\n'.join(page.extract_text() or '' for page in PdfReader(p).pages);p.with_suffix('.txt').write_text(text)
 if f['FilingType']!='P':continue
 try:
  rows,_=extract_best_text_transactions(p.read_bytes(),f['DocID'],'Nancy','Pelosi',f['index_year'],[{'id':'P000197','first_name':'Nancy','last_name':'Pelosi'}],[])
  db=[r for r in stored if r['source_url']==f['url']]
  fields=['transaction_type','transaction_date','amount_range']
  sig=lambda r:tuple(r.get(k) for k in fields)
  a=collections.Counter(map(sig,rows));b=collections.Counter(map(sig,db))
  out.append({'year':f['index_year'],'doc':f['DocID'],'url':f['url'],'parsed':len(rows),'stored':len(db),'source_only':list((a-b).elements()),'db_only':list((b-a).elements()),'rows':rows})
 except Exception as e:out.append({'doc':f['DocID'],'error':str(e)})
(root/'ptr-reconciliation.json').write_text(json.dumps(out,indent=2))
print(json.dumps([{k:v for k,v in x.items() if k!='rows'} for x in out if x.get('source_only') or x.get('db_only') or x.get('error')],indent=2));print('Checked',len(out))
