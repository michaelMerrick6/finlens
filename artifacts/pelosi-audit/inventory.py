import csv,io,json,os,requests,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from dotenv import load_dotenv
load_dotenv('.env.local');root=Path('artifacts/pelosi-audit');root.joinpath('sources').mkdir(exist_ok=True)
def year_scan(y):
 u=f'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{y}FD.txt'
 try:
  r=requests.get(u,timeout=30);r.raise_for_status();rows=list(csv.DictReader(io.StringIO(r.content.decode('utf-8-sig')),delimiter='\t'))
  return {'year':y,'url':u,'status':'checked','filings':[{**x,'index_year':y} for x in rows if x.get('Last','').strip().lower()=='pelosi' and x.get('First','').strip().lower().startswith('nancy')]}
 except Exception as e:return {'year':y,'status':'unavailable','error':str(e)}
with ThreadPoolExecutor(max_workers=4) as pool: years=list(pool.map(year_scan,range(2008,2027)))
root.joinpath('indexes.json').write_text(json.dumps(years,indent=2));filings=[f for y in years for f in y.get('filings',[])]
k=os.environ.get('SUPABASE_SERVICE_KEY') or os.environ['SUPABASE_SERVICE_ROLE_KEY'];u=os.environ.get('SUPABASE_URL') or os.environ['NEXT_PUBLIC_SUPABASE_URL'];stored=[]
for offset in range(0,100000,1000):
 r=requests.get(u+'/rest/v1/politician_trades',headers={'apikey':k,'Authorization':'Bearer '+k},params={'select':'*','member_id':'eq.P000197','order':'id','offset':offset,'limit':1000},timeout=30);r.raise_for_status();page=r.json();stored+=page
 if len(page)<1000:break
root.joinpath('stored-transactions.json').write_text(json.dumps(stored,indent=2))
counts=Counter(x.get('source_url') for x in stored)
def download(f):
 y=f['index_year'];d=f['DocID'];kind='ptr-pdfs' if f['FilingType']=='P' else 'financial-pdfs';url=f'https://disclosures-clerk.house.gov/public_disc/{kind}/{y}/{d}.pdf'
 out={**f,'url':url,'stored_rows':counts[url]}
 try:
  r=requests.get(url,timeout=30);r.raise_for_status()
  if not r.content.startswith(b'%PDF'):raise ValueError('Not PDF')
  root.joinpath('sources',f'{y}-{d}.pdf').write_bytes(r.content);out['sha256']=hashlib.sha256(r.content).hexdigest();out['download']='ok'
 except Exception as e:out['download']='failed';out['error']=str(e)
 return out
with ThreadPoolExecutor(max_workers=4) as pool: manifest=list(pool.map(download,filings))
root.joinpath('manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps({'years':[(y['year'],y['status'],len(y.get('filings',[]))) for y in years],'types':dict(Counter(f['FilingType'] for f in filings)),'stored':len(stored),'ptr_missing':sum(f['FilingType']=='P' and not f['stored_rows'] for f in manifest),'downloads_failed':sum(f['download']!='ok' for f in manifest)},indent=2))
