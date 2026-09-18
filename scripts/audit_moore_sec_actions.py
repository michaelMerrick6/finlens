"""Read-only, dated SEC corporate-action screening for Moore. Not publication clearance.

Screens selected filing types and primary documents; preserves explicit coverage limits.
Run with the repository research Python environment.
"""
import requests,json,time,re,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
root=Path(__file__).resolve().parents[1];cache=root/'artifacts/congress-holdings/moore-sec-review'
cache.mkdir(parents=True,exist_ok=True)
ticker_path=cache/'sec-company-tickers.json'
if not ticker_path.exists():
 response=requests.get('https://www.sec.gov/files/company_tickers.json',headers={'User-Agent':'Vail public disclosure research contact@vail.finance'},timeout=25)
 response.raise_for_status();ticker_path.write_bytes(response.content)
lookup={x['ticker']:x for x in json.loads(ticker_path.read_text()).values()}
m=next(m for m in json.load(open(root/'docs/research/priority-holdings-models.json'))['members'] if m['member_id']=='M001236')
tickers=sorted({r['ticker'] for r in m['rows'] if (r['model'].get('max_shares') or 0)>0}|{'HON','HY','T','SMPL','IHG','RYCEY'})
headers={'User-Agent':'Vail public disclosure research contact@vail.finance'}
def get(url,path):
 if path.exists():return path.read_bytes()
 r=requests.get(url,headers=headers,timeout=25);r.raise_for_status();path.write_bytes(r.content);time.sleep(.3);return r.content
pattern=re.compile(r'reverse.{0,20}split|stock.split|spin.off|share.consolidation|cash.in.lieu|exchange.ratio|converted into.{0,80}(?:share|cash)',re.I)
def scan(t):
 try:
  entry=lookup.get('BRK-B' if t=='BRK.B' else t)
  if not entry:return dict(ticker=t,status='no-sec-ticker-mapping')
  cik=entry['cik_str'];url=f'https://data.sec.gov/submissions/CIK{cik:010}.json'
  raw=get(url,cache/f'{t}-submissions.json');sub=json.loads(raw);f=sub['filings']['recent']
  docs=[];quarter=False
  for i,form in enumerate(f['form']):
   dt=f['filingDate'][i]
   if not '2026-01-01'<=dt<='2026-09-17':continue
   items=f['items'][i]
   selected=(form in ('10-Q','20-F') and not quarter) or form in ('25','25-NSE','S-4','S-4/A') or (form=='8-K' and any(x in items.split(',') for x in ('2.01','3.03','5.03')))
   if not selected:continue
   if form in ('10-Q','20-F'):quarter=True
   acc=f['accessionNumber'][i];doc=f['primaryDocument'][i]
   source=f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-','')}/{doc}"
   item=dict(form=form,date=dt,items=items,source=source)
   try:
    b=get(source,cache/f'{t}-{acc}.html');text=' '.join(BeautifulSoup(b,'html.parser').stripped_strings)
    hits=list(pattern.finditer(text))
    item.update(sha256=hashlib.sha256(b).hexdigest(),keyword_hit_count=len(hits),keyword_offsets=[h.start() for h in hits])
   except Exception as e:item['error']=str(e)
   docs.append(item)
  return dict(ticker=t,issuer=sub['name'],cik=cik,status='filings-screened-not-blanket-clearance',recent_filings_start=min(f['filingDate']),submissions_source=url,submissions_sha256=hashlib.sha256(raw).hexdigest(),documents=docs)
 except Exception as e:return dict(ticker=t,status='fetch-error',error=str(e))
with ThreadPoolExecutor(max_workers=3) as pool:
 results=list(pool.map(scan,tickers))
out=dict(as_of='2026-09-17',scope='Latest 10-Q/20-F and 2026 8-K items 2.01/3.03/5.03, forms 25 and S-4 from SEC recent submissions. Does not cover every exhibit, 6-K, older submissions archive or fund filing.',publication_enabled=False,issuers=results)
(root/'docs/research/moore-sec-corporate-action-screen.json').write_text(json.dumps(out,indent=2)+'\n')
for r in results:print(r['ticker'],r['status'],len(r.get('documents',[])),sum(d.get('keyword_hit_count',0) for d in r.get('documents',[])))
