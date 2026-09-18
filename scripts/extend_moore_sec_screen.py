"""Review inventory for previously omitted 8-K/6-K primary documents and EX-99 links.

Read-only evidence collection. Keyword matches are leads, not verified actions.
"""
import hashlib,json,re,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
ROOT=Path(__file__).resolve().parents[1]
CACHE=ROOT/'artifacts/congress-holdings/moore-sec-review'
HEADERS={'User-Agent':'Vail public disclosure research contact@vail.finance'}
PATTERN=re.compile(r'reverse\s+(?:stock\s+|share\s+)?split|stock\s+split|spin[ -]off|share consolidation|cash in lieu|exchange ratio',re.I)

def get(url,path):
    if path.exists():return path.read_bytes()
    response=requests.get(url,headers=HEADERS,timeout=25)
    response.raise_for_status();path.write_bytes(response.content);time.sleep(.4)
    return response.content

def collect(job):
    ticker,cik,acc,doc,form,dt=job
    source=f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-','')}/{doc}"
    out=dict(ticker=ticker,form=form,date=dt,source=source,documents=[])
    try:
        raw=get(source,CACHE/f'{ticker}-{acc}.html')
        soup=BeautifulSoup(raw,'html.parser')
        targets=[(source,raw)]
        urls=[]
        for a in soup.find_all('a',href=True):
            label=a.get_text(' ',strip=True)
            if re.search(r'\b99(?:\.\d+)?\b',label) and not a['href'].startswith('#'):
                url=urljoin(source,a['href'])
                if url.startswith('https://www.sec.gov/Archives/') and url!=source and url not in urls:urls.append(url)
        for url in urls:
            try:targets.append((url,get(url,CACHE/(hashlib.sha256(url.encode()).hexdigest()+'.html'))))
            except Exception as exc:out['documents'].append(dict(source=url,error=str(exc)))
        for url,b in targets:
            text=' '.join(BeautifulSoup(b,'html.parser').stripped_strings)
            hits=list(PATTERN.finditer(text))
            out['documents'].append(dict(source=url,sha256=hashlib.sha256(b).hexdigest(),keyword_hits=len(hits)))
            if hits:
                # Local working excerpts only; retained report contains source hashes/counts.
                (CACHE/(hashlib.sha256(url.encode()).hexdigest()+'.matches.txt')).write_text('\n'.join(text[max(0,h.start()-180):h.end()+350] for h in hits))
    except Exception as exc:out['error']=str(exc)
    return out

def main():
    original=json.loads((ROOT/'docs/research/moore-sec-corporate-action-screen.json').read_text())
    jobs=[]
    for issuer in original['issuers']:
        if not issuer.get('cik'):continue
        ticker=issuer['ticker'];sub=json.loads((CACHE/f'{ticker}-submissions.json').read_text())['filings']['recent']
        for i,form in enumerate(sub['form']):
            if form not in ('8-K','8-K/A','6-K','6-K/A') or not '2026-01-01'<=sub['filingDate'][i]<=original['as_of']:continue
            jobs.append((ticker,issuer['cik'],sub['accessionNumber'][i],sub['primaryDocument'][i],form,sub['filingDate'][i]))
    print('Primary filing jobs',len(jobs),flush=True)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results=[]
        for r in pool.map(collect,jobs):
            results.append(r)
            if len(results)%25==0:print('Collected',len(results),flush=True)
    out=dict(as_of=original['as_of'],publication_enabled=False,scope='All 2026 8-K/6-K primary documents in captured mapped-issuer inventories plus explicitly linked EX-99 documents. Automated keyword triage only; not every exhibit or fund filing.',filings=results)
    (ROOT/'docs/research/moore-sec-extended-screen.json').write_text(json.dumps(out,indent=2)+'\n')
    print('Done',len(results),'filings',sum(len(r['documents']) for r in results),'documents',flush=True)
if __name__=='__main__':main()
