"""SEC SIC classifications for recently disclosed tickers, kept separate from scraper placeholders."""
import argparse
from datetime import datetime,timedelta,timezone
import os,time
import requests
from dotenv import load_dotenv
from supabase import create_client
from sec_form4_support import SEC_HEADERS

def main():
 load_dotenv('.env.local')
 parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int,default=400);args=parser.parse_args()
 db=create_client(os.environ['SUPABASE_URL'],os.environ['SUPABASE_SERVICE_KEY'])
 session=requests.Session();session.headers.update(SEC_HEADERS)
 r=session.get('https://www.sec.gov/files/company_tickers.json',timeout=30);r.raise_for_status()
 tickers={str(v['ticker']).upper():v for v in r.json().values()}
 cutoff=(datetime.now(timezone.utc)-timedelta(days=90)).date().isoformat();wanted=set()
 offset=0
 while True:
  rows=db.table('politician_trades').select('ticker').gte('published_date',cutoff).order('id').range(offset,offset+999).execute().data or []
  wanted.update(str(t['ticker']).upper() for t in rows if t.get('ticker'))
  if len(rows)<1000:break
  offset+=1000
 existing={};offset=0
 while True:
  rows=db.table('company_research_classifications').select('ticker,verified_at').order('ticker').range(offset,offset+999).execute().data or []
  existing.update({r['ticker']:r['verified_at'] for r in rows})
  if len(rows)<1000:break
  offset+=1000
 recent=(datetime.now(timezone.utc)-timedelta(days=30)).isoformat()
 targets=sorted([t for t in wanted if t in tickers and existing.get(t,'')<recent],key=lambda t:existing.get(t,''))[:args.limit]
 done=0;failed=0
 for ticker in targets:
  info=tickers[ticker];url=f"https://data.sec.gov/submissions/CIK{int(info['cik_str']):010}.json"
  try:
   r=session.get(url,timeout=30);r.raise_for_status();data=r.json()
   # Exact SEC ticker membership prevents assigning an unrelated issuer's industry.
   if ticker not in [str(t).upper() for t in data.get('tickers',[])]:raise ValueError('Ticker/CIK mismatch')
   industry=str(data.get('sicDescription') or '').strip() or None
   db.table('company_research_classifications').upsert({'ticker':ticker,'company_name':data.get('name') or info['title'],'industry':industry,'sic':str(data.get('sic') or ''),'source_url':url,'verified_at':datetime.now(timezone.utc).isoformat()},on_conflict='ticker').execute();done+=1
  except (requests.RequestException,ValueError) as exc:
   failed+=1;print(f'{ticker}: classification unavailable ({type(exc).__name__})',flush=True)
  time.sleep(.25)
 print({'recentTickers':len(wanted),'SECmatches':len(wanted & tickers.keys()),'updated':done,'failed':failed,'remaining':max(0,len([t for t in wanted if t in tickers and existing.get(t,'')<recent])-len(targets))},flush=True)
 return 1 if failed else 0
if __name__=='__main__':raise SystemExit(main())
