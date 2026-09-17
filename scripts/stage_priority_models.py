"""Price-backed candidate ranges for review; publication stays disabled."""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import requests
from model_holdings_ranges import model_range
ROOT=Path(__file__).resolve().parents[1]
DAY=datetime.now(timezone.utc).date().isoformat()
CACHE=ROOT/'artifacts/congress-holdings/prices'/DAY

def stock(p):return p['asset_type'] in ['ST','EF'] or 'Stock' in p['asset_type'] or 'Exchange Traded Fund' in p['asset_type']
def prices(ticker):
    # Source symbols are not silently mapped to another security or ADR.
    url=f'https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker,safe="")}'
    params=dict(period1=int(datetime(2024,12,20,tzinfo=timezone.utc).timestamp()),period2=int((datetime.now(timezone.utc)+timedelta(days=1)).timestamp()),interval='1d')
    path=CACHE/(quote(ticker,safe='')+'.json')
    try:
        if path.exists():raw=json.loads(path.read_text())
        else:
            r=requests.get(url,params=params,headers={'User-Agent':'Mozilla/5.0'},timeout=20);r.raise_for_status();raw=r.json();path.write_text(json.dumps(raw))
        c=raw['chart']['result'][0]
        if c['meta']['currency']!='USD' or c['meta']['symbol']!=ticker:raise ValueError('Unexpected price identity or currency')
        q=c['indicators']['quote'][0]
        result=[dict(date=datetime.fromtimestamp(t,timezone.utc).date().isoformat(),close=q['close'][i],low=q['low'][i],high=q['high'][i]) for i,t in enumerate(c['timestamp'])]
        return ticker,dict(prices=result,source=url,error=None)
    except Exception as e:return ticker,dict(prices=[],source=url,error=str(e))

def main():
    ledger=json.loads((ROOT/'docs/research/priority-holdings-ledger.json').read_text())
    tickers=sorted({p['ticker'] for m in ledger['members'] for p in m['positions'] if p['ticker'] and p['value_bounds'] and not p['charitable'] and stock(p)})
    CACHE.mkdir(parents=True,exist_ok=True);market={}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures=[pool.submit(prices,t) for t in tickers]
        for i,f in enumerate(as_completed(futures),1):
            t,r=f.result();market[t]=r
            if i%50==0:print('Price series',i,'/',len(tickers),flush=True)
    members=[]
    for m in ledger['members']:
        rows=[]
        for p in m['positions']:
            if not p['ticker'] or not stock(p):continue
            related=[e for e in m['events'] if e['after_baseline'] and e['ticker']==p['ticker']]
            reasons=[]
            if p['charitable']:reasons.append('charitable-account-excluded')
            if m['blockers']:reasons.append('source-extraction-blocked')
            # An unallocated event could affect this account; do not ignore it.
            if any(e['review_status'] not in ['matched-account'] for e in related):reasons.append('unresolved-related-transaction')
            if not p['value_bounds']:reasons.append('unknown-starting-value')
            if not reasons:
                events=[e for e in related if e['position_id']==p['id']]
                quote_data=market.get(p['ticker'],{})
                result=model_range(dict(p,date=m['baseline_date']),events,quote_data.get('prices',[]),DAY)
            else:result=dict(status='review-required',reason=', '.join(reasons))
            rows.append(dict(position_id=p['id'],ticker=p['ticker'],name=p['name'],account=p['account'],owner=p['owner'],source=p['source'],page=p['page'],model=result))
        members.append(dict(member_id=m['member_id'],name=m['name'],rows=rows,current_holdings_eligible=False))
        print(m['name'],'conditional candidates',sum(r['model']['status']=='conditional-model' for r in rows),'/',len(rows),flush=True)
    out=dict(as_of=DAY,publication_enabled=False,warning='Candidate calculations only: source-row review, ticker identity and corporate-action reconciliation are incomplete. Do not serve as current holdings.',members=members)
    (ROOT/'docs/research/priority-holdings-models.json').write_text(json.dumps(out,indent=2)+'\n')
if __name__=='__main__':main()
