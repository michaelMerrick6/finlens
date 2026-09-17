"""Price-backed candidate ranges for review; publication stays disabled."""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
import requests
from model_holdings_ranges import model_range
from reconcile_priority_holdings import key
from holdings_market_identity import provider_symbol, validate_price_identity, blocking_actions
ROOT=Path(__file__).resolve().parents[1]
DAY=datetime.now(timezone.utc).date().isoformat()
CACHE=ROOT/'artifacts/congress-holdings/prices'/DAY

def stock(p):
    kind=p.get('asset_type') or ''
    return kind in ['ST','EF'] or 'Stock' in kind or 'Exchange Traded Fund' in kind
def could_affect_position(event, position):
    if event.get('balance_effect')=='none':return False
    if event['ticker'] != position['ticker'] or not event['after_baseline']:return False
    # A correction can change accounts: quarantine it until its original is linked.
    if (event.get('filing_status') in ['Amended','Deleted'] and event.get('correction_resolution')!='reviewed-original-link') or event['review_status']=='possible-original-of-correction':return True
    a,b=event.get('account'),position.get('account')
    if a and b and not a.startswith('UNRESOLVED:') and key(a)!=key(b):return False
    # Unknown ownership must not be treated as a different person.
    a,b=event.get('owner'),position.get('owner')
    known=lambda value:value and value not in ['not-stated','Unknown']
    if known(a) and known(b) and a!=b:return False
    return True

def unresolved_positions(member):
    groups={}
    for event in member['events']:
        if event.get('balance_effect')=='none':continue
        if not event['after_baseline'] or event['position_id'] or not event['ticker'] or not stock(event):continue
        group=(event['ticker'],event.get('owner'),event.get('account'))
        row=groups.setdefault(group,dict(ticker=event['ticker'],name=event['name'],owner=event.get('owner'),account=event.get('account'),status='starting-balance-unresolved',current_holdings_eligible=False,events=[]))
        row['events'].append(dict(id=event['id'],date=event['date'],action=event['action'],range=event['range'],source=event['source'],page=event['page']))
    return list(groups.values())

def prices(ticker):
    # Only reviewed provider aliases are allowed; preserve the filed symbol.
    symbol=provider_symbol(ticker)
    url=f'https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol,safe="")}'
    params=dict(period1=int(datetime(2024,12,20,tzinfo=timezone.utc).timestamp()),period2=int((datetime.now(timezone.utc)+timedelta(days=1)).timestamp()),interval='1d')
    path=CACHE/(quote(symbol,safe='')+'.json')
    try:
        if path.exists():raw=json.loads(path.read_text())
        else:
            r=requests.get(url,params=params,headers={'User-Agent':'Mozilla/5.0'},timeout=20);r.raise_for_status();raw=r.json();path.write_text(json.dumps(raw))
        c=raw['chart']['result'][0]
        identity=validate_price_identity(ticker,c['meta'])
        q=c['indicators']['quote'][0]
        result=[dict(date=datetime.fromtimestamp(t,timezone.utc).date().isoformat(),close=q['close'][i],low=q['low'][i],high=q['high'][i]) for i,t in enumerate(c['timestamp'])]
        return ticker,dict(prices=result,source=url,error=None,identity=identity)
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
            related=[e for e in m['events'] if could_affect_position(e,p)]
            actions=blocking_actions(p['ticker'],m['baseline_date'],DAY)
            reasons=['corporate-action-reconciliation-required'] if actions else []
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
            if actions:result['corporate_actions']=actions
            if provider_symbol(p['ticker'])!=p['ticker']:
                result['price_identity']=market.get(p['ticker'],{}).get('identity')
            rows.append(dict(position_id=p['id'],ticker=p['ticker'],name=p['name'],account=p['account'],owner=p['owner'],source=p['source'],page=p['page'],model=result))
        members.append(dict(member_id=m['member_id'],name=m['name'],rows=rows,unresolved_positions=unresolved_positions(m),current_holdings_eligible=False))
        print(m['name'],'conditional candidates',sum(r['model']['status']=='conditional-model' for r in rows),'/',len(rows),flush=True)
    out=dict(as_of=DAY,publication_enabled=False,warning='Candidate calculations only: source-row review, ticker identity and corporate-action reconciliation are incomplete. Do not serve as current holdings.',members=members)
    (ROOT/'docs/research/priority-holdings-models.json').write_text(json.dumps(out,indent=2)+'\n')
if __name__=='__main__':main()
