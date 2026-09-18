"""Build a dated, partial presentation snapshot; never certify a full portfolio."""
import json, math
from datetime import datetime, timezone, date
from holdings_market_identity import provider_symbol, validate_price_identity
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def build(data,ledger,review,closing_prices=None):
    m=next(m for m in data['members'] if m['member_id']=='M001236')
    l=next(m for m in ledger['members'] if m['member_id']=='M001236')
    hashes={s['source']:s['sha256'] for s in review['sources']}
    for r in l['positions']+l['events']:
        if hashes.get(r['source'])!=r['source_sha256']:raise ValueError('Source review is stale')
    positions={p['id']:p for p in l['positions']}
    groups={};unresolved=[];closed=[]
    for row in m['rows']:
        p=positions[row['position_id']];model=row['model']
        if model['status']!='conditional-model':
            unresolved.append(dict(ticker=row['ticker'],name=row['name'],reason='Conflicting year-end and transaction disclosures.' if row['ticker']=='HY' else 'Corporate-action scenario exists, but retained shares and valuation are not established.',source=row['source'],page=row['page']))
            continue
        if model['max_shares']==0:
            closed.append(dict(ticker=row['ticker'],account=row['account'],source=row['source'],page=row['page']));continue
        for k in ['min_shares','max_shares','min_value','max_value']:
            if not isinstance(model.get(k),(int,float)) or not math.isfinite(model[k]) or model[k]<0:raise ValueError('Invalid model bound')
        if model['min_value']>model['max_value'] or model['min_shares']>model['max_shares']:raise ValueError('Reversed bounds')
        model=dict(model)
        if model['latest_price']['date']>=data['as_of']:
            price=(closing_prices or {}).get(row['ticker'])
            if not price or price['date']>=data['as_of'] or not math.isfinite(price['close']) or price['close']<=0:raise ValueError('Missing completed closing price')
            model.update(latest_price=price,min_value=model['min_shares']*price['close'],max_value=model['max_shares']*price['close'])
        if (date.fromisoformat(data['as_of'])-date.fromisoformat(model['latest_price']['date'])).days>7:raise ValueError('Stale closing price')
        g=groups.setdefault(row['ticker'],dict(ticker=row['ticker'],name=row['name'],minShares=0,maxShares=0,minValue=0,maxValue=0,accounts=[]))
        for dest,origin in [('minShares','min_shares'),('maxShares','max_shares'),('minValue','min_value'),('maxValue','max_value')]:g[dest]+=model[origin]
        g['accounts'].append(dict(account=row['account'],source=row['source'],page=row['page'],reportedRange=p['range'],priceDate=model['latest_price']['date'],assumptions=model['assumptions']))
    for p in m['unresolved_positions']:
        first=p['events'][0]
        unresolved.append(dict(ticker=p['ticker'],name=p['name'],reason='Disclosed transactions are recorded, but the opening share balance is unknown.',source=first['source'],page=first['page']))
    rows=sorted(groups.values(),key=lambda r:(-(r['minValue']+r['maxValue'])/2,r['ticker']))
    return dict(asOf=data['as_of'],baselineDate=l['baseline_date'],status='partial-estimate',fullyVerified=False,
                rows=rows,unresolved=unresolved,closed=closed,
                subtotalMin=sum(r['minValue'] for r in rows),subtotalMax=sum(r['maxValue'] for r in rows))

def main():
    r=ROOT/'docs/research'
    data=json.loads((r/'priority-holdings-models.json').read_text())
    member=next(m for m in data['members'] if m['member_id']=='M001236');prices={}
    for row in member['rows']:
        if row['model'].get('latest_price') and row['model']['latest_price']['date']>=data['as_of']:
            raw=json.loads((ROOT/'artifacts/congress-holdings/prices'/data['as_of']/(provider_symbol(row['ticker'])+'.json')).read_text())['chart']['result'][0]
            validate_price_identity(row['ticker'],raw['meta'])
            candidates=[dict(date=datetime.fromtimestamp(t,timezone.utc).date().isoformat(),close=c) for t,c in zip(raw['timestamp'],raw['indicators']['quote'][0]['close']) if c is not None]
            prices[row['ticker']]=max((p for p in candidates if p['date']<data['as_of']),key=lambda p:p['date'])
    out=build(data,json.loads((r/'priority-holdings-ledger.json').read_text()),json.loads((r/'moore-source-review.json').read_text()),prices)
    (ROOT/'src/data/moore-holdings-snapshot.json').write_text(json.dumps(out,indent=2)+'\n')
    print(len(out['rows']),'modeled stocks;',len(out['unresolved']),'unresolved;',len(out['closed']),'conditional zero accounts')
if __name__=='__main__':main()
