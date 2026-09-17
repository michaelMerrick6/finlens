"""Reproducible Fields purchase and split evidence; never publishes holdings."""
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from model_holdings_ranges import positive

ROOT = Path(__file__).resolve().parents[1]
IPO_SOURCE = 'https://ir.quantinuum.com/news-releases/news-release-details/quantinuum-announces-pricing-upsized-initial-public-offering-0'
IPO_CLOSING = 'https://ir.quantinuum.com/news-releases/news-release-details/quantinuum-announces-closing-upsized-initial-public-offering'


def purchase_bounds(event, price, offering_price=None):
    """Acquired shares only, conditional on execution within the supplied prices."""
    if event.get('action') != 'P' or event.get('filing_status') != 'New':
        raise ValueError('Only unamended purchases supported')
    amount = event.get('value_bounds')
    if not amount or len(amount) != 2 or not all(positive(x) for x in amount) or amount[0] > amount[1]:
        raise ValueError('Unbounded purchase')
    if price.get('date') != event['date'] or not all(positive(price.get(k)) for k in ('low', 'high')) or price['low'] > price['high']:
        raise ValueError('Missing or invalid execution-day price range')
    low, high = price['low'], price['high']
    if offering_price is not None:
        if not positive(offering_price):
            raise ValueError('Invalid offering price')
        low, high = min(low, offering_price), max(high, offering_price)
    return dict(min_acquired_shares=amount[0]/high, max_acquired_shares=amount[1]/low,
                execution_price_bounds=[low, high], status='conditional-purchase-only',
                current_holdings_eligible=False,
                assumption='Execution price lies within the daily trading range' + (' or the documented IPO offering price.' if offering_price else '.'),
                limitation='Unknown opening balance. These are estimated acquired shares, not total current holdings.')


def market_evidence(ticker, as_of):
    cache = ROOT / 'artifacts/congress-holdings/fields-action-audit' / as_of
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / (ticker + '.json')
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
    params = dict(period1=int(datetime(2025,12,20,tzinfo=timezone.utc).timestamp()),
                  period2=int((datetime.fromisoformat(as_of).replace(tzinfo=timezone.utc)+timedelta(days=1)).timestamp()),
                  interval='1d', events='splits')
    if not path.exists():
        response = requests.get(url, params=params, headers={'User-Agent':'Mozilla/5.0'}, timeout=30)
        response.raise_for_status()
        path.write_text(response.text)
    raw = path.read_bytes()
    c = json.loads(raw)['chart']['result'][0]
    if c['meta']['symbol'] != ticker or c['meta']['currency'] != 'USD' or c['meta']['instrumentType'] not in ['EQUITY','ETF']:
        raise ValueError(f'Unexpected security identity: {ticker}')
    quotes = c['indicators']['quote'][0]
    prices = [dict(date=datetime.fromtimestamp(t,timezone.utc).date().isoformat(), low=quotes['low'][i], high=quotes['high'][i]) for i,t in enumerate(c['timestamp'])]
    splits = [dict(date=datetime.fromtimestamp(e['date'],timezone.utc).date().isoformat(), numerator=e['numerator'], denominator=e['denominator']) for e in c.get('events',{}).get('splits',{}).values()]
    return ticker, dict(source=url, request=params, sha256=hashlib.sha256(raw).hexdigest(),
                        provider_name=c['meta'].get('longName'), splits=splits, prices=prices)


def main():
    as_of = datetime.now(timezone.utc).date().isoformat()
    research = ROOT/'docs/research'
    member = next(m for m in json.loads((research/'priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='F000110')
    reviewed = json.loads((research/'fields-source-check.json').read_text())
    hashes = {s['source']:s['sha256'] for s in reviewed['source_checks']}
    for row in member['positions'] + member['events']:
        if row['source_sha256'] != hashes.get(row['source']):
            raise ValueError('Source changed; repeat Fields visual review')
    tickers = sorted({p['ticker'] for p in member['positions'] if p['ticker'] and p['asset_type'] in ['ST','EF']} | {e['ticker'] for e in member['events'] if e['ticker']})
    with ThreadPoolExecutor(max_workers=4) as pool:
        evidence = dict(pool.map(lambda ticker:market_evidence(ticker,as_of), tickers))
    additions=[]
    for event in member['events']:
        if not event['after_baseline'] or event['position_id']:
            continue
        series = evidence[event['ticker']]
        price = next((p for p in series['prices'] if p['date']==event['date']), {})
        ipo = event['ticker']=='QNT' and event['date']=='2026-06-04'
        if ipo and any(e['date']>event['date'] for e in series['splits']):
            raise ValueError('IPO price needs adjustment to the new split basis')
        estimate=purchase_bounds(event,price,60 if ipo else None)
        additions.append(dict(event_id=event['id'],ticker=event['ticker'],account=event['account'],date=event['date'],
                              source=event['source'],source_sha256=event['source_sha256'],disclosed_range=event['range'],
                              estimate=estimate,identity_sources=[IPO_SOURCE,IPO_CLOSING] if ipo else [],
                              opening_balance_status='not-established',current_holdings_eligible=False))
    for item in evidence.values():
        del item['prices']
    out=dict(member_id='F000110',as_of=as_of,publication_enabled=False,
             scope='Provider split-event check and conditional estimates for four reviewed unmatched purchases; not a complete corporate-action audit.',
             split_evidence=evidence,purchases=additions,
             remaining=['Opening balances for four unmatched account positions.',
                        'Independent review of mergers, spinoffs, transfers and reinvestments; no split events is not proof of no corporate actions.'])
    (research/'fields-purchase-audit.json').write_text(json.dumps(out,indent=2)+'\n')
    for p in additions:
        print(p['ticker'],p['account'],round(p['estimate']['min_acquired_shares'],2),round(p['estimate']['max_acquired_shares'],2),'acquired shares only')
    print('Tickers checked:',len(evidence),'split events:',sum(len(e['splits']) for e in evidence.values()))


if __name__=='__main__':
    main()
