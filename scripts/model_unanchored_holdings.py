"""Preserve disclosed share changes without inventing an opening balance.

Caller supplies reviewed, deduplicated transactions and prices on a consistent
split basis. This models reporting-household flow, not an identified account.
"""
from model_holdings_ranges import positive


def model_unanchored_flow(events, prices, baseline_date, as_of):
    def fail(reason):
        return dict(status='review-required', reason=reason, current_holdings_eligible=False)
    if baseline_date > as_of:
        return fail('invalid-period')
    active=[e for e in events if baseline_date < e['date'] <= as_of and e.get('balance_effect') != 'none']
    if not active:
        return fail('no-transactions-in-period')
    if len({e.get('ticker') for e in active}) != 1 or not active[0].get('ticker'):
        return fail('mixed-or-missing-security')
    by_date={p['date']:p for p in prices}
    seen=set(); low=high=0; trace=[]
    for e in sorted(active, key=lambda x:(x['date'],x['id'])):
        if e['id'] in seen:
            return fail('duplicate-event')
        seen.add(e['id'])
        if e.get('filing_status') != 'New':
            return fail('correction-needs-resolution')
        if e['action'] not in ['P','S','S (partial)']:
            return fail('unsupported-position-change')
        amount=e.get('value_bounds')
        if not amount or len(amount)!=2 or not all(positive(v) for v in amount) or amount[0]>amount[1]:
            return fail('unbounded-transaction-value')
        price=by_date.get(e['date'])
        if not price or not all(positive(price.get(k)) for k in ('low','high')) or price['low']>price['high']:
            return fail('missing-transaction-price')
        minimum, maximum=amount[0]/price['high'],amount[1]/price['low']
        if e['action']=='P': low+=minimum; high+=maximum
        else: low-=maximum; high-=minimum
        # Keep negative net flow. Clamping it to zero would manufacture a balance.
        trace.append(dict(event=e['id'], source=e['source'], page=e['page'],
                          date=e['date'], action=e['action'], min_net_share_change=low,
                          max_net_share_change=high, price=price))
    return dict(status='conditional-flow-only', opening_balance='unknown',
                min_net_share_change=low, max_net_share_change=high,
                min_current_shares=None, max_current_shares=None,
                eligible_for_portfolio_total=False, eligible_for_ranking=False,
                current_holdings_eligible=False, trace=trace,
                assumptions=[
                    'Dollar brackets converted using daily highs/lows on one split-adjusted basis.',
                    'Flow includes only supplied disclosures; no inferred opening balance or full exit.',
                    'Account ownership and non-split corporate actions require separate reconciliation.',
                ])
