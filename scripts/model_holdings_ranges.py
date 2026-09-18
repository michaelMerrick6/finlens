"""Conditional dollar-to-share bounds for review. No midpoint is a reported share count.

A caller must supply prices on ONE consistent split-adjusted (not dividend-adjusted)
basis. Unknown starting balances and unmatched/corrected events cannot be modeled.
"""
import math


def positive(value):return isinstance(value,(int,float)) and math.isfinite(value) and value>0

def model_range(baseline,events,prices,as_of):
    """Pure model for one identified account/security; no I/O or ticker guessing."""
    def fail(reason):return dict(status='review-required',reason=reason,min_shares=None,max_shares=None)
    if not baseline.get('source') or baseline['date']>as_of:return fail('invalid-baseline')
    bounds=baseline.get('value_bounds')
    explicit_zero=bounds==[0,0] and baseline.get('zero_basis')=='explicit-house-none'
    if not explicit_zero and (not bounds or len(bounds)!=2 or not all(positive(x) for x in bounds) or bounds[0]>bounds[1]):return fail('unbounded-or-unknown-baseline')
    active=[e for e in events if e.get('balance_effect')!='none' and baseline['date']<e['date']<=as_of]
    # An explicitly closed baseline with no later changes needs no market quote.
    # Delisted/obsolete symbols must not turn a known disclosed zero into unknown.
    if explicit_zero and not active:
        return dict(status='conditional-model',min_shares=0,max_shares=0,min_value=0,max_value=0,
                    baseline_price=None,latest_price=None,trace=[],
                    assumptions=['Explicit House year-end None carried forward with no supplied subsequent changes.'],
                    current_holdings_eligible=False)
    # Latest available close on/before year-end; reject stale series and future rows.
    candidates=[p for p in prices if p['date']<=baseline['date'] and positive(p.get('close'))]
    if not candidates:return fail('missing-baseline-price')
    start=max(candidates,key=lambda p:p['date'])
    from datetime import date
    if (date.fromisoformat(baseline['date'])-date.fromisoformat(start['date'])).days>7:return fail('stale-baseline-price')
    low,high=(x/start['close'] for x in bounds)
    by_date={p['date']:p for p in prices};seen=set();trace=[]
    for e in sorted(events,key=lambda e:(e['date'],e['id'])):
        if e.get('balance_effect')=='none':continue
        if e.get('position_id')!=baseline['id']:return fail('unmatched-account')
        if e['date']<=baseline['date'] or e['date']>as_of:continue
        if e['id'] in seen:return fail('duplicate-event')
        seen.add(e['id'])
        if e.get('review_status')!='matched-account':return fail('unresolved-event')
        if e.get('filing_status','New')!='New' and not (e.get('filing_status')=='Amended' and e.get('correction_resolution')=='reviewed-original-link' and e.get('replaces_event_id')):return fail('correction-needs-resolution')
        if e['action'] not in ['P','S','S (partial)','S (full)','Purchase','Sale (Partial)','Sale (Full)']:return fail('unsupported-position-change')
        amount=e.get('value_bounds');p=by_date.get(e['date'])
        if not amount or len(amount)!=2 or not all(positive(x) for x in amount) or amount[0]>amount[1]:return fail('unbounded-transaction-value')
        if not p or not all(positive(p.get(k)) for k in ['low','high']) or p['low']>p['high']:return fail('missing-transaction-price')
        # Daily trading range acknowledges unknown execution price, rather than
        # treating the closing price or bracket midpoint as an exact execution.
        min_delta,max_delta=amount[0]/p['high'],amount[1]/p['low']
        if e['action'] in ['P','Purchase']:low+=min_delta;high+=max_delta
        else:
            if high<min_delta:return fail('sale-exceeds-modeled-balance')
            previous_low=low
            low=max(0,low-max_delta);high-=min_delta
            # Full sale is account-scoped only: caller matched this exact account.
            if e['action'] in ['S (full)','Sale (Full)']:
                if max_delta < previous_low:return fail('full-sale-inconsistent-with-balance')
                low=high=0
        trace.append(dict(event=e['id'],min_shares=low,max_shares=high))
    latest=[p for p in prices if p['date']<=as_of and positive(p.get('close'))]
    if not latest:return fail('missing-latest-price')
    end=max(latest,key=lambda p:p['date'])
    if (date.fromisoformat(as_of)-date.fromisoformat(end['date'])).days>7:return fail('stale-latest-price')
    return dict(status='conditional-model',min_shares=low,max_shares=high,min_value=low*end['close'],max_value=high*end['close'],baseline_price=start,latest_price=end,trace=trace,
        assumptions=['Annual dollar bracket converted at year-end closing price.','Trade quantities bounded using daily high/low on the same split basis.','Only supplied, reconciled events included; unreported changes remain possible.','Corporate actions other than splits require separate reconciliation.'],current_holdings_eligible=False)
