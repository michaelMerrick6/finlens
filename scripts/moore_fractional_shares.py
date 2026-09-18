"""Conservative account-level allowances for two reviewed reverse splits.

Input share bounds already use split-adjusted prices. Never apply the ratio again.
These allowances include possible broker fractional custody; they do not claim
that Moore received an issuer-level rounding benefit or cash payment.
"""
import copy
import math

ACTIONS={
    'GNPX': dict(date='2026-07-16',numerator=1,denominator=22,policy='round-up',
                 source='https://www.sec.gov/Archives/edgar/data/1595248/000143774926027846/gnpx20260630_10q.htm'),
    'TZA': dict(date='2026-07-15',numerator=1,denominator=10,policy='cash-in-lieu',
                source='https://www.direxion.com/press-release/direxion-to-split-nine-etfs'),
}


def apply_allowance(model,ticker,baseline_date,events,as_of,evidence):
    if model['status']!='conditional-model' or ticker not in ACTIONS:
        return model
    action=ACTIONS[ticker]
    if not baseline_date<action['date']<=as_of:
        return model
    def fail(reason):
        return dict(status='review-required',reason=reason,current_holdings_eligible=False)
    expected=dict(date=action['date'],numerator=action['numerator'],denominator=action['denominator'])
    actual=evidence.get('symbols',{}).get(ticker,{}).get('splits',[])
    if evidence.get('as_of')!=as_of or actual!=[expected]:
        return fail('refresh-reviewed-split-evidence')
    # This narrow review covers positions with no post-split transactions.
    # A later sale needs action-time balance reconstruction, not end-date rounding.
    if any(e.get('balance_effect')!='none' and action['date']<=e['date']<=as_of for e in events):
        return fail('post-split-transaction-needs-fractional-reconciliation')
    out=copy.deepcopy(model)
    if out.get('fractional_share_review'):
        return fail('fractional-allowance-already-applied')
    low,high=out['min_shares'],out['max_shares']
    if action['policy']=='round-up':high=math.ceil(high)
    else:low=math.floor(low)
    out.update(min_shares=low,max_shares=high,
               min_value=low*out['latest_price']['close'],max_value=high*out['latest_price']['close'])
    out['fractional_share_review']=dict(action,pre_allowance_bounds=[model['min_shares'],model['max_shares']],
                                      split_ratio_applied_again=False,cash_proceeds_included=False)
    out['assumptions'].append('Single modeled account; fractional custody or issuer rounding/cash-out allowed conservatively. Actual broker treatment and cash proceeds are unknown.')
    return out
