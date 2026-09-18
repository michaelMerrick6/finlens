"""Counterfactual long-only roll-forward, never evidence of a zero baseline."""
import math
from model_unanchored_holdings import model_unanchored_flow


def model_zero_start_scenario(events, prices, start, end):
    flow = model_unanchored_flow(events, prices, start, end)
    if flow['status'] != 'conditional-flow-only': return flow
    same_day = {}
    for row in flow['trace']:
        same_day.setdefault(row['date'], set()).add(row['action'][0])
    if any(len(actions)>1 for actions in same_day.values()):
        return dict(status='review-required', reason='unknown-intraday-order', current_holdings_eligible=False)
    low=high=previous_low=previous_high=0
    trace=[]
    for row in flow['trace']:
        # Recover the signed interval increment from the unbounded flow trace.
        low += row['min_net_share_change'] - previous_low
        high += row['max_net_share_change'] - previous_high
        previous_low=row['min_net_share_change']; previous_high=row['max_net_share_change']
        if not math.isfinite(low) or not math.isfinite(high) or high < 0:
            return dict(status='inconsistent-zero-start-scenario', date=row['date'],
                        reason='Reported sales exceed possible long-only holdings under the assumed baseline.',
                        current_holdings_eligible=False)
        low=max(0,low)
        trace.append(dict(date=row['date'],source=row['source'],event=row['event'],
                          min_scenario_shares=low,max_scenario_shares=high))
    return dict(status='hypothetical-zero-start', assumed_opening_shares=0,
                current_holdings_eligible=False,eligible_for_portfolio_total=False,
                min_scenario_shares=low,max_scenario_shares=high,trace=trace,
                assumptions=['Zero shares before first recorded purchase; this is not established by the filings.',
                             'Complete supplied transaction history, no short positions, transfers or reinvestments.',
                             'Daily high/low price conversion on a consistent split basis; non-split actions require separate review.'])
