"""Explicit provider aliases and known corporate actions requiring reconciliation.

This small registry is not a complete corporate-action feed.
"""
ALIASES = {
    'BRK.B': {
        'provider_symbol': 'BRK-B',
        'issuer': 'Berkshire Hathaway Inc.',
        'share_class': 'B',
        'source': 'https://www.sec.gov/Archives/edgar/data/1067983/000119312526083899/brka-20251231.htm',
    },
}
ACTIONS = [{
    'id': 'HON-2026-06-29-aerospace-separation',
    'ticker': 'HON',
    'effective_date': '2026-06-29',
    'record_date': '2026-06-15',
    'kind': 'spinoff-and-reverse-split',
    'distributed_ticker': 'HONA',
    'distribution_per_old_share': 0.5,
    'remaining_shares_per_old_share': 0.5,
    'sources': [
        'https://www.honeywellaerospace.com/us/en/company/newsroom/2026/06/honeywell-aerospace-spin-off-begins-trading-nasdaq',
        'https://investor.honeywell.com/news-releases/news-release-details/separation-8-k-supplemental-financial-information-newsletter',
        'https://www.sec.gov/Archives/edgar/data/2089271/000208927126000021/hona-20260627.htm',
    ],
    'status': 'account-entitlement-and-price-basis-review-required',
}]


def provider_symbol(ticker):
    return ALIASES.get(ticker, {}).get('provider_symbol', ticker)


def validate_price_identity(ticker, meta):
    if meta.get('currency') != 'USD' or meta.get('symbol') != provider_symbol(ticker):
        raise ValueError('Unexpected price identity or currency')
    if ticker in ALIASES:
        alias=ALIASES[ticker]
        if meta.get('instrumentType') != 'EQUITY' or meta.get('longName') != alias['issuer']:
            raise ValueError('Unexpected issuer for reviewed alias')
    return dict(filed_symbol=ticker, provider_symbol=provider_symbol(ticker),
                alias_review=ALIASES.get(ticker))


def blocking_actions(ticker, baseline_date, as_of):
    # Post-event baselines already reflect the event. Earlier baselines require
    # entitlement and price-basis review; do not just multiply adjusted prices.
    return [dict(a) for a in ACTIONS
            if a['ticker']==ticker and baseline_date < a['effective_date'] <= as_of]
