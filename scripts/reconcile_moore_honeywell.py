"""Source-locked HON/HONA entitlement scenario, not confirmed ownership.

Keep separate from the general price model: Yahoo's back-adjusted history
must not be multiplied by the legal share ratios a second time.
"""
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = 'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf'
HASH = '22be0a0bde4b37c0df6d654990a7c1121d0810b4928ccf7600e7bac87a049483'
PRICE_SOURCE = 'https://www.sec.gov/Archives/edgar/data/773840/000077384026000023/hon-20260305.htm'
ACTION_SOURCES = [
    'https://www.honeywellaerospace.com/us/en/company/newsroom/2026/06/honeywell-aerospace-spin-off-begins-trading-nasdaq',
    'https://investor.honeywell.com/reverse-stock-split',
]


def reconcile(member, as_of):
    if member['member_id'] != 'M001236' or member['baseline_date'] != '2025-12-31':
        raise ValueError('Unreviewed member or baseline')
    positions = [p for p in member['positions'] if p['ticker'] in ('HON', 'HONA')]
    if len(positions) != 1:
        raise ValueError('Changed Honeywell baseline inventory')
    p = positions[0]
    expected = dict(id='annual:55', ticker='HON', account='TradeWinds',
                    owner='not-stated', value_bounds=[15001, 50000],
                    source=SOURCE, source_sha256=HASH, page=5, charitable=False)
    if any(p.get(k) != v for k, v in expected.items()):
        raise ValueError('Changed source row requires review')
    # Deliberately require a new review after this evidence snapshot. Do not
    # silently carry an entitlement scenario forward forever.
    if as_of != '2026-09-17':
        raise ValueError('New as-of date requires source and action review')
    if any(e.get('ticker') in ('HON', 'HONA') and e.get('date', '') > member['baseline_date']
           for e in member['events']):
        raise ValueError('Additional Honeywell transaction requires reconciliation')
    lower, upper = [value / 195.09 for value in p['value_bounds']]
    # Rounding the lower endpoint down includes cash-in-lieu outcomes; retaining
    # a fractional upper endpoint also allows broker-level fractional custody.
    # Cash proceeds themselves are outside this share-only scenario.
    scenario = dict(min_shares=math.floor(lower / 2), max_shares=upper / 2)
    return dict(member_id=member['member_id'], as_of=as_of,
                status='conditional-entitlement-reconstruction',
                current_holdings_eligible=False, publication_enabled=False,
                baseline=dict(source=SOURCE, sha256=HASH, page=5, date=member['baseline_date'],
                              value_bounds=p['value_bounds'], unadjusted_close=195.09,
                              price_source=PRICE_SOURCE, implied_old_share_range=[lower, upper]),
                action=dict(effective_date='2026-06-29', record_date='2026-06-15',
                            HON_per_old_share=0.5, HONA_per_old_share=0.5, sources=ACTION_SOURCES),
                positions=[dict(ticker=t, account=p['account'], **scenario) for t in ('HON', 'HONA')],
                assumptions=[
                    'Annual value bracket is valued at the stated year-end closing price.',
                    'No omitted transactions, transfers or reinvestments after the annual baseline.',
                    'Original shares remained held through the record date and both resulting positions were retained.',
                    'Fractional cash-in-lieu is not included; share bounds conservatively allow rounding or fractional custody.',
                ],
                limitation='Issuer conversion and price basis are sourced; Moore account entitlement and current ownership remain conditional.')


def main():
    ledger = json.loads((ROOT/'docs/research/priority-holdings-ledger.json').read_text())
    member = next(m for m in ledger['members'] if m['member_id'] == 'M001236')
    report = reconcile(member, '2026-09-17')
    (ROOT/'docs/research/moore-honeywell-reconciliation.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report['positions']))


if __name__ == '__main__':
    main()
