#!/usr/bin/env python3
"""Build an explicitly partial index of identified annual stock evidence.

Exact reviewed aliases only. A public-company name must never turn its bonds,
preferred securities or funds into common shares. Ambiguous share classes
remain unmapped. This index is not sufficient for an allocation.
Usage: python scripts/strategies/build_trump_stocks.py NASDAQ_TRADED.txt
"""
import argparse
import csv
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
IDENTITIES = Path(__file__).with_name('trump_stock_identities.json')
NASDAQ = 'https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt'


def build(annual, identities, symbols):
    result = []
    seen = set()
    for ticker, identity in identities.items():
        listing = symbols.get(ticker)
        if (not listing or listing['ETF'] != 'N' or listing['Test Issue'] != 'N'
                or re.search(r'preferred|debentures?|notes|warrants?|\bunits\b', listing['Security Name'], re.I)):
            raise ValueError(f'{ticker}: missing common-company listing')
        aliases = set(identity['aliases'])
        rows = [r for r in annual['rows'] if r['name'] in aliases and r['low'] > 0]
        if not rows:
            raise ValueError(f'{ticker}: no eligible annual source rows')
        for row in rows:
            if row['id'] in seen or 'Interest' in row['incomeType'].title() or row['account'].startswith('FAMILY TRUST'):
                raise ValueError(f'{ticker}: conflicting evidence at {row["id"]}')
            seen.add(row['id'])
        result.append({'ticker': ticker, 'name': identity['name'],
                       'listingName': listing['Security Name'],
                       'rows': [{'id': r['id'], 'name': r['name']} for r in rows]})
    return {'annualSha256': annual['sha256'], 'scope': 'public-company-stocks',
            'complete': False, 'listingSource': NASDAQ, 'listingCheckedOn': '2026-09-22',
            'note': 'Partial stock identity review. Unmapped rows and later transactions block portfolio weights.',
            'stocks': sorted(result, key=lambda r: r['ticker'])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('listings', type=Path)
    args = parser.parse_args()
    symbols = {r['Symbol']: r for r in csv.DictReader(args.listings.open(), delimiter='|')}
    annual = json.loads((ROOT / 'src/lib/strategies/trump-annual.json').read_text())
    output = build(annual, json.loads(IDENTITIES.read_text()), symbols)
    (ROOT / 'src/lib/strategies/trump-stocks.json').write_text(json.dumps(output, indent=2) + '\n')
    print(f'Indexed {len(output["stocks"])} tickers from {sum(len(s["rows"]) for s in output["stocks"])} annual rows; coverage remains partial.')


if __name__ == '__main__':
    main()
