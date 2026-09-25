"""Refresh the sourced sector catalog used by the congressional activity overview."""
import argparse
import csv
import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlopen

SOURCES = {
    'IVV': 'https://www.ishares.com/us/products/239726/ishares-core-s-p-500-etf/latest-holdings.csv',
    'IJH': 'https://www.ishares.com/us/products/239763/ishares-core-s-p-mid-cap-etf/latest-holdings.csv',
    'IWM': 'https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/latest-holdings.csv',
}


def parse_holdings(raw):
    lines = raw.decode('utf-8-sig').splitlines()
    as_of = next(csv.reader([line]) for line in lines if line.startswith('Fund Holdings as of,'))
    as_of = datetime.strptime(next(as_of)[1], '%b %d, %Y').date().isoformat()
    start = next(i for i, line in enumerate(lines) if line.startswith('Ticker,'))
    stocks = {}
    for row in csv.DictReader(lines[start:]):
        ticker = (row.get('Ticker') or '').strip().replace(' ', '.')
        if row.get('Asset Class') != 'Equity' or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,11}', ticker):
            continue
        sector = row.get('Sector') or ''
        if not sector or sector == 'Cash and/or Derivatives':
            continue
        stocks[ticker] = dict(sector='Communication services' if sector == 'Communication' else sector,
                             name=(row.get('Name') or '').strip().title())
    if not stocks:
        raise ValueError('No equity classifications found; keep the existing catalog.')
    return as_of, stocks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, help='Use previously downloaded IVV/IJH/IWM CSV files.')
    args = parser.parse_args()
    sources, stocks = [], {}
    for name, url in SOURCES.items():
        raw = (args.input_dir / f'{name}.csv').read_bytes() if args.input_dir else urlopen(url, timeout=30).read()
        as_of, rows = parse_holdings(raw)
        sources.append(dict(id=name, url=url, holdingsAsOf=as_of, checkedOn=date.today().isoformat(), sha256=hashlib.sha256(raw).hexdigest()))
        for ticker, classification in rows.items():
            if ticker in stocks and stocks[ticker]['sector'] != classification['sector']:
                raise ValueError(f'Conflicting sector for {ticker}; review before publishing.')
            stocks.setdefault(ticker, dict(**classification, sourceId=name))
    target = Path(__file__).resolve().parents[1] / 'src/lib/analysis-sectors.json'
    target.write_text(json.dumps(dict(sources=sources, stocks=dict(sorted(stocks.items()))), separators=(',', ':')) + '\n')
    print(f'Wrote {len(stocks)} sourced classifications to {target.name}')


if __name__ == '__main__':
    main()
