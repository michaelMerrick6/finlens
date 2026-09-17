"""Conservative Senate annual staging. No balances inferred from income or PTR tables."""
import re
from bs4 import BeautifulSoup


def clean(value):
    return ' '.join(value.split())


def parse_senate_annual(html, first, last):
    soup = BeautifulSoup(html, 'html.parser')
    heading = clean(soup.h1.get_text(' ', strip=True)) if soup.h1 else ''
    identity = clean(soup.h2.get_text(' ', strip=True)) if soup.h2 else ''
    key = lambda s: re.sub(r'[^a-z0-9]', '', s.lower())
    if key(f'{last}, {first}') not in [key(part) for part in re.findall(r'\(([^()]*)\)', identity)]:
        raise ValueError('Senate annual identity mismatch')
    annual = re.fullmatch(r'Annual Report for Calendar (\d{4})(?: \(Amendment \d+\))?', heading)
    new_filer = re.fullmatch(r'New Filer Report for (\d{2}/\d{2}/\d{4})', heading)
    if not annual and not new_filer: raise ValueError('Unsupported Senate report heading')
    asset_tables = []
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if rows and [clean(c.get_text(' ',strip=True)) for c in rows[0].find_all(['th','td'])] == ['', 'Asset', 'Asset Type', 'Owner', 'Value', 'Income Type', 'Income']:
            asset_tables.append(rows)
    if len(asset_tables) != 1: raise ValueError('Missing or ambiguous Senate asset table')
    holdings, seen = [], {}
    for row in asset_tables[0][1:]:
        cells = [clean(c.get_text(' ',strip=True)) for c in row.find_all(['th','td'])]
        if len(cells) != 7: raise ValueError('Incomplete Senate asset row')
        row_id, name, asset_type, owner, value, income_type, income = cells
        if not re.fullmatch(r'\d+(?:\.\d+)*', row_id) or row_id in seen: raise ValueError('Invalid/duplicate Senate asset row')
        parent = row_id.rsplit('.',1)[0] if '.' in row_id else None
        if parent and parent not in seen: raise ValueError('Missing parent account')
        # Bank identifiers and private entity labels must not become stock tickers.
        ticker = re.match(r'^([A-Z][A-Z0-9.\-]{0,9}) - ', name) if ('Stock' in asset_type or 'Exchange Traded Fund' in asset_type) else None
        entry = dict(row_id=row_id, parent_row_id=parent, account_path=(seen[parent]['account_path']+[seen[parent]['asset_name']]) if parent else [],
                     asset_name=name, asset_type=asset_type, owner=owner, value_range=value,
                     income_type=income_type, income_range=income, ticker=ticker[1] if ticker else None,
                     is_container=value=='--')
        holdings.append(entry);seen[row_id]=entry
    issues=['source-review-and-transaction-reconciliation-required']
    if new_filer: issues.append('new-filer-valuation-date-needs-review')
    if 'Amendment' in heading: issues.append('amendment-needs-reconciliation')
    exemption = soup.find('input', {'name':'filing_omitted_assets'})
    exempt = exemption.has_attr('checked') if exemption else None
    if exempt is None: issues.append('exemption-declaration-needs-review')
    if exempt: issues.append('filer-declared-exempt-assets')
    return dict(heading=heading, source_identity=identity, reporting_year=int(annual[1]) if annual else None,
                valuation_date=f'{annual[1]}-12-31' if annual else None, declared_exempt_assets=exempt,
                holdings=holdings, issues=issues, current_holdings_eligible=False)
