"""Build effective quarter snapshots without treating additions as restatements."""
from copy import deepcopy
import json
from pathlib import Path


def reviewed_addition(state, entry, overlap):
    reviews = json.loads((Path(__file__).resolve().parents[1] / 'config' / 'reviewed_13f_additions.json').read_text())
    for review in reviews:
        if review['report_period'] != entry['report_period'] or set(review['overlap_tickers']) != overlap:
            continue
        if all(parsed.get('accession') == source['accession']
               and parsed.get('source_sha256') == source['sha256']
               and parsed.get('source_url') == source['source_url']
               for parsed, source in [(state, review['baseline']), (entry, review['amendment'])]):
            return True
    return False



def compose_periods(filings, max_periods=8):
    grouped = {}
    for filing in filings:
        grouped.setdefault(filing['report_period'], []).append(filing)
    periods, issues = [], []
    for period, entries in sorted(grouped.items()):
        state = None
        seen = set()
        try:
            for entry in sorted(entries, key=lambda x: (x.get('published_date') or '', x.get('accession') or '')):
                accession = entry.get('accession')
                if accession and accession in seen:
                    continue
                seen.add(accession)
                kind = entry.get('amendment_type', 'ORIGINAL')
                if kind in ('ORIGINAL', 'RESTATEMENT'):
                    state = deepcopy(entry)
                elif kind == 'NEW HOLDINGS':
                    if state is None:
                        raise ValueError('New-holdings amendment has no original/restated baseline in fetched history')
                    def key(row):
                        return row.get('ticker') or row.get('_asset_key') or row.get('_cusip')
                    existing = {key(row) for row in state['holdings']}
                    additions = {key(row) for row in entry['holdings']}
                    overlap = existing & additions
                    if None in existing or None in additions or (overlap and not reviewed_addition(state, entry, overlap)):
                        raise ValueError('Overlapping or unidentified amendment holdings require source review')
                    by_key = {key(row): row for row in state['holdings']}
                    for row in deepcopy(entry['holdings']):
                        if key(row) in overlap:
                            current = by_key[key(row)]
                            if not current.get('_cusip') or current['_cusip'] != row.get('_cusip') or current.get('_share_class') != row.get('_share_class'):
                                raise ValueError('Reviewed amendment security identity changed')
                            current['shares_held'] += row['shares_held']
                            current['value_held'] += row['value_held']
                            current['_source_urls'] = [current['source_url'], row['source_url']]
                            current['source_url'] = row['source_url']
                            current['_accession'] = entry.get('accession')
                        else:
                            state['holdings'].append(row)
                    for field in ('rows_seen','rows_supported','rows_skipped','rows_resolved','rows_unresolved'):
                        state[field] = int(state.get(field,0)) + int(entry.get(field,0))
                    state['published_date'] = entry['published_date']
                    state['accession'] = entry.get('accession')
                    state['source_sha256'] = None
                else:
                    raise ValueError('Unknown amendment type')
            if state:
                # Combined data was not publicly available before the last amendment.
                for row in state['holdings']:
                    row['published_date'] = state['published_date']
                periods.append(state)
        except ValueError as exc:
            issues.append({'report_period':period, 'error':str(exc)})
    if max_periods and max_periods > 0:
        periods = periods[-max_periods:]
    return periods, issues


def previous_quarter_holdings(periods, index):
    """Do not label a comparison against a nonadjacent quarter as quarter-over-quarter."""
    from datetime import date, timedelta
    if index <= 0:
        return []
    current = date.fromisoformat(periods[index]['report_period'])
    if current.month not in (3, 6, 9, 12):
        return []
    expected = current.replace(month=current.month-2, day=1) - timedelta(days=1)
    return periods[index-1]['holdings'] if periods[index-1]['report_period'] == expected.isoformat() else []
