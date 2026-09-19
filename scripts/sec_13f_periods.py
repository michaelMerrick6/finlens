"""Build effective quarter snapshots without treating additions as restatements."""
from copy import deepcopy


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
                    if None in existing or None in additions or existing & additions:
                        raise ValueError('Overlapping or unidentified amendment holdings require source review')
                    state['holdings'].extend(deepcopy(entry['holdings']))
                    for field in ('rows_seen','rows_supported','rows_skipped','rows_resolved','rows_unresolved'):
                        state[field] = int(state.get(field,0)) + int(entry.get(field,0))
                    state['published_date'] = entry['published_date']
                    state['accession'] = entry.get('accession')
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
