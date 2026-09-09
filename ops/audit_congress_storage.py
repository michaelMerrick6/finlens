"""Read-only counts of stored congressional trades. Not a filing-completeness audit."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from audit_congress_roster import get_supabase_client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    if not 2012 <= args.start_year <= now.year:
        parser.error('start-year must be between 2012 and the current year')
    db = get_supabase_client()
    rows = []
    for chamber in ['House', 'Senate']:
        for year in range(args.start_year, now.year + 1):
            query = db.table('politician_trades').select('id', count='exact', head=True).eq('chamber', chamber).gte('published_date', f'{year}-01-01').lt('published_date', f'{year + 1}-01-01')
            result = query.execute()
            if result.count is None:
                raise RuntimeError('Database returned no count; refusing to report zero')
            rows.append({'chamber': chamber, 'filing_year': year, 'stored_transaction_rows': result.count,
                         'official_filing_count': None, 'filing_completeness': 'not measured'})
    result = {'checked_at': now.isoformat(), 'scope': 'Stored transaction counts by published_date; includes all stored asset types and sources.',
              'limitations': ['No official filing inventory comparison.', 'Row counts may include duplicates.',
                              'Zero rows do not establish that nobody traded.', 'Years before start-year are not assessed.'],
              'rows': rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(rows))


if __name__ == '__main__':
    main()
