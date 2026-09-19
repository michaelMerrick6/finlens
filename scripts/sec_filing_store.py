"""Durable discovery and atomic per-document publication for SEC Form 4."""
from datetime import datetime, timedelta, timezone


def register_filings(client, filings):
    entries = {f['accession']: {'accession': f['accession'], 'filing': f} for f in filings}
    rows = list(entries.values())
    for start in range(0, len(rows), 100):
        client.table('sec_filing_queue').upsert(rows[start:start+100], on_conflict='accession', ignore_duplicates=True).execute()


def pending_filings(client, limit=600):
    return [r['filing'] for r in client.table('sec_filing_queue').select('filing')
            .neq('status', 'complete').lte('next_attempt_at', datetime.now(timezone.utc).isoformat())
            .order('next_attempt_at').limit(limit).execute().data or []]


def publish(client, filing, rows):
    clean = [{k: v for k, v in row.items() if not k.startswith('_')} for row in rows]
    return client.rpc('publish_insider_filing', {'target_accession': filing['accession'], 'trades': clean}).execute().data


def fail(client, filing, error):
    now = datetime.now(timezone.utc)
    client.table('sec_filing_queue').update({'status': 'failed', 'last_error': str(error)[:2000],
        'next_attempt_at': (now + timedelta(hours=1)).isoformat(), 'updated_at': now.isoformat()}) \
        .eq('accession', filing['accession']).execute()
