"""Compare transaction multisets so equal counts cannot hide changed values."""
from collections import Counter

FIELDS = ('ticker', 'transaction_date', 'published_date', 'transaction_type', 'amount_range', 'member_id', 'asset_name', 'asset_type')


def compare_transactions(parsed, stored):
    def signature(row):
        return tuple(str(row.get(field) or '').strip() for field in FIELDS)
    expected = Counter(map(signature, parsed))
    actual = Counter(map(signature, stored))
    missing = expected - actual
    unexpected = actual - expected
    if not missing and not unexpected:
        return None
    return {'unmatched_parsed_rows': sum(missing.values()), 'unmatched_stored_rows': sum(unexpected.values()),
            'parsed_examples': [dict(zip(FIELDS, row)) for row in list(missing.elements())[:5]],
            'stored_examples': [dict(zip(FIELDS, row)) for row in list(unexpected.elements())[:5]]}
