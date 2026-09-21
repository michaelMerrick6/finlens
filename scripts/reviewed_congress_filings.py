"""Source-locked transcriptions for paper filings the OCR parsers cannot read reliably.

Unknown filings use the normal parser. A changed reviewed source raises instead of
silently falling back to the parser that originally corrupted the filing.
"""
import hashlib
import json
import re
from datetime import date
from pathlib import Path

REVIEW_DIR = Path(__file__).resolve().parent.parent / 'config' / 'reviewed_filings'


def load_reviewed_filing(prefix: str) -> dict | None:
    if not re.fullmatch(r'(house|senate)-[a-zA-Z0-9-]+', prefix):
        raise ValueError('Invalid filing identifier')
    path = REVIEW_DIR / f'{prefix}.json'
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    if data['version'] != 1 or len(data['rows']) != data['row_count']:
        raise ValueError(f'Invalid reviewed filing: {prefix}')
    if not data["rows"] and not (data.get("verified_no_trades") is True and data.get("review_note")):
        raise ValueError(f"Empty reviewed filing lacks no-transaction evidence: {prefix}")
    locations = set()
    indexes = set()
    for index, row in enumerate(data['rows']):
        source_index = row.get('source_index', index)
        if type(source_index) is not int or source_index < 0 or source_index in indexes:
            raise ValueError(f'Duplicate/invalid source index: {prefix}')
        indexes.add(source_index)
        location = (row['page'], row['row'])
        if location in locations or min(location) < 1:
            raise ValueError(f'Duplicate/invalid source row: {prefix} {location}')
        locations.add(location)
        if (date.fromisoformat(row['transaction_date']) > date.fromisoformat(data['published_date'])
                or row['transaction_type'] not in {'buy', 'sell', 'exchange'}
                or not row['asset_name'] or not row['amount_range'].startswith(('$', 'Over $'))):
            raise ValueError(f'Invalid reviewed transaction: {prefix} {location}')
    validate_amendment_links(prefix, data, locations, indexes)
    return data


def validate_amendment_links(prefix, data, locations, indexes):
    """Require reciprocal, explicit replacements; never infer from equal trade values."""
    if data.get('amendment_only') and len(data.get('replaces_rows', [])) != len(data['rows']):
        raise ValueError('Amendment-only filing must link every replacement row')
    if data.get('source_transaction_count', len(data['rows'])) != len(data['rows']) + len(data.get('superseded_rows', [])):
        raise ValueError('Original source row accounting is incomplete')
    for field in ('superseded_rows', 'replaces_rows'):
        for link in data.get(field, []):
            original = field == 'superseded_rows'
            own = 'original' if original else 'amendment'
            other = 'amendment' if original else 'original'
            other_prefix = link[f'{other}_filing']
            if link[f'{own}_filing'] != prefix or not re.fullmatch(r'senate-[a-f0-9-]+', other_prefix):
                raise ValueError('Invalid amendment filing link')
            other_data = json.loads((REVIEW_DIR / f'{other_prefix}.json').read_text())
            reciprocal = 'replaces_rows' if original else 'superseded_rows'
            if (link not in other_data.get(reciprocal, [])
                    or other_data['member_id'] != data['member_id']
                    or other_data['chamber'] != data['chamber']):
                raise ValueError('Amendment link is not reciprocal for the same member')
            original_data, amendment_data = (data, other_data) if original else (other_data, data)
            if amendment_data['published_date'] <= original_data['published_date']:
                raise ValueError('Amendment must follow the original filing')
            if any((r['page'], r['row']) == (link['original_page'], link['original_row'])
                   or r.get('source_index', i) == link['original_source_index']
                   for i, r in enumerate(original_data['rows'])):
                raise ValueError('Superseded original row must not remain active')
            replacements = [r for i, r in enumerate(amendment_data['rows'])
                            if (r['page'], r['row'], r.get('source_index', i)) ==
                            (link['amendment_page'], link['amendment_row'], link['amendment_source_index'])]
            if len(replacements) != 1 or any(replacements[0].get(k) != link[k] for k in
                    ('account', 'ticker', 'transaction_date', 'transaction_type', 'amount_range')):
                raise ValueError('Amendment replacement does not match reviewed correction')
    superseded = data.get('superseded_rows', [])
    if superseded:
        excluded_indexes = [r['original_source_index'] for r in superseded]
        excluded_locations = [(r['original_page'], r['original_row']) for r in superseded]
        total = data.get('source_transaction_count')
        if (len(set(excluded_indexes)) != len(superseded)
                or len(set(excluded_locations)) != len(superseded)
                or set(excluded_locations) & locations or set(excluded_indexes) & indexes
                or type(total) is not int or total != len(data['rows']) + len(superseded)
                or indexes | set(excluded_indexes) != set(range(total))):
            raise ValueError('Original source row accounting is incomplete')


def reviewed_trades(prefix: str, data: dict) -> list[dict]:
    # Keep one row per source position, including identical transactions in
    # different accounts. Numeric suffixes retain the ingestion/audit ID contract.
    return [dict(
        member_id=data['member_id'], politician_name=data['politician_name'],
        chamber=data['chamber'], party='Unknown', ticker=row['ticker'],
        transaction_date=row['transaction_date'], published_date=data['published_date'],
        transaction_type=row['transaction_type'], asset_type=row['asset_type'],
        amount_range=row['amount_range'], source_url=data['source_url'],
        doc_id=f"{prefix}-{row.get('source_index', index)}", asset_name=row['asset_name'],
    ) for index, row in enumerate(data['rows'])]


def reviewed_house_trades(prefix: str, pdf_bytes: bytes) -> list[dict] | None:
    data = load_reviewed_filing(prefix)
    if data is None:
        return None
    if hashlib.sha256(pdf_bytes).hexdigest() != data.get('source_sha256'):
        raise ValueError(f'Reviewed House source changed; manual review required: {prefix}')
    return reviewed_trades(prefix, data)


def reviewed_senate_trades(prefix: str, images, member_id: str, filed_date: str) -> list[dict] | None:
    data = load_reviewed_filing(prefix)
    if data is None:
        return None
    hashes = [hashlib.sha256(im.convert('RGB').tobytes()).hexdigest() for im in images]
    if hashes != data.get('image_rgb_sha256'):
        raise ValueError(f'Reviewed Senate source changed; manual review required: {prefix}')
    if member_id != data['member_id'] or filed_date != data['published_date']:
        raise ValueError(f'Reviewed Senate filing metadata changed: {prefix}')
    return reviewed_trades(prefix, data)
