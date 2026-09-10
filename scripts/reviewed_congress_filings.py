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
    locations = set()
    for row in data['rows']:
        location = (row['page'], row['row'])
        if location in locations or min(location) < 1:
            raise ValueError(f'Duplicate/invalid source row: {prefix} {location}')
        locations.add(location)
        if (date.fromisoformat(row['transaction_date']) > date.fromisoformat(data['published_date'])
                or row['transaction_type'] not in {'buy', 'sell', 'exchange'}
                or not row['asset_name'] or not row['amount_range'].startswith(('$', 'Over $'))):
            raise ValueError(f'Invalid reviewed transaction: {prefix} {location}')
    return data


def reviewed_trades(prefix: str, data: dict) -> list[dict]:
    # Keep one row per source position, including identical transactions in
    # different accounts. Numeric suffixes retain the ingestion/audit ID contract.
    return [dict(
        member_id=data['member_id'], politician_name=data['politician_name'],
        chamber=data['chamber'], party='Unknown', ticker=row['ticker'],
        transaction_date=row['transaction_date'], published_date=data['published_date'],
        transaction_type=row['transaction_type'], asset_type=row['asset_type'],
        amount_range=row['amount_range'], source_url=data['source_url'],
        doc_id=f'{prefix}-{index}', asset_name=row['asset_name'],
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
