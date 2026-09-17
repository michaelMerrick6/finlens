"""Apply explicit source-locked correction links to an internal evidence ledger."""

def apply_corrections(events, links):
    by_id={e['id']:e for e in events}
    used=set()
    for link in links:
        original=by_id.get(link['original_id']); correction=by_id.get(link['correction_id'])
        if original is None or correction is None:
            raise ValueError('Reviewed correction source row missing')
        if original['id'] in used or correction['id'] in used:
            raise ValueError('Overlapping correction chain requires review')
        used.update([original['id'],correction['id']])
        if original['source_sha256']!=link['original_sha256'] or correction['source_sha256']!=link['correction_sha256']:
            raise ValueError('Reviewed correction source changed')
        if original.get('filing_status')!='New' or correction.get('filing_status')!=link['filing_status'] or link['filing_status'] not in ['Amended','Deleted']:
            raise ValueError('Correction status changed')
        if original['account']!=link['original_account'] or correction['account']!=link['corrected_account'] or correction.get('original_transaction_id')!=link['official_correction_transaction_id']:
            raise ValueError('Correction identity changed')
        if any(original.get(k)!=v or correction.get(k)!=v for k,v in link['expected'].items()):
            raise ValueError('Reviewed transaction details changed')
        candidates=[e['id'] for e in events if e.get('filing_status')=='New' and all(e.get(k)==v for k,v in link['expected'].items())]
        if candidates!=[original['id']]:
            raise ValueError('Original correction match is no longer unique')
        original.update(review_status='superseded-by-reviewed-correction',superseded_by=correction['id'],balance_effect='none')
        correction.update(correction_resolution='reviewed-original-link',replaces_event_id=original['id'],correction_link_basis=link['basis'])
        if correction['filing_status']=='Deleted':
            correction.update(review_status='reviewed-deletion',balance_effect='none')
        else:
            correction.update(review_status='matched-account' if correction['position_id'] else 'unmatched-baseline-or-account',balance_effect='replacement')
