"""Classify the evidence supporting every Moore transaction/account link."""
import json
from collections import Counter
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def audit(member, reviewed):
    hashes={r['source']:r['sha256'] for r in reviewed['sources']}
    positions={p['id']:p for p in member['positions']}
    rows=[]
    for e in member['events']:
        if hashes.get(e['source'])!=e['source_sha256']:
            raise ValueError('Changed source requires review')
        p=positions.get(e.get('position_id'))
        if not e['after_baseline']:basis='baseline-period-not-rolled-forward'
        elif not p:basis='no-established-baseline-account'
        elif not e.get('account'):basis='inferred-unique-annual-position'
        else:basis='explicit-account-label'
        rows.append(dict(event_id=e['id'],ticker=e['ticker'],date=e['date'],source=e['source'],page=e['page'],
                         filed_account=e.get('account'),candidate_account=p['account'] if p else None,
                         basis=basis,explicit_account_confirmation=basis=='explicit-account-label'))
    return dict(member_id=member['member_id'],status='account-evidence-audit-complete',
                actual_account_assignment_verified=False,publication_enabled=False,
                counts=dict(Counter(r['basis'] for r in rows)),rows=rows,
                conclusion='Account labels are absent from all reviewed PTR rows. Unique annual matches are conditional inferences, not disclosed account confirmations.')

def main():
    research=ROOT/'docs/research'
    m=next(m for m in json.loads((research/'priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='M001236')
    result=audit(m,json.loads((research/'moore-source-review.json').read_text()))
    (research/'moore-account-audit.json').write_text(json.dumps(result,indent=2)+'\n')
    print(result['counts'])
if __name__=='__main__':main()
