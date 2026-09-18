"""Expose useful partial evidence; never promote missing baselines to holdings."""
import json
from pathlib import Path
from model_unanchored_holdings import model_unanchored_flow
from stage_priority_models import prices

ROOT=Path(__file__).resolve().parents[1]


def main():
    research=ROOT/'docs/research'
    ledger=json.loads((research/'priority-holdings-ledger.json').read_text())
    member=next(m for m in ledger['members'] if m['member_id']=='M001236')
    reviewed=json.loads((research/'moore-source-review.json').read_text())
    hashes={r['source']:r['sha256'] for r in reviewed['sources']}
    models=json.loads((research/'priority-holdings-models.json').read_text())
    as_of=models['as_of']
    rows=[]
    for ticker in ['T','SMPL','IHG','RYCEY']:
        events=[e for e in member['events'] if e['ticker']==ticker]
        if any(hashes.get(e['source'])!=e['source_sha256'] for e in events):
            raise ValueError('Changed transaction source requires review')
        if any(p['ticker']==ticker for p in member['positions']):
            raise ValueError('New baseline requires account reconciliation')
        _,market=prices(ticker)
        model=model_unanchored_flow(events,market['prices'],member['baseline_date'],as_of)
        rows.append(dict(ticker=ticker, model=model, price_source=market['source'],
                         source_hashes={e['source']:e['source_sha256'] for e in events}))
    result=dict(member_id='M001236',as_of=as_of,publication_enabled=False,
                status='partial-evidence-not-total-holdings',rows=rows,
                HY=dict(status='conflicting-source-evidence',eligible_for_portfolio_total=False,
                        annual_source='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf',
                        annual_pages=[4,19],transaction_source='https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033856.pdf',
                        issue='Annual year-end None conflicts with December 31 purchase and January 5 sale.',
                        resolution='Requires corrected disclosure or dated account balance; do not choose either branch silently.'))
    (research/'moore-missing-balance-evidence.json').write_text(json.dumps(result,indent=2)+'\n')
    for row in rows:
        print(row['ticker'],row['model']['status'],row['model'].get('min_net_share_change'),row['model'].get('max_net_share_change'))

if __name__=='__main__':main()
