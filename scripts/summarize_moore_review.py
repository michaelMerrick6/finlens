"""Summarize one member without promoting candidates to verified portfolios."""
import json
from pathlib import Path
from reconcile_moore_honeywell import reconcile
ROOT=Path(__file__).resolve().parents[1]

def main():
    research=ROOT/'docs/research'
    data=json.loads((research/'priority-holdings-models.json').read_text())
    member=next(m for m in data['members'] if m['member_id']=='M001236')
    ledger=next(m for m in json.loads((research/'priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='M001236')
    reviewed=json.loads((research/'moore-source-review.json').read_text())
    hashes={r['source']:r['sha256'] for r in reviewed['sources']}
    for row in ledger['positions']+ledger['events']:
        if hashes.get(row['source'])!=row['source_sha256']:
            raise ValueError('Moore sources changed; repeat source review')
    positive=[];closed=[];blocked=[]
    for row in member['rows']:
        model=row['model']
        item=dict(ticker=row['ticker'],account=row['account'],position_id=row['position_id'],source=row['source'],page=row['page'])
        if model['status']!='conditional-model':
            blocked.append(dict(item,reason=model['reason']))
        elif model['max_shares']==0:
            closed.append(dict(item,status='conditional-zero',assumptions=model['assumptions']))
        else:
            positive.append(dict(item,status='conditional-estimate',min_shares=model['min_shares'],max_shares=model['max_shares'],min_value=model['min_value'],max_value=model['max_value'],assumptions=model['assumptions']))
    report=dict(member_id='M001236',name='Tim Moore',as_of=data['as_of'],status='source-rows-reviewed-portfolio-not-fully-verified',publication_enabled=False,
        source_review=dict(annual_rows=72,transaction_rows=35,annual_schedule_a_pages=[1,2,3,4,5,6],additional_hyster_yale_schedule_b_page=19),
        counts=dict(positive_account_candidates=len(positive),distinct_positive_candidate_tickers=len({r['ticker'] for r in positive}),conditional_zero_accounts=len(closed),blocked_baseline_accounts=len(blocked),unresolved_additional_tickers=len(member['unresolved_positions'])),
        positive_candidates=positive,conditional_zero_positions=closed,blocked_positions=blocked,unresolved_positions=member['unresolved_positions'],
        honeywell_entitlement_scenario=reconcile(ledger, data['as_of']),
        account_evidence_audit=json.loads((research/'moore-account-audit.json').read_text()),
        remaining=[
            'HON/HONA issuer conversion and unadjusted baseline price are reconciled in the attached conditional scenario; account entitlement and retained ownership remain unconfirmed.',
            'HY annual None conflicts with annual Schedule B December 31 purchase and January 5 PTR sale; exact account and opening quantity unresolved.',
            'No reviewed starting balances for T, SMPL, IHG and RYCEY; generic sale codes do not establish complete exits.',
            'Finish issuer-level non-split corporate-action coverage; provider absence of splits is not clearance for mergers or distributions.',
            'Ranges remain conditional on disclosed events, unknown execution prices, inferred accounts and no unreported changes. GNPX/TZA fractional settlement is conservatively bounded; actual broker treatment and cash proceeds remain unknown.'
        ])
    (research/'moore-verification-status.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report['counts']))
if __name__=='__main__':main()
