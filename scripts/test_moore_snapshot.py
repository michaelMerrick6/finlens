import copy,json,unittest
from build_moore_snapshot import build,ROOT

class MooreSnapshotTests(unittest.TestCase):
    def fixture(self):
        positions=[dict(id=f'p{i}',source='source',source_sha256='hash',range='$1,001 - $15,000') for i in (1,2)]
        model=dict(status='conditional-model',min_shares=2,max_shares=4,min_value=20,max_value=40,latest_price=dict(date='2026-09-16',close=10),assumptions=[])
        rows=[dict(position_id=p['id'],ticker='TEST',name='Test',account=p['id'],source='source',page=1,model=copy.deepcopy(model)) for p in positions]
        data=dict(as_of='2026-09-17',members=[dict(member_id='M001236',rows=rows,unresolved_positions=[dict(ticker='UNKNOWN',name='Unknown',events=[dict(source='source',page=1)])])])
        ledger=dict(members=[dict(member_id='M001236',baseline_date='2025-12-31',positions=positions,events=[])])
        review=dict(sources=[dict(source='source',sha256='hash')])
        return data,ledger,review
    def test_combines_accounts_and_excludes_unknown(self):
        out=build(*self.fixture())
        self.assertEqual(len(out['rows']),1)
        self.assertEqual(out['subtotalMin'],40)
        self.assertEqual(out['subtotalMax'],80)
        self.assertEqual(len(out['rows'][0]['accounts']),2)
        self.assertFalse(out['fullyVerified'])
        self.assertEqual(out['unresolved'][0]['ticker'],'UNKNOWN')
    def test_stale_source_rejected(self):
        d,l,r=self.fixture();l['members'][0]['positions'][0]['source_sha256']='changed'
        with self.assertRaises(ValueError):build(d,l,r)
    def test_intraday_quote_requires_completed_replacement(self):
        d,l,r=self.fixture();d['members'][0]['rows'][0]['model']['latest_price']['date']='2026-09-17'
        with self.assertRaises(ValueError):build(d,l,r)
        out=build(d,l,r,{'TEST':dict(date='2026-09-16',close=20)})
        self.assertEqual(out['subtotalMin'],60)
    def test_generated_snapshot_is_partial_and_sorted(self):
        s=json.loads((ROOT/'src/data/moore-holdings-snapshot.json').read_text())
        self.assertEqual(len(s['rows']),32);self.assertEqual(len(s['unresolved']),6)
        self.assertFalse(s['fullyVerified'])
        self.assertTrue({'HY','HON','T','SMPL','IHG','RYCEY'}=={r['ticker'] for r in s['unresolved']})
        self.assertFalse({r['ticker'] for r in s['rows']} & {r['ticker'] for r in s['unresolved']})
        self.assertAlmostEqual(s['subtotalMin'],sum(r['minValue'] for r in s['rows']))
        values=[(r['minValue']+r['maxValue'])/2 for r in s['rows']]
        self.assertEqual(values,sorted(values,reverse=True))
if __name__=='__main__':unittest.main()
