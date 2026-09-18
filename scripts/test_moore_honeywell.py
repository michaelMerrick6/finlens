import copy
import json
import unittest
from reconcile_moore_honeywell import ROOT, reconcile

class HoneywellReviewTests(unittest.TestCase):
    def setUp(self):
        ledger=json.loads((ROOT/'docs/research/priority-holdings-ledger.json').read_text())
        self.member=next(m for m in ledger['members'] if m['member_id']=='M001236')

    def test_legal_ratios_once_on_unadjusted_baseline(self):
        result=reconcile(self.member,'2026-09-17')
        self.assertFalse(result['current_holdings_eligible'])
        self.assertEqual([p['ticker'] for p in result['positions']],['HON','HONA'])
        for p in result['positions']:
            self.assertEqual(p['min_shares'],38)
            self.assertAlmostEqual(p['max_shares'],50000/195.09/2)

    def test_changed_evidence_fails_closed(self):
        for change in ['hash','account','date','event','member','additional-position']:
            m=copy.deepcopy(self.member)
            p=next(p for p in m['positions'] if p['ticker']=='HON')
            date='2026-09-17'
            if change=='hash':p['source_sha256']='new'
            if change=='account':p['account']='Other'
            if change=='date':date='2026-09-18'
            if change=='event':m['events'].append(dict(ticker='HONA',date='2026-08-01'))
            if change=='member':m['member_id']='other'
            if change=='additional-position':m['positions'].append(dict(p,ticker='HONA'))
            with self.subTest(change=change), self.assertRaises(ValueError):
                reconcile(m,date)

if __name__=='__main__':unittest.main()
