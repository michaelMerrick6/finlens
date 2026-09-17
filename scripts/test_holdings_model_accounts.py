import unittest
from stage_priority_models import could_affect_position, unresolved_positions
from reconcile_priority_holdings import house_account

class AccountModelTests(unittest.TestCase):
    def setUp(self):
        self.position=dict(ticker='GOOG',account='Brokerage #2',owner='not-stated')
        self.event=dict(ticker='GOOG',account='IRA',owner='not-stated',after_baseline=True,filing_status='New',review_status='unmatched-baseline-or-account')
    def test_new_ira_holding_does_not_suppress_known_brokerage(self):
        self.assertFalse(could_affect_position(self.event,self.position))
    def test_missing_account_or_correction_still_blocks(self):
        self.assertTrue(could_affect_position(dict(self.event,account=None),self.position))
        self.assertTrue(could_affect_position(dict(self.event,filing_status='Amended'),self.position))
        self.assertTrue(could_affect_position(dict(self.event,account='UNRESOLVED: IRA'),self.position))
    def test_account_number_prefix_cannot_match_another_account(self):
        self.assertTrue(house_account(['S O: Brokerage #10'],{'Brokerage #1'}).startswith('UNRESOLVED:'))
        self.assertEqual(house_account(['S O: Brokerage #10'],{'Brokerage #1','Brokerage #10'}),'Brokerage #10')
    def test_parent_trust_does_not_swallow_missing_child_account(self):
        self.assertTrue(house_account(['S O: Family Trust > Broker'],{'Family Trust'}).startswith('UNRESOLVED:'))
    def test_unmatched_purchases_remain_visible_without_invented_quantity(self):
        e=dict(self.event,id='source#1',name='Alphabet',position_id=None,asset_type='ST',date='2026-01-12',action='P',range='$1,001 - $15,000',source='source',page=1)
        rows=unresolved_positions(dict(events=[e]))
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['status'],'starting-balance-unresolved')
        self.assertFalse(rows[0]['current_holdings_eligible'])
        self.assertNotIn('min_shares',rows[0])
        self.assertEqual(rows[0]['events'][0]['range'],'$1,001 - $15,000')
if __name__=='__main__':unittest.main()
