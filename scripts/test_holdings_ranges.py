import unittest
from model_holdings_ranges import model_range
class RangeTests(unittest.TestCase):
    def setUp(self):
        self.base=dict(id='a',date='2025-12-31',source='annual',value_bounds=[1000,2000])
        self.prices=[dict(date='2025-12-31',close=10),dict(date='2026-01-02',close=12,low=10,high=20),dict(date='2026-01-05',close=15)]
    def event(self,**kw):return dict(dict(id='e',date='2026-01-02',position_id='a',review_status='matched-account',action='P',value_bounds=[100,200]),**kw)
    def run_model(self,events):return model_range(self.base,events,self.prices,'2026-01-05')
    def test_buys_use_daily_price_bounds(self):
        r=self.run_model([self.event()]);self.assertEqual((r['min_shares'],r['max_shares']),(105,220));self.assertFalse(r['current_holdings_eligible'])
    def test_sale_interval_is_subtracted_in_reverse(self):
        r=self.run_model([self.event(action='S (partial)')]);self.assertEqual((r['min_shares'],r['max_shares']),(80,195))
    def test_full_sale_is_account_scoped(self):
        self.assertEqual(self.run_model([self.event(action='S (full)',value_bounds=[1000,2000])])['max_shares'],0)
        self.assertEqual(self.run_model([self.event(action='S (full)',position_id='other')])['status'],'review-required')
    def test_full_sale_cannot_erase_a_larger_inconsistent_balance(self):
        self.assertEqual(self.run_model([self.event(action='S (full)')])['reason'],'full-sale-inconsistent-with-balance')

    def test_prebaseline_trades_are_not_applied_twice(self):
        self.assertEqual(self.run_model([self.event(date='2025-12-30')])['min_shares'],100)
    def test_amendments_duplicates_and_missing_prices_stop_model(self):
        for events in [[self.event(filing_status='Deleted')],[self.event(),self.event()],[self.event(date='2026-01-03')],[self.event(action='E')]]:
            self.assertEqual(self.run_model(events)['status'],'review-required')
    def test_unknown_baseline_never_becomes_zero(self):
        self.base['value_bounds']=None;self.assertEqual(self.run_model([])['status'],'review-required')
    def test_only_explicit_reviewed_zero_can_start_at_zero(self):
        self.base['value_bounds']=[0,0]
        self.assertEqual(self.run_model([self.event()])['status'],'review-required')
        self.base['zero_basis']='explicit-house-none'
        self.assertEqual(self.run_model([self.event()])['min_shares'],5)

    def test_consistent_split_basis_does_not_double_apply_split(self):
        # A 2-for-1 split reflected in all historical prices doubles modeled shares,
        # while current value remains unchanged; no extra split multiplier is used.
        before=self.run_model([self.event()])
        self.prices=[{k:v/2 if k in ['close','low','high'] else v for k,v in p.items()} for p in self.prices]
        after=self.run_model([self.event()]);self.assertEqual(after['min_shares'],2*before['min_shares']);self.assertEqual(after['min_value'],before['min_value'])
if __name__=='__main__':unittest.main()
