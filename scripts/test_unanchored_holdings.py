import unittest
from model_unanchored_holdings import model_unanchored_flow

class UnanchoredTests(unittest.TestCase):
    def event(self,id,action,amount):
        return dict(id=id,ticker='TEST',date='2026-01-02',action=action,value_bounds=amount,
                    filing_status='New',source='reviewed-source',page=1)
    def run_model(self,events):
        return model_unanchored_flow(events,[dict(date='2026-01-02',low=10,high=20)],'2025-12-31','2026-09-17')
    def test_purchase_flow_is_not_total_holdings(self):
        m=self.run_model([self.event('a','P',[1000,2000])])
        self.assertEqual((m['min_net_share_change'],m['max_net_share_change']),(50,200))
        self.assertIsNone(m['max_current_shares'])
        self.assertIsNone(m['min_current_shares'])
        self.assertFalse(m['eligible_for_portfolio_total'])
        self.assertFalse(m['eligible_for_ranking'])
    def test_equal_brackets_do_not_imply_full_exit(self):
        m=self.run_model([self.event('a','P',[1000,2000]),self.event('b','S',[1000,2000])])
        self.assertEqual((m['min_net_share_change'],m['max_net_share_change']),(-150,150))
        self.assertIsNone(m['max_current_shares'])
    def test_duplicate_correction_full_sale_and_mixed_tickers_fail_closed(self):
        a=self.event('a','P',[1000,2000])
        for events in [[a,a],[dict(a,filing_status='Amended')],[dict(a,action='S (full)')],
                       [a,dict(a,id='b',ticker='OTHER')]]:
            with self.subTest(events=events):
                self.assertEqual(self.run_model(events)['status'],'review-required')
    def test_future_and_baseline_events_are_excluded(self):
        a=self.event('a','P',[1000,2000])
        m=self.run_model([a,dict(a,id='b',date='2025-12-31'),dict(a,id='c',date='2026-10-01')])
        self.assertEqual(len(m['trace']),1)

if __name__=='__main__':unittest.main()
