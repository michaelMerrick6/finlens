import unittest
from moore_fractional_shares import ACTIONS, apply_allowance

class FractionalTests(unittest.TestCase):
    def model(self):
        return dict(status='conditional-model',min_shares=46.4,max_shares=748.46,latest_price=dict(close=3),assumptions=[])
    def run_case(self,ticker,events=None,as_of='2026-09-17'):
        a=ACTIONS[ticker]
        evidence=dict(as_of='2026-09-17',symbols={ticker:dict(splits=[{k:a[k] for k in ['date','numerator','denominator']}])})
        return apply_allowance(self.model(),ticker,'2025-12-31',events or [],as_of,evidence)
    def test_round_up_widens_only_upper_endpoint_no_second_split(self):
        m=self.run_case('GNPX')
        self.assertEqual((m['min_shares'],m['max_shares']),(46.4,749))
        self.assertEqual(m['max_value'],2247)
    def test_cash_out_widens_only_lower_endpoint(self):
        m=self.run_case('TZA')
        self.assertEqual((m['min_shares'],m['max_shares']),(46,748.46))
    def test_later_sale_or_stale_review_blocks(self):
        for m in [self.run_case('GNPX',[dict(date='2026-08-01')]),self.run_case('TZA',as_of='2026-09-18')]:
            self.assertEqual(m['status'],'review-required')
    def test_zero_stays_zero(self):
        m=self.model();m.update(min_shares=0,max_shares=0)
        a=ACTIONS['TZA'];e=dict(as_of='2026-09-17',symbols={'TZA':dict(splits=[{k:a[k] for k in ['date','numerator','denominator']}])})
        r=apply_allowance(m,'TZA','2025-12-31',[],'2026-09-17',e)
        self.assertEqual((r['min_shares'],r['max_shares']),(0,0))

if __name__=='__main__':unittest.main()
