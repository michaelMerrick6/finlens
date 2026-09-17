import unittest
from audit_fields_holdings import purchase_bounds

class PurchaseAuditTests(unittest.TestCase):
    def setUp(self):
        self.event=dict(action='P',filing_status='New',date='2026-06-04',value_bounds=[1001,15000])
        self.price=dict(date='2026-06-04',low=68,high=80)

    def test_purchase_is_not_a_current_holding(self):
        result=purchase_bounds(self.event,self.price)
        self.assertEqual(result['min_acquired_shares'],1001/80)
        self.assertEqual(result['max_acquired_shares'],15000/68)
        self.assertFalse(result['current_holdings_eligible'])
        self.assertNotIn('min_shares',result)

    def test_ipo_allocation_widens_market_price_envelope(self):
        result=purchase_bounds(self.event,self.price,60)
        self.assertEqual(result['execution_price_bounds'],[60,80])
        self.assertEqual(result['max_acquired_shares'],250)

    def test_invalid_rows_cannot_be_quantified(self):
        for change in [dict(action='S'),dict(filing_status='Amended'),dict(value_bounds=None),dict(value_bounds=[200,100])]:
            with self.subTest(change=change),self.assertRaises(ValueError):
                purchase_bounds(dict(self.event,**change),self.price)
        for change in [dict(date='2026-06-05'),dict(low=None),dict(high=60),dict(high=float('nan'))]:
            with self.subTest(change=change),self.assertRaises(ValueError):
                purchase_bounds(self.event,dict(self.price,**change))

if __name__=='__main__':
    unittest.main()
