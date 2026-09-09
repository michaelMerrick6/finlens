import unittest
from audit_trade_comparison import compare_transactions

class ComparisonTests(unittest.TestCase):
    def test_equal_count_changed_amount_is_not_success(self):
        difference = compare_transactions([{'ticker':'ABC','amount_range':'$1,001 - $15,000'}], [{'ticker':'ABC','amount_range':'$15,001 - $50,000'}])
        self.assertEqual(difference['unmatched_parsed_rows'], 1)
        self.assertEqual(difference['unmatched_stored_rows'], 1)
    def test_order_does_not_matter_but_duplicates_do(self):
        rows=[{'ticker':'ABC'}, {'ticker':'XYZ'}]
        self.assertIsNone(compare_transactions(rows, rows[::-1]))
        self.assertEqual(compare_transactions(rows + [rows[0]], rows)['unmatched_parsed_rows'], 1)

if __name__ == '__main__': unittest.main()
