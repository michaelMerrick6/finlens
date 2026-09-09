import unittest
from unittest.mock import patch
import ingest_senate_official as senate


class OwnerMarkerTests(unittest.TestCase):
    def test_noisy_spouse_marker_is_not_a_stock_symbol(self):
        for text in ['= » (S) Not Fade Away LLC', '= N (S) MH Built to Last LLC', 'w (S) Not Fade Away LLC']:
            with self.subTest(text=text), patch.object(senate, 'supabase') as db:
                self.assertIsNone(senate.resolve_company_ticker(text, {'S', 'TKNO'}))
                db.table.assert_not_called()

    def test_real_symbol_after_owner_marker_is_preserved(self):
        self.assertEqual(senate.resolve_company_ticker('= a (S) Alpha Teknova Inc (Stock)(TKNO)', {'S', 'TKNO'}), 'TKNO')

    def test_company_name_with_s_symbol_is_preserved(self):
        self.assertEqual(senate.resolve_company_ticker('SentinelOne Inc (S)', {'S'}), 'S')


if __name__ == '__main__':
    unittest.main()
