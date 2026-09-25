import unittest
from strategies.import_trump_annual import parse_pages, parse_value
from strategies.build_trump_stocks import build


class TrumpAnnualImportTests(unittest.TestCase):
    def test_stock_identity_rejects_fund_or_preferred_listing_and_duplicate_rows(self):
        annual = {'sha256': 'test', 'rows': [{'id': '1-1', 'name': 'APPLE INC', 'low': 1001,
                                             'incomeType': 'Dividend', 'account': 'INVESTMENT ACCOUNT #1'}]}
        identity = {'AAPL': {'name': 'Apple', 'aliases': ['APPLE INC']}}
        listing = {'ETF': 'N', 'Test Issue': 'N', 'Security Name': 'Apple Inc. Common Stock'}
        result = build(annual, identity, {'AAPL': listing})
        self.assertFalse(result['complete'])
        with self.assertRaises(ValueError):
            build(annual, identity, {'AAPL': {**listing, 'ETF': 'Y'}})
        with self.assertRaises(ValueError):
            build(annual, identity, {'AAPL': {**listing, 'Security Name': 'Preferred Stock'}})
        with self.assertRaises(ValueError):
            build(annual, {**identity, 'OTHER': identity['AAPL']}, {'AAPL': listing, 'OTHER': listing})

    def test_interest_bearing_asset_cannot_become_stock_through_a_name_match(self):
        annual = {'sha256': 'test', 'rows': [{'id': '1-1', 'name': 'APPLE INC', 'low': 1001,
                                             'incomeType': 'Interest/Capital Gains', 'account': 'INVESTMENT ACCOUNT #1'}]}
        with self.assertRaises(ValueError):
            build(annual, {'AAPL': {'name': 'Apple', 'aliases': ['APPLE INC']}},
                  {'AAPL': {'ETF': 'N', 'Test Issue': 'N', 'Security Name': 'Apple Inc. Common Stock'}})

    def test_missing_and_unbounded_values_preserve_uncertainty(self):
        self.assertEqual(parse_value('None (or less than $1,001)'), (0, 1000))
        self.assertEqual(parse_value('Over $50,000,000'), (50000001, None))
        self.assertEqual(parse_value('$1,000,001 - $5,000,000'), (1000001, 5000000))

    def test_ocr_corrupted_range_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_value('$1,OOO,OO1 - $5,000,000')
        with self.assertRaises(ValueError):
            parse_value('$1,000,001')

    def test_separate_accounts_reusing_line_numbers_remain_separate(self):
        text = '''Part 6: Other Assets and Income
1  APPLE INC  N/A  $1,001 - $15,000  Dividend
1  APPLE INC COM  N/A  $50,001 - $100,000  Dividend
'''
        rows, pages = parse_pages(text)
        self.assertEqual(pages, [1])
        self.assertEqual([r['id'] for r in rows], ['1-1', '1-2'])
        self.assertEqual([r['line'] for r in rows], [1, 1])
        self.assertEqual([r['low'] for r in rows], [1001, 50001])

    def test_partial_row_extraction_fails_closed(self):
        with self.assertRaises(ValueError):
            parse_pages('Part 6: Other Assets and Income\n1  APPLE INC  unreadable range')


if __name__ == '__main__':
    unittest.main()
