import hashlib
import unittest
from unittest.mock import patch
from PIL import Image
import reviewed_congress_filings as reviewed
from emit_signal_events import build_politician_events
from notification_compiler import compile_grouped_events, compile_filing_summary_events

HOUSE = 'house-2026-9116328'
SENATE = 'senate-929216d5-5dbd-429c-858c-1e9332924627'


class ReviewedFilingsTests(unittest.TestCase):
    def test_counts_and_source_positions(self):
        for prefix, count in [(HOUSE, 244), (SENATE, 111)]:
            data = reviewed.load_reviewed_filing(prefix)
            trades = reviewed.reviewed_trades(prefix, data)
            self.assertEqual(len(trades), count)
            self.assertEqual(len({r['doc_id'] for r in trades}), count)
            self.assertEqual(trades[0]['doc_id'], prefix + '-0')

    def test_known_corrupt_fields_corrected(self):
        house = reviewed.load_reviewed_filing(HOUSE)
        row = next(r for r in house['rows'] if (r['page'], r['row']) == (15, 16))
        self.assertEqual((row['ticker'], row['transaction_date'], row['transaction_type'], row['amount_range']),
                         ('CL', '2026-08-03', 'sell', '$1,001 - $15,000'))
        senate = reviewed.load_reviewed_filing(SENATE)
        self.assertNotIn('S', {r['ticker'] for r in senate['rows']})
        self.assertNotIn('exchange', {r['transaction_type'] for r in senate['rows']})
        # Identical-looking private transactions remain distinct across accounts.
        private = [r for r in senate['rows'] if r['asset_name'] == 'Not Fade Away LLC']
        self.assertGreater(len(private), len({(r['transaction_date'], r['transaction_type'], r['amount_range']) for r in private}))

    def test_source_changes_fail_closed(self):
        with self.assertRaisesRegex(ValueError, 'source changed'):
            reviewed.reviewed_house_trades(HOUSE, b'changed PDF')
        with self.assertRaisesRegex(ValueError, 'source changed'):
            reviewed.reviewed_senate_trades(SENATE, [], 'B001277', '2026-08-31')

    def test_unknown_source_uses_normal_parser(self):
        self.assertIsNone(reviewed.reviewed_house_trades('house-2026-123', b'any'))
        self.assertIsNone(reviewed.reviewed_senate_trades('senate-123', [], 'any', '2026-09-01'))

    def test_senate_exact_source_and_metadata(self):
        data = reviewed.load_reviewed_filing(SENATE)
        im = Image.new('RGB', (2, 2), 'white')
        data['image_rgb_sha256'] = [hashlib.sha256(im.tobytes()).hexdigest()]
        with patch.object(reviewed, 'load_reviewed_filing', return_value=data):
            self.assertEqual(len(reviewed.reviewed_senate_trades(SENATE, [im], 'B001277', '2026-08-31')), 111)
            with self.assertRaisesRegex(ValueError, 'metadata changed'):
                reviewed.reviewed_senate_trades(SENATE, [im], 'wrong-member', '2026-08-31')

    def test_no_path_traversal(self):
        with self.assertRaises(ValueError):
            reviewed.load_reviewed_filing('house-../../secret')

    def test_grouped_tickers_produce_one_filing_summary(self):
        trades = reviewed.reviewed_trades(HOUSE, reviewed.load_reviewed_filing(HOUSE))
        # Two buys in one ticker plus a different ticker from the same filing.
        sample = [r for r in trades if r['ticker'] == 'MRK' and r['transaction_type'] == 'buy']
        sample += [next(r for r in trades if r['ticker'] == 'BSX')]
        _, events = build_politician_events(sample)
        for i, event in enumerate(events):
            event['id'] = str(i)
        summaries = compile_filing_summary_events(compile_grouped_events(events))
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]['payload']['summary_filing_key'], HOUSE)
        self.assertEqual(summaries[0]['payload']['summary_trade_count'], len(sample))


if __name__ == '__main__':
    unittest.main()
