import hashlib
import unittest
import json
import tempfile
from pathlib import Path
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

    def test_new_reviewed_sources_preserve_all_physical_rows(self):
        expected = {'house-2026-9115711': 0, 'house-2026-9115762': 0,
                    'house-2026-9116146': 0, 'house-2026-9115808': 1,
                    'house-2026-9116249': 1, 'senate-0663c4d9-f5f8-43d4-81d3-6d46b49d4dfd': 3,
                    'house-2026-9116267': 274, 'house-2026-9116290': 11,
                    'house-2026-9116292': 10, 'house-2026-9116308': 7,
                    'house-2026-9116311': 0, 'house-2026-9116326': 8,
                    'senate-3a4c5095-028a-4614-a692-836719da4e63': 46,
                    'senate-ec20cd93-6702-4a29-b3a6-983f4b17f365': 32}
        for key, count in expected.items():
            data = reviewed.load_reviewed_filing(key)
            self.assertEqual(len(reviewed.reviewed_trades(key, data)), count)
            self.assertEqual(len({(r['page'], r['row']) for r in data['rows']}), count)
        mann = reviewed.load_reviewed_filing('house-2026-9116292')
        self.assertEqual({r['transaction_date'] for r in mann['rows']}, {'2024-08-20', '2024-09-09'})
        self.assertEqual(mann['published_date'], '2026-08-13')

    def test_harshbarger_bonds_keep_repeated_rows_without_stock_tickers(self):
        data = reviewed.load_reviewed_filing('house-2026-9116331')
        trades = reviewed.reviewed_trades('house-2026-9116331', data)
        self.assertEqual(len(trades), 5)
        self.assertEqual({r['ticker'] for r in trades}, {'N/A'})
        self.assertEqual({r['asset_type'] for r in trades}, {'Bond'})
        self.assertEqual(sum(r['transaction_type'] == 'buy' for r in trades), 3)
        self.assertEqual(sum(r['transaction_type'] == 'sell' for r in trades), 2)
        self.assertEqual({r['amount_range'] for r in trades}, {'$15,001 - $50,000'})
        self.assertEqual(trades[0]['asset_name'], trades[3]['asset_name'])
        self.assertNotEqual(trades[0]['doc_id'], trades[3]['doc_id'])
        self.assertEqual(trades[1]['asset_name'], trades[4]['asset_name'])
        self.assertEqual(trades[2]['transaction_date'], '2026-08-06')

    def test_fetterman_cover_letter_and_examples_are_not_trades(self):
        key = 'senate-3b1affbf-2359-4fef-980d-1d2600cb8731'
        data = reviewed.load_reviewed_filing(key)
        trades = reviewed.reviewed_trades(key, data)
        self.assertEqual(len(data['image_rgb_sha256']), 2)
        self.assertEqual([(r['page'], r['row']) for r in data['rows']], [(2, 1), (2, 2)])
        self.assertEqual(len(trades), 2)
        self.assertEqual({r['asset_type'] for r in trades}, {'Bond'})
        self.assertEqual({r['ticker'] for r in trades}, {'N/A'})
        self.assertEqual({r['transaction_type'] for r in trades}, {'buy'})
        self.assertEqual({r['transaction_date'] for r in trades}, {'2024-06-06', '2024-06-21'})
        with self.assertRaisesRegex(ValueError, 'source changed'):
            reviewed.reviewed_senate_trades(key, [], 'F000479', '2024-07-12')

    def test_empty_review_requires_explicit_evidence_and_unchanged_source(self):
        key = 'house-2026-9116311'
        data = reviewed.load_reviewed_filing(key)
        with self.assertRaisesRegex(ValueError, 'source changed'):
            reviewed.reviewed_house_trades(key, b'changed declaration')
        with tempfile.TemporaryDirectory() as directory:
            data.pop('verified_no_trades')
            Path(directory, key + '.json').write_text(json.dumps(data))
            with patch.object(reviewed, 'REVIEW_DIR', Path(directory)), self.assertRaisesRegex(ValueError, 'no-transaction evidence'):
                reviewed.load_reviewed_filing(key)

    def test_reviewed_empty_declaration_does_not_fall_back_to_ocr(self):
        import sync_recent_house_filings as sync
        key = 'house-2026-9116311'
        data = reviewed.load_reviewed_filing(key)
        source = b'reviewed one-page declaration'
        data['source_sha256'] = hashlib.sha256(source).hexdigest()
        filing = dict(year=2026, doc_id='9116311', filing_date_raw='8/20/2026')
        with patch.object(reviewed, 'load_reviewed_filing', return_value=data), \
             patch.object(sync.requests, 'get') as get, \
             patch.object(sync, 'extract_best_text_transactions') as parser:
            get.return_value.content = source
            self.assertEqual(sync.parse_house_doc(filing, [], []), ('no_trade', []))
            parser.assert_not_called()
            filing['filing_date_raw'] = '8/21/2026'
            with self.assertRaisesRegex(ValueError, 'metadata changed'):
                sync.parse_house_doc(filing, [], [])

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
