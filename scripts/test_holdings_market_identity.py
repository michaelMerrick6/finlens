import unittest
import json
from pathlib import Path
from holdings_market_identity import blocking_actions, provider_symbol, validate_price_identity

class MarketIdentityTests(unittest.TestCase):
    def test_only_explicit_aliases_are_mapped(self):
        self.assertEqual(provider_symbol('BRK.B'),'BRK-B')
        self.assertEqual(provider_symbol('BRK.A'),'BRK.A')
        self.assertEqual(provider_symbol('SMCYY'),'SMCYY')
        meta=dict(symbol='BRK-B',currency='USD',instrumentType='EQUITY',longName='Berkshire Hathaway Inc.')
        self.assertEqual(validate_price_identity('BRK.B',meta)['filed_symbol'],'BRK.B')
        for change in [dict(symbol='BRK-A'),dict(currency='CAD'),dict(longName='Another issuer'),dict(instrumentType='ETF')]:
            with self.subTest(change=change),self.assertRaises(ValueError):
                validate_price_identity('BRK.B',dict(meta,**change))
    def test_rebuilt_candidates_keep_alias_and_block_known_spinoff(self):
        root=Path(__file__).resolve().parents[1]
        members=json.loads((root/'docs/research/priority-holdings-models.json').read_text())['members']
        moore=next(m for m in members if m['member_id']=='M001236')
        berkshire=next(r for r in moore['rows'] if r['ticker']=='BRK.B')
        self.assertEqual(berkshire['model']['status'],'conditional-model')
        self.assertEqual(berkshire['model']['price_identity']['provider_symbol'],'BRK-B')
        honeywell=[r for m in members for r in m['rows'] if r['ticker']=='HON']
        self.assertTrue(honeywell)
        for row in honeywell:
            self.assertEqual(row['model']['status'],'review-required')
            self.assertIn('corporate-action-reconciliation-required',row['model']['reason'])

    def test_spinoff_blocks_crossing_baseline_not_future_or_other_ticker(self):
        actions=blocking_actions('HON','2025-12-31','2026-09-17')
        self.assertEqual(len(actions),1)
        self.assertEqual(actions[0]['distributed_ticker'],'HONA')
        self.assertEqual(blocking_actions('HON','2025-12-31','2026-06-28'),[])
        self.assertEqual(blocking_actions('HON','2026-12-31','2027-01-01'),[])
        self.assertEqual(blocking_actions('HOG','2025-12-31','2026-09-17'),[])

if __name__=='__main__':unittest.main()
