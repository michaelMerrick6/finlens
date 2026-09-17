import copy
import json
from pathlib import Path
import unittest
from reviewed_holdings_corrections import apply_corrections
from stage_priority_models import could_affect_position, unresolved_positions
from model_holdings_ranges import model_range

ROOT=Path(__file__).resolve().parents[1]
class CorrectionTests(unittest.TestCase):
    def setUp(self):
        self.links=json.loads((ROOT/'docs/research/reviewed-holdings-corrections.json').read_text())['links']
        self.events=copy.deepcopy(next(m for m in json.loads((ROOT/'docs/research/priority-holdings-ledger.json').read_text())['members'] if m['member_id']=='H001082')['events'])
    def test_originals_and_deletion_have_no_balance_effect(self):
        apply_corrections(self.events,self.links)
        linked={e['id']:e for e in self.events}
        for link in self.links:
            old=linked[link['original_id']];new=linked[link['correction_id']]
            self.assertEqual(old['balance_effect'],'none')
            self.assertFalse(could_affect_position(old,dict(ticker=old['ticker'])))
            self.assertEqual(new['replaces_event_id'],old['id'])
            if link['filing_status']=='Deleted':
                self.assertFalse(could_affect_position(new,dict(ticker=new['ticker'])))
            else:
                self.assertEqual(new['account'],'Hern Family Revocable Trust')
                self.assertFalse(new['charitable'])
        self.assertFalse(any(r['ticker']=='VSNT' for r in unresolved_positions(dict(events=self.events))))
    def test_changed_source_or_ambiguous_original_blocks(self):
        for mode in ['hash','duplicate','missing']:
            events=copy.deepcopy(self.events)
            original=next(e for e in events if e['id']==self.links[0]['original_id'])
            if mode=='hash':original['source_sha256']='changed'
            if mode=='duplicate':events.append(dict(original,id='another-row'))
            if mode=='missing':events.remove(original)
            with self.subTest(mode=mode),self.assertRaises(ValueError):apply_corrections(events,self.links)
    def test_reviewed_amendment_is_applied_once(self):
        base=dict(id='a',date='2025-12-31',source='annual',value_bounds=[1000,2000])
        prices=[dict(date='2025-12-31',close=10),dict(date='2026-01-02',close=10,low=10,high=10)]
        original=dict(id='old',date='2026-01-02',position_id='a',balance_effect='none')
        corrected=dict(id='new',date='2026-01-02',position_id='a',review_status='matched-account',filing_status='Amended',correction_resolution='reviewed-original-link',replaces_event_id='old',action='S',value_bounds=[100,200])
        result=model_range(base,[original,corrected],prices,'2026-01-02')
        self.assertEqual((result['min_shares'],result['max_shares']),(80,190))
        self.assertEqual(len(result['trace']),1)

if __name__=='__main__':unittest.main()
