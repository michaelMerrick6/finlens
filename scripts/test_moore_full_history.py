import json,unittest
from pathlib import Path
from model_zero_start_scenario import model_zero_start_scenario

class MooreHistoryTests(unittest.TestCase):
    def event(self,n,date,action,amount):
        return dict(id=str(n),ticker='T',date=date,action=action,value_bounds=amount,filing_status='New',source='fixture',page=1)
    def run_model(self,events):
        prices=[dict(date=e['date'],low=10,high=10) for e in events]
        return model_zero_start_scenario(events,prices,'2025-01-01','2026-09-17')
    def test_sales_constrain_long_only_scenario_without_claiming_baseline(self):
        result=self.run_model([self.event(1,'2025-01-02','P',[100,200]),self.event(2,'2025-01-03','S',[150,250])])
        self.assertEqual(result['min_scenario_shares'],0)
        self.assertEqual(result['max_scenario_shares'],5)
        self.assertFalse(result['current_holdings_eligible'])
        self.assertFalse(result['eligible_for_portfolio_total'])
    def test_impossible_zero_baseline_does_not_silently_reset(self):
        r=self.run_model([self.event(1,'2025-01-02','S',[100,200])])
        self.assertEqual(r['status'],'inconsistent-zero-start-scenario')
    def test_corrections_and_ambiguous_intraday_order_block(self):
        events=[self.event(1,'2025-01-02','P',[100,200]),self.event(2,'2025-01-02','S',[100,200])]
        self.assertEqual(self.run_model(events)['reason'],'unknown-intraday-order')
        events[0]['filing_status']='Amended'
        self.assertEqual(self.run_model(events)['reason'],'correction-needs-resolution')
    def test_reviewed_hy_rows_match_independent_transcription(self):
        report=json.loads((Path(__file__).resolve().parents[1]/'docs/research/moore-full-transaction-history.json').read_text())
        hy=next(t for t in report['targets'] if t['ticker']=='HY')
        expected=[('2025-08-07','P',[15001,50000]),('2025-08-12','P',[15001,50000]),('2025-08-13','S',[50001,100000]),('2025-09-11','P',[50001,100000]),('2025-09-19','S',[50001,100000]),('2025-10-08','P',[50001,100000]),('2025-10-10','P',[15001,50000]),('2025-10-14','P',[15001,50000]),('2025-10-24','S',[100001,250000]),('2025-11-03','P',[50001,100000]),('2025-11-05','P',[15001,50000]),('2025-11-12','P',[50001,100000]),('2025-11-14','P',[15001,50000]),('2025-11-20','P',[15001,50000]),('2025-12-03','S',[100001,250000]),('2025-12-31','P',[15001,50000]),('2026-01-05','S',[15001,50000])]
        self.assertEqual([(e['date'],e['action'],e['value_bounds']) for e in hy['events']],expected)
        self.assertEqual(len(report['documents']),21)
        self.assertFalse(report['extraction_issues'])
        for t in report['targets']: self.assertFalse(t['current_holdings_eligible'])

if __name__=='__main__':unittest.main()
