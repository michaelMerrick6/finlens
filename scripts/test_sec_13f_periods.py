import unittest
from sec_13f_periods import compose_periods, previous_quarter_holdings

class AmendmentTests(unittest.TestCase):
    def filing(self, kind, ticker, date, accession):
        return dict(report_period='2025-03-31',published_date=date,accession=accession,
            amendment_type=kind,rows_supported=1,rows_resolved=1,
            holdings=[dict(ticker=ticker,shares_held=10,value_held=100,source_url=accession)])

    def test_new_holdings_add_to_original_once(self):
        base=self.filing('ORIGINAL','AAPL','2025-05-15','base')
        added=self.filing('NEW HOLDINGS','UNH','2025-08-14','add')
        rows,issues=compose_periods([added,base,added])
        self.assertEqual(issues,[])
        self.assertEqual({x['ticker'] for x in rows[0]['holdings']},{'AAPL','UNH'})
        self.assertEqual(rows[0]['rows_resolved'],2)
        self.assertEqual({x['published_date'] for x in rows[0]['holdings']},{'2025-08-14'})
        self.assertEqual({x['source_url'] for x in rows[0]['holdings']},{'base','add'})
        self.assertEqual(len(base['holdings']),1)

    def test_restatement_replaces_original(self):
        rows,issues=compose_periods([self.filing('ORIGINAL','AAPL','2025-05-15','base'),self.filing('RESTATEMENT','UNH','2025-08-14','restated')])
        self.assertFalse(issues)
        self.assertEqual([x['ticker'] for x in rows[0]['holdings']],['UNH'])

    def test_missing_base_and_unknown_amendments_are_not_published(self):
        for kind in ['NEW HOLDINGS','UNKNOWN']:
            rows,issues=compose_periods([self.filing(kind,'UNH','2025-08-14','add')])
            self.assertEqual(rows,[])
            self.assertEqual(len(issues),1)

    def test_overlap_needs_review_instead_of_doubling_position(self):
        rows,issues=compose_periods([self.filing('ORIGINAL','AAPL','2025-05-15','base'),self.filing('NEW HOLDINGS','AAPL','2025-08-14','add')])
        self.assertEqual(rows,[])
        self.assertIn('Overlapping',issues[0]['error'])

    def test_missing_quarter_does_not_create_false_exits(self):
        periods=[dict(report_period='2024-12-31',holdings=[{'ticker':'AAPL'}]),
                 dict(report_period='2025-06-30',holdings=[])]
        self.assertEqual(previous_quarter_holdings(periods,1),[])
        periods[0]['report_period']='2025-03-31'
        self.assertEqual(previous_quarter_holdings(periods,1),[{'ticker':'AAPL'}])

if __name__ == '__main__':unittest.main()
