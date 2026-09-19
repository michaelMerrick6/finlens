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

    def test_reviewed_additions_require_exact_sources_and_security(self):
        import json
        from pathlib import Path
        from copy import deepcopy
        review = json.loads((Path(__file__).resolve().parents[1] / 'config/reviewed_13f_additions.json').read_text())[0]
        base = self.filing('ORIGINAL', 'LEN-B', '2025-05-15', review['baseline']['accession'])
        added = self.filing('NEW HOLDINGS', 'LEN-B', '2025-08-14', review['amendment']['accession'])
        for parsed, source, shares, value in [(base, review['baseline'], 152572, 16641028),
                                               (added, review['amendment'], 202, 22032)]:
            parsed.update(source_sha256=source['sha256'], source_url=source['source_url'])
            parsed['holdings'][0].update(_cusip='526057302', _share_class='B',
                source_url=source['source_url'], shares_held=shares, value_held=value)
        rows, issues = compose_periods([base, added, added])
        self.assertFalse(issues)
        position = rows[0]['holdings'][0]
        self.assertEqual(position['shares_held'], 152774)
        self.assertEqual(position['value_held'], 16663060)
        self.assertEqual(len(position['_source_urls']), 2)
        self.assertEqual(position['published_date'], '2025-08-14')
        for field in ['source_sha256', 'source_url', 'accession']:
            changed = deepcopy(added)
            changed[field] = 'changed'
            self.assertTrue(compose_periods([base, changed])[1])
        changed = deepcopy(added)
        changed['holdings'][0]['_cusip'] = 'wrong'
        self.assertTrue(compose_periods([base, changed])[1])
        # The pristine source objects must be reusable, not mutated by composition.
        self.assertEqual(base['holdings'][0]['shares_held'], 152572)

    def test_missing_quarter_does_not_create_false_exits(self):
        periods=[dict(report_period='2024-12-31',holdings=[{'ticker':'AAPL'}]),
                 dict(report_period='2025-06-30',holdings=[])]
        self.assertEqual(previous_quarter_holdings(periods,1),[])
        periods[0]['report_period']='2025-03-31'
        self.assertEqual(previous_quarter_holdings(periods,1),[{'ticker':'AAPL'}])

if __name__ == '__main__':unittest.main()
