import unittest
from continue_congress_backfill import next_batch

class BackfillChainTests(unittest.TestCase):
    def report(self):
        return {'chambers': [dict(chamber=c, filings_completed=10, filings_pending=100,
                                 failed_doc_ids=['unreadable'], parse_failures=1) for c in ['House','Senate']]}

    def test_document_failures_do_not_starve_untouched_work(self):
        self.assertEqual(next_batch(self.report(),20),19)

    def test_stops_on_infrastructure_error_exhausted_budget_or_no_progress(self):
        report=self.report()
        self.assertIsNone(next_batch(report,1))
        report['chambers'][0]['error']='Database unavailable'
        self.assertIsNone(next_batch(report,20))
        del report['chambers'][0]['error']
        for row in report['chambers']: row['filings_completed']=0
        self.assertIsNone(next_batch(report,20))

    def test_drained_queue_stops_even_with_failed_documents(self):
        report=self.report()
        for row in report['chambers']: row['filings_pending']=0
        self.assertIsNone(next_batch(report,20))

    def test_invalid_budget_and_missing_report_fail_closed(self):
        for budget in [0,101]:
            with self.assertRaises(ValueError): next_batch(self.report(),budget)
        with self.assertRaises(ValueError): next_batch({},20)

if __name__=='__main__': unittest.main()
