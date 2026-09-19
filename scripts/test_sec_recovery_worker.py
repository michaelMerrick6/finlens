import unittest
from unittest.mock import Mock, patch
import requests
import ingest_sec_daily as worker

class SecWorkerTests(unittest.TestCase):
    def run_worker(self, parsed, pending=0):
        client=Mock()
        client.table.return_value.select.return_value.neq.return_value.limit.return_value.execute.return_value.count=pending
        filings=[dict(accession='0000000001-26-000001',source_url='https://www.sec.gov/example',filed_date='2026-09-01')]
        with patch.object(worker,'get_supabase_client',return_value=client), \
             patch.object(worker,'create_session'), \
             patch.object(worker,'load_recent_form4_filings',return_value=[]), \
             patch('sec_filing_store.register_filings') as register, \
             patch('sec_filing_store.pending_filings',return_value=filings), \
             patch.object(worker,'parse_form4_filing',side_effect=parsed), \
             patch.object(worker,'upsert_companies'), \
             patch('sec_filing_store.publish',return_value=0) as publish, \
             patch('sec_filing_store.fail') as fail:
            try: worker.main()
            except SystemExit as exc: return exc.code,publish.call_count,fail.call_count
            return 0,publish.call_count,fail.call_count

    def test_queued_filing_processed_even_when_no_longer_in_feed(self):
        self.assertEqual(self.run_worker([{'rows':[]}]),(0,1,0))

    def test_parse_failure_never_marks_complete(self):
        self.assertEqual(self.run_worker([ValueError('bad XML')],pending=1),(1,0,1))

    def test_rate_limit_is_retained_and_fails_run(self):
        response=requests.Response();response.status_code=429
        self.assertEqual(self.run_worker([requests.HTTPError('rate limited',response=response)],pending=1),(1,0,1))

if __name__ == '__main__':unittest.main()
