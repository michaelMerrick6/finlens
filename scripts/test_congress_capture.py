import unittest
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup
from congress_filing_store import publish_filing
from parser_write_policy import read_only_parser_scope
from ingest_senate_official import parse_senate_html_table, parse_filed_date, PaperFilingReviewRequired
from ingest_house_official import extract_transactions_from_lines, HouseScanReviewRequired
from sync_recent_senate_filings import load_recent_senate_filings


class CongressCaptureTests(unittest.TestCase):
    def senate_parse(self, rows):
        soup = BeautifulSoup('<table class="table"><tbody>' + rows + '</tbody></table>', 'html.parser')
        with read_only_parser_scope():
            return parse_senate_html_table(soup, doc_key='test', member_id='B001288', first_name='Cory',
                last_name='Booker', filed_date='2026-09-09', source_url='https://example.com/report')

    def row(self, day='08/18/2026', direction='Purchase', amount='$1,001 - $15,000'):
        return f'<tr><td>1</td><td>{day}</td><td>Spouse</td><td>--</td><td>Private Company</td><td>Non-Public Stock</td><td>{direction}</td><td>{amount}</td></tr>'

    def test_html_refuses_whole_filing_when_second_row_is_invalid(self):
        for invalid in [self.row(day='bad'), self.row(direction='unclear'), self.row(amount=''),
                        '<tr><td>2</td><td>08/18/2026</td></tr>']:
            with self.subTest(invalid=invalid), self.assertRaises(PaperFilingReviewRequired):
                self.senate_parse(self.row() + invalid)

    def test_invalid_filed_date_never_defaults_to_today(self):
        with self.assertRaises(ValueError):
            parse_filed_date('not a date')

    def test_house_equal_physical_rows_are_preserved(self):
        row = ['Example Corporation (MSFT) [ST]', 'P 03/12/2026 03/13/2026 $1,001 - $15,000']
        with read_only_parser_scope():
            trades = extract_transactions_from_lines(row + row, '123', 'Cory', 'Test', 2026,
                [{'id':'T1', 'first_name':'Cory', 'last_name':'Test', 'chamber':'House'}], [])
        self.assertEqual(len(trades), 2)
        self.assertEqual(len({t['doc_id'] for t in trades}), 2)

    def test_house_refuses_partial_text_extraction(self):
        lines = ['Example (MSFT) [ST]', 'P 03/12/2026 03/13/2026 $1,001 - $15,000',
                 'Second Company [ST]', 'P unreadable date and amount']
        with read_only_parser_scope(), self.assertRaises(HouseScanReviewRequired):
            extract_transactions_from_lines(lines, '123', 'Cory', 'Test', 2026, [], [])

    def feed(self, size):
        rows = [['Test','Member','', f'<a href="/search/view/ptr/00000000-0000-0000-0000-{i:012d}/">PTR</a>',
                 '09/01/2026'] for i in range(size)]
        session = MagicMock()
        def post(*args, **kwargs):
            start = int(kwargs['data']['start'])
            response = MagicMock()
            response.json.return_value = {'recordsFiltered': size, 'data': rows[start:start+100]}
            return response
        session.post.side_effect = post
        return session

    def test_discovery_exhausts_more_than_five_senate_pages(self):
        from datetime import date
        with patch('sync_recent_senate_filings.congress_today', return_value=date(2026,9,14)):
            filings = load_recent_senate_filings(self.feed(601), days=30, limit=None)
        self.assertEqual(len(filings), 601)

    def test_discovery_fails_on_truncated_feed(self):
        session = self.feed(601)
        session.post.side_effect = None
        session.post.return_value.json.return_value = {'recordsFiltered':601, 'data':[]}
        with self.assertRaises(RuntimeError):
            load_recent_senate_filings(session, days=30, limit=None)

    def test_unstable_senate_pages_are_refetched_in_disjoint_date_ranges(self):
        from datetime import date
        import sync_recent_senate_filings as senate
        with patch.object(senate,'congress_today',return_value=date(2026,9,14)), \
             patch.object(senate,'load_recent_senate_filings',side_effect=[
                 senate.SenatePaginationError('duplicate at page boundary'),[{'doc_key':'one'}],[{'doc_key':'two'}]]) as fetch:
            rows=senate.load_senate_interval(object(),date(2026,9,1),date(2026,9,4))
        self.assertEqual(len(rows),2)
        self.assertEqual(fetch.call_args_list[1].kwargs['end_date'],date(2026,9,2))
        self.assertEqual(fetch.call_args_list[2].kwargs['days'],11)

    def test_empty_extraction_cannot_delete_a_filing(self):
        db = MagicMock()
        with self.assertRaises(ValueError):
            publish_filing(db, 'house-2026-123', [])
        db.rpc.assert_not_called()

    def test_atomic_rpc_failure_has_no_http_delete_fallback(self):
        db = MagicMock()
        db.rpc.return_value.execute.side_effect = RuntimeError('database rejected publication')
        trade = {'doc_id':'house-2026-123-0', 'transaction_date':'2026-09-01',
                 'published_date':'2026-09-03', 'transaction_type':'buy', 'amount_range':'$1,001 - $15,000'}
        with self.assertRaises(RuntimeError):
            publish_filing(db, 'house-2026-123', [trade], token='claimed')
        db.table.assert_not_called()

    def test_one_chamber_failure_does_not_prevent_other_chamber_capture(self):
        import capture_congress as capture
        with patch('sys.argv', ['capture_congress.py']), patch.object(capture, 'get_supabase_client'), \
             patch.object(capture, 'emit_summary') as summary, \
             patch.object(capture, 'capture_chamber', side_effect=[RuntimeError('House unavailable'),
                 {'chamber':'Senate','parse_failures':0}]) as run:
            with self.assertRaises(SystemExit) as failure:
                capture.main()
        self.assertEqual(failure.exception.code,1)
        self.assertEqual(run.call_count,2)
        self.assertEqual(summary.call_args.args[0]['parse_failures'],1)

    def test_document_failure_is_persisted_and_later_work_continues(self):
        import capture_congress as capture
        db = MagicMock()
        claims = iter([{'filing_id':'house-2026-1','claim_token':'one','filing':{}},
                       {'filing_id':'house-2026-2','claim_token':'two','filing':{}}])
        def rpc(name, args):
            result = MagicMock()
            result.execute.return_value.data = next(claims) if name == 'claim_congress_filing' else None
            return result
        db.rpc.side_effect = rpc
        with patch.object(capture,'discover',return_value=2), patch.object(capture,'load_congress_members',return_value=[]), \
             patch('ingest_house_official.load_company_lookup',return_value=[]), \
             patch('ingest_house_official.prepare_house_trades_for_insert',side_effect=lambda rows: rows), \
             patch('sync_recent_house_filings.ensure_referenced_companies'), \
             patch.object(capture,'run_document',side_effect=[{'status':'timeout'}, {'status':'completed','result':[{'doc_id':'house-2026-2-0'}]}]), \
             patch.object(capture,'publish_filing',return_value=(0,1)) as publish:
            summary = capture.capture_chamber(db,'House',start_year=2026,limit=2,seconds=120,document_timeout=30)
        self.assertEqual(summary['failed_doc_ids'],['house-2026-1'])
        self.assertEqual(summary['filings_completed'],1)
        self.assertEqual(summary['parse_failures'],1)
        self.assertEqual(publish.call_count,1)
        self.assertIn('fail_congress_filing',[call.args[0] for call in db.rpc.call_args_list])

    def test_freshness_does_not_pass_when_sources_failed_to_parse(self):
        from datetime import date
        from validate_recent_trade_capture import evaluate_lag
        for latest in [None,date(2026,9,1)]:
            result = evaluate_lag('congress',latest,date(2026,9,1),grace_days=2,
                details={'house':{'parse_failures':[{'error':'unreadable PDF'}]}})
            self.assertEqual(result['status'],'failed')

    def test_parser_worker_uses_fresh_client_and_disables_writes(self):
        import capture_congress as capture
        import ingest_house_official as house
        import ingest_senate_official as senate
        from parser_write_policy import parser_writes_allowed
        child_client = object()
        def parse(*args):
            self.assertIs(house.supabase,child_client)
            self.assertIs(senate.supabase,child_client)
            self.assertFalse(parser_writes_allowed())
            return 'trades',[{'member_id':'TEST'}]
        with patch.object(house,'supabase'), patch.object(senate,'supabase'), \
             patch.object(capture,'get_supabase_client',return_value=child_client), \
             patch('sync_recent_house_filings.parse_house_doc',side_effect=parse):
            self.assertEqual(capture.parse_claim('House',{},[],[]),[{'member_id':'TEST'}])


if __name__ == '__main__':
    unittest.main()
