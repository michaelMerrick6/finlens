"""Real database rollback and idempotency tests; never use a production DSN."""
import json
import os
from pathlib import Path
import unittest

DSN = os.environ.get('VAIL_TEST_POSTGRES_DSN')

@unittest.skipUnless(DSN, 'Requires isolated test PostgreSQL')
class SecRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.db = psycopg.connect(DSN, autocommit=True)
        cls.db.execute("DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role; END IF; END $$")
        cls.db.execute('SET search_path TO public,extensions')
        root = Path(__file__).resolve().parents[1]
        for file in ['supabase_schema.sql', 'supabase_vail_sec_recovery.sql']:
            cls.db.execute((root/file).read_text())

    @classmethod
    def tearDownClass(cls): cls.db.close()

    def setUp(self):
        self.db.execute('BEGIN')
        self.db.execute("INSERT INTO companies(ticker,name) VALUES('SEC_TEST','Test') ON CONFLICT DO NOTHING")
        self.db.execute("INSERT INTO sec_filing_queue(accession,filing) VALUES('0000000001-26-000001','{}')")

    def tearDown(self): self.db.execute('ROLLBACK')

    def publish(self, rows):
        return self.db.execute('SELECT publish_insider_filing(%s,%s::jsonb)',('0000000001-26-000001',json.dumps(rows))).fetchone()[0]

    def trade(self):
        return dict(ticker='SEC_TEST',filer_name='Test',filer_relation='Director',transaction_date='2026-09-01',published_date='2026-09-02',transaction_code='buy',amount=10,price=2,value=20,source_url='https://www.sec.gov/Archives/0000000001-26-000001.txt#1')

    def holding(self):
        return dict(fund_name='SEC Test Fund',ticker='SEC_TEST',report_period='2026-06-30',published_date='2026-08-01',shares_held=10,value_held=20,source_url='https://www.sec.gov/test')

    def test_retry_does_not_duplicate_or_change_row_id(self):
        self.publish([self.trade(),self.trade()])
        before=self.db.execute("SELECT id FROM insider_trades WHERE ticker='SEC_TEST' ORDER BY id").fetchall()
        self.publish([self.trade(),self.trade()])
        self.assertEqual(before,self.db.execute("SELECT id FROM insider_trades WHERE ticker='SEC_TEST' ORDER BY id").fetchall())
        self.assertEqual(len(before),2)

    def test_failed_insert_preserves_old_rows_and_queue_state(self):
        self.publish([self.trade()])
        bad=self.trade();bad['ticker']='MISSING_X'
        with self.assertRaises(Exception):
            with self.db.transaction(): self.publish([bad])
        self.assertEqual(self.db.execute("SELECT count(*) FROM insider_trades WHERE ticker='SEC_TEST'").fetchone()[0],1)

    def test_confirmed_zero_trade_filing_is_completed(self):
        self.publish([])
        self.assertEqual(self.db.execute("SELECT status FROM sec_filing_queue WHERE accession='0000000001-26-000001'").fetchone()[0],'complete')

    def test_fund_insert_failure_does_not_erase_existing_period(self):
        def replace(rows):return self.db.execute('SELECT replace_13f_period(%s,%s,%s::jsonb)',('SEC Test Fund','2026-06-30',json.dumps(rows)))
        replace([self.holding()])
        bad=self.holding();bad['ticker']='MISSING_X'
        with self.assertRaises(Exception):
            with self.db.transaction():replace([bad])
        self.assertEqual(self.db.execute("SELECT shares_held FROM institutional_holdings WHERE fund_name='SEC Test Fund'").fetchone()[0],10)
        with self.assertRaises(Exception):
            with self.db.transaction():replace([])

    def test_other_period_with_same_publication_date_survives(self):
        row=self.holding();row['report_period']='2026-03-31'
        self.db.execute('SELECT replace_13f_period(%s,%s,%s::jsonb)',('SEC Test Fund','2026-03-31',json.dumps([row])))
        self.db.execute('SELECT replace_13f_period(%s,%s,%s::jsonb)',('SEC Test Fund','2026-06-30',json.dumps([self.holding()])))
        self.assertEqual(self.db.execute("SELECT count(*) FROM institutional_holdings WHERE fund_name='SEC Test Fund'").fetchone()[0],2)

if __name__ == '__main__':unittest.main()
