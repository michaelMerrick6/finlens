"""Actual PostgreSQL rollback/claim/correction tests; CI provides an isolated DB."""
import json
import os
from pathlib import Path
import unittest

DSN = os.environ.get('VAIL_TEST_POSTGRES_DSN')


@unittest.skipUnless(DSN, 'Set VAIL_TEST_POSTGRES_DSN to an isolated test database')
class CongressPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.conn = psycopg.connect(DSN, autocommit=True)
        cls.conn.execute("""DO $$ BEGIN
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF;
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated; END IF;
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role; END IF;
        END $$""")
        cls.conn.execute("SET search_path TO public, extensions")
        root = Path(__file__).resolve().parents[1]
        for name in ['supabase_schema.sql', 'supabase_vail_phase1.sql', 'supabase_vail_phase14_congress_capture.sql', 'supabase_vail_phase15_congress_backfill.sql']:
            cls.conn.execute((root/name).read_text())
        # Supabase installs uuid-ossp outside public; security-definer RPCs must not depend on its search path.
        cls.conn.execute('CREATE SCHEMA IF NOT EXISTS extensions')
        cls.conn.execute('ALTER EXTENSION "uuid-ossp" SET SCHEMA extensions')

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.conn.execute('BEGIN')
        self.conn.execute("INSERT INTO congress_members(id,first_name,last_name) VALUES('TEST','Test','Member')")
        self.conn.execute("INSERT INTO companies(ticker,name) VALUES('TST','Test Company')")
        self.register('house-2026-123')

    def tearDown(self):
        self.conn.execute('ROLLBACK')

    def register(self, key):
        self.conn.execute('SELECT register_congress_filings(%s::jsonb)', (json.dumps([
            {'filing_id':key, 'chamber':'House', 'published_date':'2026-09-01', 'filing':{'doc_id':key}}]),))

    def rows(self, count, key='house-2026-123'):
        return [dict(doc_id=f'{key}-{i}',member_id='TEST',politician_name='Test Member',chamber='House',
            party='Unknown',ticker='TST',asset_name='Test Company',transaction_date='2026-08-25',
            published_date='2026-09-01',transaction_type='buy',asset_type='Stock',
            amount_range='$1,001 - $15,000',source_url='https://example.com/filing') for i in range(count)]

    def publish(self, rows, *, token=None, key='house-2026-123', empty=False):
        from emit_signal_events import build_politician_events
        raw, events = build_politician_events(rows)
        return self.conn.execute('SELECT publish_congress_filing(%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s,%s,%s::jsonb,%s)',
            (key,json.dumps(rows),json.dumps(raw),json.dumps(events),token,'test',None,'{}',empty)).fetchone()[0]

    def count(self, table):
        return self.conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    def test_backfill_claim_excludes_complete_failed_and_live_leases(self):
        for key in ['house-2026-failed','house-2026-live','house-2026-expired','house-2026-complete']:
            self.register(key)
        self.conn.execute("UPDATE congress_filings SET status='failed' WHERE filing_id='house-2026-failed'")
        self.conn.execute("UPDATE congress_filings SET status='complete' WHERE filing_id='house-2026-complete'")
        self.conn.execute("UPDATE congress_filings SET status='processing',lease_until=now()+interval '1 minute' WHERE filing_id='house-2026-live'")
        self.conn.execute("UPDATE congress_filings SET status='processing',lease_until=now()-interval '1 minute' WHERE filing_id='house-2026-expired'")
        claimed = [self.conn.execute("SELECT claim_congress_backfill('House')").fetchone()[0] for _ in range(3)]
        self.assertEqual({r['filing_id'] for r in claimed if r}, {'house-2026-123','house-2026-expired'})
        self.assertIsNone(claimed[-1])
        self.assertTrue(all(r['claim_token'] and r['status']=='processing' for r in claimed if r))

    def test_failed_replacement_preserves_all_original_tables(self):
        self.publish(self.rows(2))
        baseline = {t:self.count(t) for t in ['politician_trades','signal_events','raw_filings','congress_corrections']}
        invalid = self.rows(1)
        invalid[0]['member_id'] = 'DOES-NOT-EXIST'
        with self.assertRaises(Exception):
            with self.conn.transaction():
                self.publish(invalid)
        self.assertEqual(baseline, {t:self.count(t) for t in baseline})
        self.assertEqual(self.conn.execute('SELECT DISTINCT member_id FROM politician_trades').fetchone()[0], 'TEST')

    def test_correction_removes_obsolete_signal_and_archives_dependents(self):
        self.publish(self.rows(2))
        event_id = self.conn.execute("SELECT id FROM signal_events WHERE source_document_id='house-2026-123-1'").fetchone()[0]
        self.conn.execute("INSERT INTO signal_events(source,signal_type,source_document_id,ticker,actor_name,actor_type,title,summary,payload) VALUES('derived','politician_cluster','cluster-test','TST','Test','cluster','Old cluster','Old cluster summary',%s::jsonb)",
            (json.dumps({'cluster_event_ids':[str(event_id)]}),))
        self.conn.execute("INSERT INTO alert_deliveries(signal_event_id,delivery_key,channel,status) VALUES(%s,'sent-test','email','sent')",(event_id,))
        self.publish(self.rows(1))
        self.assertEqual(self.count('politician_trades'),1)
        self.assertEqual(self.count('signal_events'),1)
        self.assertEqual(self.count('raw_filings'),1)
        history = self.conn.execute("SELECT snapshot FROM congress_corrections ORDER BY corrected_at DESC,id DESC").fetchall()
        self.assertTrue(any(row[0].get('deliveries') for row in history))

    def test_stale_emitter_cannot_resurrect_deleted_trade(self):
        from emit_signal_events import build_politician_events
        old = self.rows(2)
        self.publish(old)
        self.publish(self.rows(1))
        _, events = build_politician_events(old)
        self.conn.execute("INSERT INTO signal_events(source,signal_type,source_document_id,ticker,actor_name,actor_type,title,payload) VALUES('congress','politician_trade','house-2026-123-1','TST','Test Member','politician','Stale',%s::jsonb)",(json.dumps(events[1]['payload']),))
        self.assertEqual(self.count('signal_events'),1)

    def test_replacement_does_not_match_another_filing_prefix(self):
        self.register('house-2026-1234')
        self.publish(self.rows(1,key='house-2026-1234'),key='house-2026-1234')
        self.publish(self.rows(1))
        self.publish([],empty=True)
        self.assertEqual(self.conn.execute('SELECT doc_id FROM politician_trades').fetchone()[0],'house-2026-1234-0')

    def test_prefix_replacement_under_linguistic_collation(self):
        collation = self.conn.execute("SELECT collname FROM pg_collation WHERE collname='en-x-icu'").fetchone()
        if not collation:
            self.skipTest('ICU English collation unavailable')
        for table,column in [('politician_trades','doc_id'),('raw_filings','source_document_id'),('signal_events','source_document_id')]:
            self.conn.execute(f'ALTER TABLE {table} ALTER COLUMN {column} TYPE text COLLATE "en-x-icu"')
        self.publish(self.rows(2))
        self.publish(self.rows(1))
        self.assertEqual(self.count('politician_trades'),1)
        self.assertEqual(self.count('raw_filings'),1)
        self.assertEqual(self.count('signal_events'),1)

    def test_legacy_uppercase_signal_is_replaced_without_duplicate(self):
        self.conn.execute("INSERT INTO politician_trades(member_id,politician_name,chamber,ticker,transaction_date,published_date,transaction_type,amount_range,doc_id) VALUES('TEST','Test Member','House','TST','2026-08-25','2026-09-01','buy','$1,001 - $15,000','HOUSE-2026-999-0')")
        self.conn.execute("INSERT INTO signal_events(source,signal_type,source_document_id,ticker,actor_name,actor_type,title,summary) VALUES('congress','politician_trade','HOUSE-2026-999-0','TST','Test Member','politician','Old','Old')")
        self.register('house-2026-999')
        self.publish(self.rows(1,key='house-2026-999'),key='house-2026-999')
        self.assertEqual(self.count('signal_events'),1)
        self.assertEqual(self.conn.execute('SELECT source_document_id FROM signal_events').fetchone()[0],'house-2026-999-0')

    def test_empty_extraction_requires_explicit_verification(self):
        self.publish(self.rows(1))
        with self.assertRaises(Exception):
            with self.conn.transaction():
                self.publish([])
        self.assertEqual(self.count('politician_trades'),1)

    def test_null_rpc_payload_cannot_erase_existing_filing(self):
        self.publish(self.rows(1))
        with self.assertRaises(Exception):
            with self.conn.transaction():
                self.conn.execute("SELECT publish_congress_filing('house-2026-123',NULL,NULL,NULL,NULL,'test',NULL,'{}',false)")
        self.assertEqual(self.count('politician_trades'),1)

    def test_unchanged_reparse_preserves_ids_and_does_not_invalidate_signals(self):
        self.publish(self.rows(1))
        trade_id = self.conn.execute('SELECT id FROM politician_trades').fetchone()[0]
        event_id = self.conn.execute('SELECT id FROM signal_events').fetchone()[0]
        archives = self.count('congress_corrections')
        self.publish(self.rows(1))
        self.assertEqual(self.conn.execute('SELECT id FROM politician_trades').fetchone()[0],trade_id)
        self.assertEqual(self.conn.execute('SELECT id FROM signal_events').fetchone()[0],event_id)
        self.assertEqual(self.count('congress_corrections'),archives)

    def test_correction_does_not_requeue_sent_delivery_key(self):
        self.publish(self.rows(2))
        event_id = self.conn.execute("SELECT id FROM signal_events WHERE source_document_id='house-2026-123-1'").fetchone()[0]
        self.conn.execute("INSERT INTO alert_deliveries(signal_event_id,delivery_key,channel,status) VALUES(%s,'already-sent','email','sent')",(event_id,))
        self.publish(self.rows(1))
        new_id = self.conn.execute('SELECT id FROM signal_events').fetchone()[0]
        self.conn.execute("INSERT INTO alert_deliveries(signal_event_id,delivery_key,channel,status) VALUES(%s,'already-sent','email','pending')",(new_id,))
        self.assertEqual(self.count('alert_deliveries'),0)

    def test_failed_filing_stays_in_queue_when_rediscovered(self):
        claim = self.conn.execute("SELECT claim_congress_filing('House',true)").fetchone()[0]
        self.conn.execute("SELECT fail_congress_filing('house-2026-123',%s,'Unreadable scan')",(claim['claim_token'],))
        self.register('house-2026-123')
        row = self.conn.execute('SELECT status,last_error,attempts FROM congress_filings').fetchone()
        self.assertEqual(row,('failed','Unreadable scan',1))

    def test_claims_survive_rediscovery_and_reject_stale_worker(self):
        first = self.conn.execute("SELECT claim_congress_filing('House',true)").fetchone()[0]
        self.register('house-2026-123')
        self.assertIsNone(self.conn.execute("SELECT claim_congress_filing('House',true)").fetchone()[0])
        self.conn.execute("UPDATE congress_filings SET lease_until=now()-interval '1 second'")
        second = self.conn.execute("SELECT claim_congress_filing('House',true)").fetchone()[0]
        with self.assertRaises(Exception):
            with self.conn.transaction():
                self.publish(self.rows(1),token=first['claim_token'])
        self.publish(self.rows(1),token=second['claim_token'])
        self.assertEqual(self.conn.execute('SELECT status FROM congress_filings').fetchone()[0],'complete')

    def test_public_roles_cannot_call_mutation_rpcs(self):
        self.assertFalse(self.conn.execute("SELECT has_function_privilege('anon','register_congress_filings(jsonb)','EXECUTE')").fetchone()[0])
        self.assertFalse(self.conn.execute("SELECT has_function_privilege('authenticated','publish_congress_filing(text,jsonb,jsonb,jsonb,uuid,text,text,jsonb,boolean)','EXECUTE')").fetchone()[0])


if __name__ == '__main__':
    unittest.main()
