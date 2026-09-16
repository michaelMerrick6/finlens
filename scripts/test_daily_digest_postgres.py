"""Real database invariants for cadence, concurrent workers, privacy and carry-forward."""
import json
import os
from pathlib import Path
import unittest
import uuid

DSN=os.environ.get('VAIL_TEST_POSTGRES_DSN')

@unittest.skipUnless(DSN,'Set VAIL_TEST_POSTGRES_DSN to an isolated test database')
class DailyDigestDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.conn=psycopg.connect(DSN,autocommit=True)
        cls.conn.execute("DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role; END IF; END $$")
        root=Path(__file__).resolve().parents[1]
        for name in ['supabase_schema.sql','supabase_vail_phase1.sql']:
            cls.conn.execute((root/name).read_text())
        # Minimal auth boundary for an isolated PostgreSQL server without Supabase Auth.
        cls.conn.execute('CREATE TABLE IF NOT EXISTS profiles(id uuid PRIMARY KEY,email_enabled boolean NOT NULL DEFAULT false)')
        cls.conn.execute('ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS user_id uuid')
        cls.migration=(root/'supabase_vail_phase17_daily_digests.sql').read_text()
        cls.set_clock('2026-09-17 14:00:00+00')

    @classmethod
    def set_clock(cls,value):
        # Only the test copy replaces the clock; production callers cannot override time.
        cls.conn.execute(cls.migration.replace('BEGIN;','').replace('COMMIT;','').replace('now()',f"TIMESTAMPTZ '{value}'"))

    @classmethod
    def tearDownClass(cls):
        cls.conn.execute(cls.migration)
        cls.conn.close()

    def setUp(self):
        self.conn.execute('BEGIN')
        self.user=str(uuid.uuid4());self.other=str(uuid.uuid4());self.events=[]
        self.conn.execute('INSERT INTO profiles(id,email_enabled) VALUES(%s,true),(%s,true)',(self.user,self.other))
        self.sub=self.subscription(self.user)
        self.queue(self.sub,4)

    def tearDown(self):
        self.conn.execute('ROLLBACK')
        self.conn.execute('DELETE FROM watchlists WHERE user_id IN (%s,%s)',(self.user,self.other))
        self.conn.execute('DELETE FROM profiles WHERE id IN (%s,%s)',(self.user,self.other))
        self.conn.execute('DELETE FROM signal_events WHERE id=ANY(%s::uuid[])',(self.events,))

    def subscription(self,user):
        watch=self.conn.execute("INSERT INTO watchlists(owner_type,owner_key,name,user_id) VALUES('auth_user',%s,%s,%s) RETURNING id",(user,str(uuid.uuid4()),user)).fetchone()[0]
        return self.conn.execute("INSERT INTO alert_subscriptions(watchlist_id,channel,destination,active) VALUES(%s,'email','test@example.invalid',true) RETURNING id",(watch,)).fetchone()[0]

    def queue(self,sub,count,queued='2026-09-16 12:00+00'):
        ids=[]
        for _ in range(count):
            event=self.conn.execute("INSERT INTO signal_events(source,signal_type,source_document_id,ticker,actor_name,actor_type,title,summary,payload) VALUES('test','trade',%s,'TEST','Nancy Pelosi','politician','Trade','Test trade','{}') RETURNING id",(str(uuid.uuid4()),)).fetchone()[0]
            self.events.append(str(event))
            ids.append(self.conn.execute("INSERT INTO alert_deliveries(signal_event_id,subscription_id,delivery_key,channel,destination,queued_at) VALUES(%s,%s,%s,'email','test@example.invalid',%s) RETURNING id",(event,sub,str(uuid.uuid4()),queued)).fetchone()[0])
        return ids

    def claim(self):return self.conn.execute('SELECT claim_daily_email_digest()').fetchone()[0]

    def prepare(self,d):
        items=json.dumps([{'eventId':e['id']} for e in d['events']])
        return self.conn.execute("SELECT prepare_daily_email_digest(%s,'Daily','<p>Full digest</p>','Full digest',%s::jsonb)",(d['id'],items)).fetchone()[0]

    def finish(self,d,status='sent'):self.conn.execute('SELECT finish_daily_email_digest(%s,%s,%s)',(d['id'],status,'provider-test'))

    def test_concurrent_database_worker_cannot_claim_same_account(self):
        import psycopg
        self.conn.execute('COMMIT')
        self.conn.execute('BEGIN')
        first=self.claim()
        with psycopg.connect(DSN) as second:
            second.execute("SET LOCAL statement_timeout='2s'")
            self.assertIsNone(second.execute('SELECT claim_daily_email_digest()').fetchone()[0])
        self.assertIsNotNone(first)

    def test_multiple_watchlists_one_digest_and_new_activity_waits(self):
        self.queue(self.subscription(self.user),2)
        self.queue(self.sub,1,queued='2026-09-17 12:00+00')
        d=self.claim();self.assertEqual(len(d['events']),6)
        self.assertIsNone(self.claim())
        self.assertTrue(self.prepare(d));self.finish(d)
        self.assertIsNone(self.claim())
        self.assertEqual(self.conn.execute("SELECT count(*) FROM alert_deliveries WHERE status='pending'").fetchone()[0],1)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM alert_deliveries WHERE status='digested'").fetchone()[0],6)

    def test_provider_uncertainty_blocks_another_day_too(self):
        d=self.claim();self.prepare(d);self.finish(d,'uncertain')
        self.set_clock('2026-09-18 14:00:00+00')
        self.assertIsNone(self.claim())

    def test_rate_limit_retries_same_digest_and_snapshot(self):
        d=self.claim();self.prepare(d);self.finish(d,'ready')
        again=self.claim();self.assertEqual(again['id'],d['id']);self.assertEqual(again['events'],d['events'])

    def test_disabled_email_cancels_before_provider(self):
        d=self.claim();self.conn.execute('UPDATE profiles SET email_enabled=false WHERE id=%s',(self.user,))
        self.assertFalse(self.prepare(d))
        self.assertEqual(self.conn.execute('SELECT status FROM daily_email_digests WHERE id=%s',(d['id'],)).fetchone()[0],'cancelled')

    def test_changed_destination_cancels_before_provider(self):
        d=self.claim();self.conn.execute("UPDATE alert_subscriptions SET destination='changed@example.invalid' WHERE id=%s",(self.sub,))
        self.assertFalse(self.prepare(d))

    def test_legacy_sender_cannot_bypass_digest_gate(self):
        self.assertEqual(self.conn.execute("UPDATE alert_deliveries SET status='sending' WHERE subscription_id=%s RETURNING id",(self.sub,)).fetchall(),[])

    def test_sent_digest_cannot_be_requeued_and_history_is_private(self):
        d=self.claim();self.prepare(d);self.finish(d)
        self.conn.execute("UPDATE alert_deliveries SET status='pending' WHERE digest_id=%s",(d['id'],))
        self.assertEqual(self.conn.execute("SELECT DISTINCT status FROM alert_deliveries WHERE digest_id=%s",(d['id'],)).fetchone()[0],'digested')
        self.assertEqual(len(self.conn.execute('SELECT * FROM sent_email_history(%s)',(self.user,)).fetchall()),1)
        self.assertEqual(self.conn.execute('SELECT * FROM sent_email_history(%s)',(self.other,)).fetchall(),[])
        self.assertIsNone(self.conn.execute('SELECT sent_email_detail(%s,%s)',(self.other,d['id'])).fetchone()[0])
        self.assertEqual(self.conn.execute('SELECT sent_email_detail(%s,%s)',(self.user,d['id'])).fetchone()[0]['text_body'],'Full digest')

    def test_rolling_24_hours_and_next_day_carry_forward(self):
        d=self.claim();self.prepare(d);self.finish(d)
        self.queue(self.sub,1,queued='2026-09-17 15:00+00')
        self.set_clock('2026-09-18 13:00:00+00');self.assertIsNone(self.claim())
        self.set_clock('2026-09-18 14:00:01+00');self.assertEqual(len(self.claim()['events']),1)

    def test_previously_emailed_trade_is_not_repeated_via_another_follow(self):
        d=self.claim();self.prepare(d);self.finish(d)
        sub=self.subscription(self.user)
        self.conn.execute("INSERT INTO alert_deliveries(signal_event_id,subscription_id,delivery_key,channel,destination,queued_at) VALUES(%s,%s,%s,'email','test@example.invalid','2026-09-17 15:00+00')",(self.events[0],sub,str(uuid.uuid4())))
        self.queue(sub,1,queued='2026-09-17 15:00+00')
        self.set_clock('2026-09-18 14:00:01+00')
        next_digest=self.claim()
        unseen=self.conn.execute('SELECT daily_email_unseen_events(%s,%s::jsonb)',(next_digest['id'],json.dumps(next_digest['events']))).fetchone()[0]
        self.assertEqual(len(next_digest['events']),2)
        self.assertEqual(len(unseen),1)
        self.assertEqual(unseen[0]['id'],self.events[-1])

    def test_already_emailed_only_digest_is_cancelled_without_sending(self):
        d=self.claim()
        self.assertFalse(self.conn.execute("SELECT prepare_daily_email_digest(%s,'Daily','Body','Body','[]')",(d['id'],)).fetchone()[0])
        self.assertEqual(self.conn.execute('SELECT status FROM daily_email_digests WHERE id=%s',(d['id'],)).fetchone()[0],'cancelled')

    def test_no_send_before_eight_eastern(self):
        self.set_clock('2026-09-17 11:59:00+00');self.assertIsNone(self.claim())

    def test_no_truncation_at_postgrest_row_limit(self):
        self.queue(self.sub,1001)
        self.assertEqual(len(self.claim()['events']),1005)

    def test_legacy_sent_email_also_counts_against_daily_cap(self):
        self.conn.execute("UPDATE alert_deliveries SET status='sent',sent_at='2026-09-17 10:00+00' WHERE id=(SELECT id FROM alert_deliveries WHERE subscription_id=%s LIMIT 1)",(self.sub,))
        self.assertIsNone(self.claim())

if __name__=='__main__':unittest.main()
