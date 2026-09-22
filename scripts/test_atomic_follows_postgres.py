"""Verify account follow quotas with real concurrent PostgreSQL transactions."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from threading import Barrier
import unittest
import uuid

DSN = os.environ.get('VAIL_TEST_POSTGRES_DSN')


@unittest.skipUnless(DSN, 'Requires isolated PostgreSQL')
class AtomicFollowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.db = psycopg.connect(DSN, autocommit=True)
        cls.db.execute("""DO $$ BEGIN
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF;
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated; END IF;
            IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role; END IF;
        END $$""")
        cls.db.execute("""CREATE SCHEMA IF NOT EXISTS auth;
            CREATE OR REPLACE FUNCTION auth.role() RETURNS text LANGUAGE sql STABLE AS
            $$ SELECT nullif(current_setting('request.jwt.claim.role', true), '') $$;
        """)
        root = Path(__file__).resolve().parents[1]
        for name in ['supabase_schema.sql', 'supabase_vail_phase1.sql',
                     'supabase_vail_phase2_notifications.sql', 'supabase_vail_phase4_follow_modes.sql']:
            cls.db.execute((root / name).read_text())
        # Match the account columns without requiring a running Supabase Auth service.
        cls.db.execute("""CREATE TABLE IF NOT EXISTS profiles (
            id uuid PRIMARY KEY, email_enabled boolean NOT NULL DEFAULT false
        );
        ALTER TABLE profiles ADD COLUMN IF NOT EXISTS follow_limit integer NOT NULL DEFAULT 3;
        ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS user_id uuid;
        """)
        cls.migration = (root / 'supabase_vail_phase20_atomic_follows.sql').read_text()
        cls.db.execute(cls.migration)
        cls.db.execute("SELECT set_config('request.jwt.claim.role', 'service_role', false)")

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def setUp(self):
        self.user = uuid.uuid4()
        self.other = uuid.uuid4()
        self.db.execute('INSERT INTO profiles(id,follow_limit) VALUES(%s,3),(%s,3)', (self.user, self.other))
        self.watchlist = self.db.execute("""INSERT INTO watchlists(owner_type,owner_key,name,user_id)
            VALUES('auth_user',%s,'Test',%s) RETURNING id""", (str(self.user), self.user)).fetchone()[0]
        self.save(self.db, 'ticker', 'AAPL')
        self.save(self.db, 'actor', 'p000197')

    def tearDown(self):
        self.db.execute('DELETE FROM watchlists WHERE id=%s', (self.watchlist,))
        self.db.execute('DELETE FROM profiles WHERE id IN (%s,%s)', (self.user, self.other))

    def save(self, db, kind, target, mode='activity', user=None):
        return db.execute('SELECT save_account_follow(%s,%s,%s,%s,%s,%s,%s,%s::jsonb)', (
            user or self.user, self.watchlist, kind, target, mode,
            'politician' if kind == 'actor' else None,
            'Test politician' if kind == 'actor' else None, '{"source":"test"}',
        ))

    def count(self):
        return self.db.execute('''SELECT
            (SELECT count(*) FROM watchlist_tickers WHERE watchlist_id=%s) +
            (SELECT count(*) FROM watchlist_actors WHERE watchlist_id=%s)''',
            (self.watchlist, self.watchlist)).fetchone()[0]

    def concurrent(self, follows):
        import psycopg
        barrier = Barrier(len(follows))

        def attempt(follow):
            try:
                with psycopg.connect(DSN) as db:
                    db.execute("SELECT set_config('request.jwt.claim.role', 'service_role', true)")
                    db.execute("SET LOCAL statement_timeout = '5s'")
                    barrier.wait(timeout=5)
                    self.save(db, *follow)
                return True
            except psycopg.errors.RaiseException as error:
                if str(error).splitlines()[0] != 'FOLLOW_LIMIT_REACHED':
                    raise
                return False

        with ThreadPoolExecutor(max_workers=len(follows)) as pool:
            return list(pool.map(attempt, follows))

    def test_concurrent_ticker_and_actor_share_one_remaining_slot(self):
        results = self.concurrent([('ticker', 'MSFT'), ('actor', 'k000389')])
        self.assertEqual(sum(results), 1)
        self.assertEqual(self.count(), 3)

    def test_concurrent_ticker_additions_cannot_exceed_the_limit(self):
        self.assertEqual(sum(self.concurrent([('ticker', 'MSFT'), ('ticker', 'NVDA')])), 1)
        self.assertEqual(self.count(), 3)

    def test_concurrent_actor_additions_cannot_exceed_the_limit(self):
        self.assertEqual(sum(self.concurrent([('actor', 'k000389'), ('actor', 'm001157')])), 1)
        self.assertEqual(self.count(), 3)

    def test_repeated_same_follow_is_idempotent_at_the_limit(self):
        self.assertTrue(all(self.concurrent([('ticker', 'MSFT'), ('ticker', 'MSFT')])))
        self.assertEqual(self.count(), 3)

    def test_existing_follows_can_change_mode_after_a_downgrade(self):
        self.db.execute('UPDATE profiles SET follow_limit=1 WHERE id=%s', (self.user,))
        self.save(self.db, 'ticker', 'AAPL', 'unusual')
        self.save(self.db, 'actor', 'p000197', 'both')
        self.assertEqual(self.count(), 2)
        self.assertEqual(self.db.execute('SELECT alert_mode FROM watchlist_tickers WHERE watchlist_id=%s',
                                        (self.watchlist,)).fetchone()[0], 'unusual')
        self.assertEqual(self.db.execute('SELECT alert_mode FROM watchlist_actors WHERE watchlist_id=%s',
                                        (self.watchlist,)).fetchone()[0], 'both')

    def test_another_accounts_watchlist_is_rejected(self):
        with self.assertRaisesRegex(Exception, 'does not own'):
            self.save(self.db, 'ticker', 'MSFT', user=self.other)
        self.assertEqual(self.count(), 2)

    def test_rollback_releases_the_slot_and_migration_preserves_follows(self):
        with self.assertRaisesRegex(RuntimeError, 'rollback'):
            with self.db.transaction():
                self.save(self.db, 'ticker', 'MSFT')
                raise RuntimeError('rollback')
        self.save(self.db, 'ticker', 'NVDA')
        self.db.execute(self.migration)
        self.assertEqual(self.count(), 3)

    def test_only_service_role_can_execute_the_writer(self):
        signature = 'save_account_follow(uuid,uuid,text,text,text,text,text,jsonb)'
        for role in ['anon', 'authenticated']:
            self.assertFalse(self.db.execute('SELECT has_function_privilege(%s,%s,\'EXECUTE\')',
                                             (role, signature)).fetchone()[0])
        self.db.execute("SELECT set_config('request.jwt.claim.role', 'authenticated', false)")
        try:
            with self.assertRaisesRegex(Exception, 'requires the service role'):
                self.save(self.db, 'ticker', 'MSFT')
        finally:
            self.db.execute("SELECT set_config('request.jwt.claim.role', 'service_role', false)")
        self.assertEqual(self.count(), 2)


if __name__ == '__main__':
    unittest.main()
