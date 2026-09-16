"""Real database verification of paid-request budget and private saved screens."""
import os
from pathlib import Path
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

@unittest.skipUnless(os.environ.get('VAIL_TEST_POSTGRES_DSN'),'Requires isolated PostgreSQL')
class ResearchDatabaseTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  import psycopg
  cls.dsn=os.environ['VAIL_TEST_POSTGRES_DSN'];cls.db=psycopg.connect(cls.dsn,autocommit=True)
  cls.db.execute('CREATE SCHEMA IF NOT EXISTS auth; CREATE TABLE IF NOT EXISTS auth.users(id uuid PRIMARY KEY)')
  cls.db.execute((Path(__file__).resolve().parents[1]/'supabase_vail_phase19_research_screens.sql').read_text())
 @classmethod
 def tearDownClass(cls):cls.db.close()
 def setUp(self):
  self.user=uuid.uuid4();self.other=uuid.uuid4()
  self.db.execute('INSERT INTO auth.users(id) VALUES(%s),(%s)',(self.user,self.other))
 def tearDown(self):self.db.execute('DELETE FROM auth.users WHERE id IN (%s,%s)',(self.user,self.other))
 def test_concurrent_requests_stop_at_twenty(self):
  import psycopg
  def claim(_):
   with psycopg.connect(self.dsn) as c:return c.execute('SELECT claim_research_request(%s)',(self.user,)).fetchone()[0]
  with ThreadPoolExecutor(max_workers=6) as pool:results=list(pool.map(claim,range(30)))
  self.assertEqual(sum(results),20)
  self.assertTrue(self.db.execute('SELECT claim_research_request(%s)',(self.other,)).fetchone()[0])
 def test_old_days_do_not_consume_todays_allowance(self):
  self.db.execute("INSERT INTO research_usage VALUES(%s,(now() AT TIME ZONE 'UTC')::date-1,20)",(self.user,))
  self.assertTrue(self.db.execute('SELECT claim_research_request(%s)',(self.user,)).fetchone()[0])
 def test_private_data_and_quota_cannot_be_accessed_by_public_roles(self):
  for role in ['anon','authenticated']:
   for table in ['research_screens','research_usage','company_research_classifications']:
    self.assertFalse(self.db.execute('SELECT has_table_privilege(%s,%s,\'SELECT\')',(role,table)).fetchone()[0])
   self.assertFalse(self.db.execute("SELECT has_function_privilege(%s,'claim_research_request(uuid)','EXECUTE')",(role,)).fetchone()[0])
 def test_saved_screen_limit_is_enforced_in_database(self):
  for i in range(20):self.db.execute("INSERT INTO research_screens(user_id,name,filters) VALUES(%s,%s,'{}')",(self.user,str(i)))
  with self.assertRaisesRegex(Exception,'Saved screen limit'):self.db.execute("INSERT INTO research_screens(user_id,name,filters) VALUES(%s,'extra','{}')",(self.user,))
  self.db.execute("INSERT INTO research_screens(user_id,name,filters) VALUES(%s,'other','{}')",(self.other,))

if __name__=='__main__':unittest.main()
