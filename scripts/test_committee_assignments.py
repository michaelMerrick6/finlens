"""Offline contract and database safety tests; never fetch live sources."""
import copy
from datetime import datetime, timezone
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid
from sync_committee_assignments import parse_house, parse_senate, validate, snapshot, congress_on, run_source


def payload(count=20, source='official_house'):
    return {'source_key':source,'source_version':str(uuid.uuid4()),'evidence_kind':'official_current',
            'observed_at':datetime.now(timezone.utc).isoformat(),'congress':119,
            'committees':[{'id':'a','name':'Example','parent_id':None,'source_url':'https://clerk.house.gov'}],
            'members':[{'id':f'A{i:06}','chamber':'House'} for i in range(count)],
            'assignments':[{'committee_id':'a','member_id':f'A{i:06}','chamber':'House','role':'Member'} for i in range(count)],
            'documents':[{'url':'https://clerk.house.gov','content':'test evidence'}]}


class ParsingTests(unittest.TestCase):
    def test_duplicate_identity_fails(self):
        p=payload();p['assignments'].append(p['assignments'][0])
        with self.assertRaisesRegex(ValueError,'Duplicate'): validate(p)

    def test_unknown_member_fails(self):
        p=payload();p['members']=[]
        with self.assertRaisesRegex(ValueError,'unresolved'): validate(p)

    def test_missing_committee_fails(self):
        p=payload();p['committees']=[]
        with self.assertRaises(ValueError): validate(p)

    def test_truncated_current_roster_fails(self):
        with self.assertRaisesRegex(ValueError,'Incomplete'): validate(payload())

    def test_archive_empty_is_preserved_as_gap(self):
        p=payload();p.update(evidence_kind='archive_snapshot',assignments=[],members=[])
        validate(p)

    def test_congress_boundary(self):
        self.assertEqual(congress_on(datetime(2025,1,2)),118)
        self.assertEqual(congress_on(datetime(2025,1,3)),119)

    def test_house_empty_placeholders_subcommittees_and_roles(self):
        members=''.join(f'<member><member-info><bioguideID>A{i:06}</bioguideID><official-name>Member {i}</official-name></member-info><committee-assignments><committee rank=""/><committee comcode="AS00" leadership="Chair"/><subcommittee subcomcode="AS01"/></committee-assignments></member>' for i in range(435))
        body=f'<MemberData><title-info><congress-num>119</congress-num></title-info><members>{members}</members><committees><committee comcode="AS00"><committee-fullname>Armed Services</committee-fullname><subcommittee subcomcode="AS01"><subcommittee-fullname>Readiness</subcommittee-fullname></subcommittee></committee></committees></MemberData>'
        with patch('sync_committee_assignments.validate'), patch('sync_committee_assignments.congress_on',return_value=119):
            p=parse_house(body)
        self.assertEqual(len(p['assignments']),870)
        self.assertEqual(p['assignments'][0]['role'],'Chair')
        self.assertEqual(p['committees'][1]['parent_id'],'house:AS00')
        self.assertNotIn('effective_from',p['assignments'][0])

    def test_senate_unmatched_identity_fails(self):
        roster='<root>'+''.join(f'<senator><name><last>Name{i}</last></name><state>TX</state><bioguideId>A{i:06}</bioguideId></senator>' for i in range(100))+'</root>'
        codes=['SS'+chr(65+i)+'A' for i in range(20)]
        index=' '.join(f'committee_memberships_{code}.htm' for code in codes)
        sources={c:f'<committee_membership><committees><committee_code>{c}00</committee_code><committee_name>Committee</committee_name><members><member><name><last>Unknown</last></name><state>TX</state></member></members></committees></committee_membership>' for c in codes}
        with self.assertRaisesRegex(ValueError,'Unresolved Senate'): parse_senate(roster,index,sources)


@unittest.skipUnless(os.environ.get('VAIL_TEST_POSTGRES_DSN'),'Requires isolated PostgreSQL')
class DatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.db=psycopg.connect(os.environ['VAIL_TEST_POSTGRES_DSN'],autocommit=True)
        cls.db.execute("DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated; END IF; IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='service_role') THEN CREATE ROLE service_role; END IF; END $$")
        cls.db.execute((Path(__file__).resolve().parents[1]/'supabase_schema.sql').read_text())
        cls.db.execute((Path(__file__).resolve().parents[1]/'supabase_vail_phase18_committee_assignments.sql').read_text())

    @classmethod
    def tearDownClass(cls): cls.db.close()

    def setUp(self):
        self.key='test_'+str(uuid.uuid4())
        self.p=payload(source=self.key)

    def tearDown(self): self.db.execute('DELETE FROM committee_snapshots WHERE source_key=%s',(self.key,))

    def publish(self,p):
        from psycopg.types.json import Jsonb
        return self.db.execute('SELECT publish_committee_snapshot(%s)',(Jsonb(p),)).fetchone()[0]

    def test_idempotent_publication(self):
        a=self.publish(self.p);b=self.publish(self.p)
        self.assertEqual(a,b)
        self.assertEqual(self.db.execute('SELECT count(*) FROM committee_assignments WHERE snapshot_id=%s',(a,)).fetchone()[0],20)

    def test_mass_removal_preserves_previous(self):
        a=self.publish(self.p);p=copy.deepcopy(self.p);p['source_version']='removed';p['assignments']=p['assignments'][:2]
        with self.assertRaisesRegex(Exception,'Large roster'): self.publish(p)
        self.assertEqual(self.db.execute('SELECT count(*) FROM committee_snapshots WHERE source_key=%s',(self.key,)).fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM committee_assignments WHERE snapshot_id=%s',(a,)).fetchone()[0],20)

    def test_empty_current_rejected(self):
        self.p['assignments']=[]
        with self.assertRaisesRegex(Exception,'Empty'): self.publish(self.p)

    def test_one_removal_preserves_history(self):
        a=self.publish(self.p);p=copy.deepcopy(self.p);p['source_version']='updated';p['assignments'].pop()
        b=self.publish(p)
        self.assertNotEqual(a,b)
        self.assertEqual(self.db.execute('SELECT count(*) FROM committee_assignments WHERE snapshot_id=%s',(a,)).fetchone()[0],20)
        self.assertEqual(self.db.execute('SELECT count(*) FROM committee_assignments WHERE snapshot_id=%s',(b,)).fetchone()[0],19)

    def test_congress_transition_requires_review(self):
        self.publish(self.p);p=copy.deepcopy(self.p);p['congress']=120;p['source_version']='new'
        with self.assertRaisesRegex(Exception,'Congress transition'): self.publish(p)

    def test_archive_never_replaces_current(self):
        a=self.publish(self.p);p=copy.deepcopy(self.p);p.update(source_key=self.key+'_archive',source_version='archive',evidence_kind='archive_snapshot',assignments=[],members=[])
        try:
            self.publish(p)
            self.assertEqual(self.db.execute('SELECT count(*) FROM committee_assignments WHERE snapshot_id=%s',(a,)).fetchone()[0],20)
        finally: self.db.execute('DELETE FROM committee_snapshots WHERE source_key=%s',(p['source_key'],))

    def test_untrusted_roles_cannot_publish(self):
        for role in ['anon','authenticated']:
            self.assertFalse(self.db.execute("SELECT has_function_privilege(%s,'publish_committee_snapshot(jsonb)','EXECUTE')",(role,)).fetchone()[0])
            self.assertFalse(self.db.execute("SELECT has_table_privilege(%s,'committee_snapshots','SELECT')",(role,)).fetchone()[0])

if __name__=='__main__': unittest.main()
