import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ops'))
from audit_congress_roster import parse_roster, load_stored_members
from reconcile_congress_roster import propose_changes

HOUSE = '''<root><member><member-info><bioguideID>A000001</bioguideID><firstname>Alex</firstname><lastname>Smith</lastname><official-name>Alex Smith</official-name><state postal-code="CA"/><party>D</party></member-info></member></root>'''
SENATE = '''<root><senator><bioguideId>B000002</bioguideId><name><first>Blair</first><last>Jones</last></name><state>NY</state><party>I</party></senator></root>'''


class RosterTests(unittest.TestCase):
    def test_both_chambers_have_canonical_fields(self):
        house, count = parse_roster(HOUSE, 'House')
        senate, _ = parse_roster(SENATE, 'Senate')
        self.assertEqual(count, 1)
        self.assertEqual(house['A000001']['party'], 'Democrat')
        self.assertEqual(senate['B000002']['state'], 'NY')
        self.assertTrue(senate['B000002']['active'])

    def test_empty_or_duplicate_rosters_fail(self):
        for content in ['<root/>', HOUSE.replace('</root>', HOUSE[6:])]:
            with self.assertRaises(ValueError):
                parse_roster(content, 'House')

    def test_named_member_without_id_fails(self):
        with self.assertRaises(ValueError):
            parse_roster(HOUSE.replace('A000001', ''), 'House')

    def test_vacancy_is_not_a_person(self):
        content = HOUSE.replace('</root>', '<member><member-info/></member></root>')
        members, entries = parse_roster(content, 'House')
        self.assertEqual(len(members), 1)
        self.assertEqual(entries, 2)

    def test_surname_collision_never_merges_or_deletes(self):
        official, _ = parse_roster(HOUSE, 'House')
        stored = [{'id': 'legacy-smith', 'last_name': 'Smith', 'first_name': 'Alex', 'active': True}]
        proposal = propose_changes(official, stored)
        self.assertEqual(proposal['proposed_additions'][0]['same_surname_ids_for_review'], ['legacy-smith'])
        self.assertEqual(len(proposal['active_status_reviews']), 1)
        self.assertEqual(proposal['automatic_merges'], [])
        self.assertEqual(proposal['automatic_deletions'], [])
        self.assertTrue(stored[0]['active'])

    def test_corrections_have_before_after_and_do_not_mutate(self):
        official, _ = parse_roster(HOUSE, 'House')
        stored = [dict(official['A000001'], active=False)]
        correction = propose_changes(official, stored)['proposed_corrections'][0]
        self.assertEqual(correction['changes'], {'active': {'before': False, 'after': True}})
        self.assertFalse(stored[0]['active'])

    def test_database_inventory_reads_past_default_row_cap(self):
        class ReadOnlyDatabase:
            def __init__(self):
                self.rows = [{'id': str(i)} for i in range(1120)]
            def table(self, _): return self
            def select(self, _): return self
            def order(self, _): return self
            def range(self, start, end):
                self.data = self.rows[start:end + 1]
                return self
            def execute(self): return self
        self.assertEqual(len(load_stored_members(ReadOnlyDatabase())), 1120)


if __name__ == '__main__':
    unittest.main()
