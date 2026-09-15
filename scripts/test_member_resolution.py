import unittest
from unittest.mock import patch
import ingest_house_official as house
import ingest_senate_official as senate
from parser_write_policy import read_only_parser_scope


class MemberResolutionTests(unittest.TestCase):
    def resolve(self, first, rows):
        results = []
        for module, chamber in [(house, 'House'), (senate, 'Senate')]:
            with read_only_parser_scope(), patch.object(module, 'supabase') as db:
                result = module.resolve_member_id(first, 'Smith', [dict(row, chamber=chamber) for row in rows], target_chamber=chamber)
                db.table.assert_not_called()
                results.append(result)
        return results

    def test_conflicting_first_name_never_uses_unique_surname(self):
        for result in self.resolve('Alan', [{'id': 'A000001', 'first_name': 'Robert', 'last_name': 'Smith', 'active': True}]):
            self.assertTrue(result.startswith('unknown-'))

    def test_ambiguous_initial_does_not_depend_on_row_order(self):
        rows = [{'id': 'A000001', 'first_name': 'Adam', 'last_name': 'Smith', 'active': True},
                {'id': 'A000002', 'first_name': 'Alex', 'last_name': 'Smith', 'active': False}]
        for ordered in [rows, list(reversed(rows))]:
            for result in self.resolve('A', ordered):
                self.assertTrue(result.startswith('unknown-'))

    def test_matching_former_member_is_not_replaced_by_active_namesake(self):
        rows = [{'id': 'A000001', 'first_name': 'Alex', 'last_name': 'Smith', 'active': False},
                {'id': 'A000002', 'first_name': 'Robert', 'last_name': 'Smith', 'active': True}]
        self.assertEqual(self.resolve('Alex', rows), ['A000001', 'A000001'])

    def test_disclosure_name_variants_resolve_known_members(self):
        cases = [('Rob', 'Robert'), ('Timothy P', 'Tim'), ('Rohit', 'Ro')]
        for filed, canonical in cases:
            self.assertEqual(self.resolve(filed, [dict(id='A000001', first_name=canonical, last_name='Smith')]),
                             ['A000001', 'A000001'])

    def test_full_senate_name_retains_ambiguity(self):
        rows = [dict(id='A000001', first_name='Tim', last_name='Smith', chamber='Senate'),
                dict(id='A000002', first_name='Timothy', last_name='Smith', chamber='Senate')]
        for ordered in (rows, list(reversed(rows))):
            with read_only_parser_scope():
                self.assertTrue(senate.resolve_member_id_from_full_name('Timothy Smith', ordered).startswith('unknown-'))

    def test_missing_first_name_stays_unresolved(self):
        for result in self.resolve('', [{'id': 'A000001', 'first_name': 'Alex', 'last_name': 'Smith'}]):
            self.assertTrue(result.startswith('unknown-'))


if __name__ == '__main__':
    unittest.main()
