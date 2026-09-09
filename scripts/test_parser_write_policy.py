import unittest
from unittest.mock import patch

import ingest_house_official as house
import ingest_senate_official as senate
from parser_write_policy import parser_writes_allowed, read_only_parser_scope, read_only_parsing


class ParserWritePolicyTests(unittest.TestCase):
    def test_audit_helpers_never_access_database_for_metadata_writes(self):
        for module, chamber in [(house, 'House'), (senate, 'Senate')]:
            with self.subTest(chamber=chamber), patch.object(module, 'supabase') as database:
                database.table.side_effect = AssertionError('Unexpected database access')
                members = []
                with read_only_parser_scope():
                    result = module.resolve_member_id('Unmatched', 'Example', members, target_chamber=chamber)
                    module.upsert_company('TEST', 'Test Corporation')
                self.assertTrue(result.startswith('unknown-'))
                self.assertEqual(members, [])
                database.table.assert_not_called()

    def test_nested_context_and_exception_restore_ingestion_policy(self):
        self.assertTrue(parser_writes_allowed())
        with self.assertRaises(RuntimeError):
            with read_only_parser_scope():
                with read_only_parser_scope():
                    self.assertFalse(parser_writes_allowed())
                self.assertFalse(parser_writes_allowed())
                raise RuntimeError('Parse failed')
        self.assertTrue(parser_writes_allowed())

    def test_decorator_preserves_return_and_restores_policy(self):
        @read_only_parsing
        def audit():
            self.assertFalse(parser_writes_allowed())
            return 'result'
        self.assertEqual(audit(), 'result')
        self.assertTrue(parser_writes_allowed())


if __name__ == '__main__':
    unittest.main()
