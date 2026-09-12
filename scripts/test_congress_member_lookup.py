import unittest
from types import SimpleNamespace
from congress_member_lookup import load_congress_members


class LookupTests(unittest.TestCase):
    def client(self, rows, fail_second=False):
        class Query:
            offset = 0
            def table(self, name): return self
            def select(self, columns): return self
            def order(self, column):
                assert column == 'id'
                return self
            def range(self, start, end):
                self.offset, self.end = start, end
                return self
            def execute(self):
                if fail_second and self.offset:
                    raise RuntimeError('lookup unavailable')
                return SimpleNamespace(data=rows[self.offset:self.end+1])
        return Query()

    def test_members_beyond_database_page_limit_are_loaded(self):
        expected = [{'id': str(i)} for i in range(1120)]
        self.assertEqual(load_congress_members(self.client(expected)), expected)

    def test_empty_or_partial_failed_lookup_aborts(self):
        for client in [self.client([]), self.client([{'id': str(i)} for i in range(1120)], True)]:
            with self.assertRaises(RuntimeError):
                load_congress_members(client)


if __name__ == '__main__':
    unittest.main()
