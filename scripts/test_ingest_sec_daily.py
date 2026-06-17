import unittest
from datetime import timedelta

from ingest_sec_daily import load_existing_accessions
from time_utils import congress_today


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(self, rows):
        self.rows = rows
        self.gte_calls = []
        self.range_calls = []

    def select(self, _columns):
        return self

    def gte(self, column, value):
        self.gte_calls.append((column, value))
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self.range_calls.append((start, end))
        self._start = start
        self._end = end
        return self

    def execute(self):
        start = getattr(self, "_start", 0)
        end = getattr(self, "_end", len(self.rows) - 1)
        return _FakeResponse(self.rows[start : end + 1])


class _FakeSupabase:
    def __init__(self, rows):
        self.rows = rows
        self.query = _FakeQuery(rows)

    def table(self, name):
        self.table_name = name
        return self.query


class IngestSecDailyTests(unittest.TestCase):
    def test_load_existing_accessions_limits_to_recent_rows(self) -> None:
        supabase = _FakeSupabase(
            [
                {"source_url": "https://www.sec.gov/Archives/edgar/data/1/0000000001/0000000001-26-000001.txt#tx-0"},
                {"source_url": "https://www.sec.gov/Archives/edgar/data/2/0000000002/0000000002-26-000002.txt#tx-0"},
            ]
        )

        accessions = load_existing_accessions(supabase, days=14, accession_limit=50)

        self.assertEqual(accessions, {"0000000001-26-000001", "0000000002-26-000002"})
        self.assertEqual(supabase.table_name, "insider_trades")
        self.assertEqual(supabase.query.gte_calls[0][0], "created_at")
        self.assertEqual(supabase.query.gte_calls[0][1], (congress_today() - timedelta(days=14)).isoformat())
        self.assertEqual(supabase.query.range_calls[0], (0, 999))


if __name__ == "__main__":
    unittest.main()
