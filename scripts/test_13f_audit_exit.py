"""Coverage failures must fail the scheduled audit, not merely print a summary."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('audit_13f', Path(__file__).resolve().parents[1] / 'ops/audit_recent_13f_coverage.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditExitTests(unittest.TestCase):
    def run_audit(self, parsed, stored):
        with patch.object(audit, 'get_supabase_client'), \
             patch.object(audit, 'create_13f_session'), \
             patch.object(audit, 'load_company_reference', return_value=[]), \
             patch.object(audit, 'SecTickerResolver'), \
             patch.object(audit, 'TRACKED_13F_FUNDS', [{'name': 'Example Fund'}]), \
             patch.object(audit, 'load_available_13f_filings', return_value=[{}]), \
             patch.object(audit, 'parse_13f_filing', return_value=parsed), \
             patch.object(audit, 'load_db_holdings', return_value=stored), \
             patch.object(audit, 'emit_summary') as summary:
            audit.main()
            return summary.call_args.args[0]

    def test_parse_failure_exits_nonzero(self):
        with self.assertRaises(SystemExit) as exc:
            self.run_audit(None, [])
        self.assertEqual(exc.exception.code, 1)

    def test_missing_holdings_exit_nonzero(self):
        with self.assertRaises(SystemExit) as exc:
            self.run_audit(self.parsed(), [])
        self.assertEqual(exc.exception.code, 1)

    def test_matching_holdings_pass(self):
        parsed = self.parsed()
        self.assertEqual(self.run_audit(parsed, parsed['holdings'])['coverage_mismatches'], 0)

    @staticmethod
    def parsed():
        return {'rows_supported': 1, 'rows_resolved': 1, 'rows_unresolved': 0,
                'report_period': '2026-06-30', 'published_date': '2026-08-01',
                'holdings': [{'ticker': 'AAPL', 'shares_held': 10, 'value_held': 2000}]}


if __name__ == '__main__':
    unittest.main()
