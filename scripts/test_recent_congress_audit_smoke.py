import importlib
import os
import sys
import unittest
from unittest.mock import patch


def import_with_stub(module_name: str):
    repo_root = os.path.dirname(os.path.dirname(__file__))
    ops_path = os.path.join(repo_root, "ops")
    if ops_path not in sys.path:
        sys.path.insert(0, ops_path)

    for stale_name in (
        module_name,
        "audit_recent_congress_coverage",
        "audit_capitol_trades_gaps",
        "validate_recent_trade_capture",
        "sync_recent_house_filings",
        "ingest_house_official",
        "ingest_senate_official",
    ):
        sys.modules.pop(stale_name, None)

    with patch.dict(
        os.environ,
        {
            "SUPABASE_URL": "https://example.supabase.co",
            "SUPABASE_SERVICE_KEY": "test-service-key",
        },
        clear=False,
    ):
        with patch("supabase.create_client", return_value=object()):
            return importlib.import_module(module_name)


class RecentCongressAuditSmokeTests(unittest.TestCase):
    def test_recent_house_sync_exports_audit_helpers(self) -> None:
        module = import_with_stub("sync_recent_house_filings")

        self.assertTrue(callable(getattr(module, "load_recent_house_filings", None)))
        self.assertTrue(callable(getattr(module, "parse_house_doc", None)))

    def test_recent_congress_audit_module_imports(self) -> None:
        module = import_with_stub("audit_recent_congress_coverage")

        self.assertTrue(callable(getattr(module, "audit_house", None)))
        self.assertTrue(callable(getattr(module, "audit_senate", None)))

    def test_trade_capture_validator_flags_capitol_fallback_rows(self) -> None:
        module = import_with_stub("validate_recent_trade_capture")

        gap = module.summarize_coverage_gap(
            source="house",
            source_doc_id="house-2026-20034207",
            filing_date=module.parse_date("2026-06-11"),
            politician_name="Test Member",
            trades=[
                {"doc_id": "house-2026-20034207-0", "ticker": "NVDA"},
                {"doc_id": "house-2026-20034207-1", "ticker": "AAPL"},
            ],
            existing_rows={},
            prefix_rows=[
                {
                    "doc_id": "house-2026-20034207-capitol-10001",
                    "published_date": "2026-06-11",
                },
                {
                    "doc_id": "house-2026-20034207-capitol-10002",
                    "published_date": "2026-06-11",
                },
            ],
        )

        self.assertIsNotNone(gap)
        self.assertEqual(gap["missing_rows"], 2)
        self.assertEqual(gap["fallback_rows"], 2)
        self.assertEqual(
            gap["fallback_doc_ids"],
            [
                "house-2026-20034207-capitol-10001",
                "house-2026-20034207-capitol-10002",
            ],
        )

    def test_capitol_gap_audit_flags_missing_recent_leads(self) -> None:
        module = import_with_stub("audit_capitol_trades_gaps")

        health = module.assess_capture_health([], days=7)
        self.assertEqual(health["status"], "failed")
        self.assertIn("No Capitol Trades lead rows", health["reason"])

    def test_house_recent_sync_parses_failed_doc_ids(self) -> None:
        module = import_with_stub("sync_recent_house_filings")

        self.assertEqual(module.parse_house_failed_doc_id("2026-9115808"), (2026, "9115808"))
        self.assertIsNone(module.parse_house_failed_doc_id("bad-doc-id"))

    def test_house_recent_sync_extracts_unique_failed_doc_ids(self) -> None:
        module = import_with_stub("sync_recent_house_filings")

        run_rows = [
            {
                "run_metadata": {
                    "failed_doc_ids": ["2026-9115808", "2026-9115809"],
                    "carryover_failed_doc_ids": ["2025-8221287"],
                }
            },
            {
                "run_metadata": {
                    "failed_doc_ids": ["2026-9115808"],
                    "carryover_failed_doc_ids": ["2026-9115815"],
                }
            },
        ]

        self.assertEqual(
            module.extract_failed_house_doc_ids(run_rows, doc_limit=10),
            [
                (2026, "9115808"),
                (2026, "9115809"),
                (2025, "8221287"),
                (2026, "9115815"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
