import unittest
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from scripts.ingest_senate_official import (
    parse_senate_paper_lines,
    prepare_senate_trades_for_insert,
    resolve_company_ticker,
)


class SenateParserRegressionTests(unittest.TestCase):
    def test_private_entity_names_do_not_become_false_tickers(self) -> None:
        valid_tickers = {"MH", "AWAY", "IBM", "N/A"}

        self.assertIsNone(resolve_company_ticker("MH Built to Last LLC", valid_tickers))
        self.assertIsNone(resolve_company_ticker("Not Fade Away LLC", valid_tickers))
        self.assertEqual(resolve_company_ticker("IBM Corp. (stock)", valid_tickers), "IBM")

    def test_paper_lines_keep_unmapped_rows_as_na(self) -> None:
        line_tokens = [
            [
                {"page": 0, "top": 100, "left": 120, "text": "(S)"},
                {"page": 0, "top": 100, "left": 180, "text": "MH"},
                {"page": 0, "top": 100, "left": 260, "text": "Built"},
                {"page": 0, "top": 100, "left": 340, "text": "to"},
                {"page": 0, "top": 100, "left": 390, "text": "Last"},
                {"page": 0, "top": 100, "left": 470, "text": "LLC"},
                {"page": 0, "top": 100, "left": 1500, "text": "x"},
                {"page": 0, "top": 100, "left": 1700, "text": "2/5/26"},
                {"page": 0, "top": 100, "left": 2100, "text": "x"},
            ],
            [
                {"page": 0, "top": 180, "left": 120, "text": "(S)"},
                {"page": 0, "top": 180, "left": 180, "text": "Not"},
                {"page": 0, "top": 180, "left": 260, "text": "Fade"},
                {"page": 0, "top": 180, "left": 340, "text": "Away"},
                {"page": 0, "top": 180, "left": 430, "text": "LLC"},
                {"page": 0, "top": 180, "left": 1500, "text": "x"},
                {"page": 0, "top": 180, "left": 1700, "text": "2/9/26"},
                {"page": 0, "top": 180, "left": 2100, "text": "x"},
            ],
        ]

        trades = parse_senate_paper_lines(
            line_tokens,
            doc_key="paper-doc",
            member_id="B001277",
            first_name="Richard",
            last_name="Blumenthal",
            filed_date="2026-03-13",
            source_url="https://example.com/paper",
            valid_tickers={"MH", "AWAY", "IBM", "N/A"},
        )

        self.assertEqual(len(trades), 2)
        self.assertTrue(all(trade["ticker"] == "N/A" for trade in trades))
        self.assertEqual([trade["transaction_date"] for trade in trades], ["2026-02-05", "2026-02-09"])

    def test_prepare_senate_trades_strips_private_helper_fields(self) -> None:
        trades = [
            {
                "member_id": "C001047",
                "politician_name": "Shelley M Capito",
                "chamber": "Senate",
                "party": "Unknown",
                "ticker": "PNC",
                "transaction_date": "2026-03-12",
                "published_date": "2026-04-04",
                "transaction_type": "buy",
                "asset_type": "Stock",
                "amount_range": "$1,001 - $15,000",
                "source_url": "https://example.com/filing",
                "doc_id": "senate-ab712d8d-1afd-4f09-b29e-f1b65bba50b8-0",
                "_asset_name": "PNC Financial Services Group, Inc. (The) Common Stock",
                "_debug_only": "ignore-me",
            }
        ]

        with patch("scripts.ingest_senate_official.politician_trades_has_asset_name_column", return_value=False):
            prepared = prepare_senate_trades_for_insert(trades)
            self.assertEqual(len(prepared), 1)
            self.assertNotIn("_asset_name", prepared[0])
            self.assertNotIn("_debug_only", prepared[0])
            self.assertNotIn("asset_name", prepared[0])

        with patch("scripts.ingest_senate_official.politician_trades_has_asset_name_column", return_value=True):
            prepared = prepare_senate_trades_for_insert(trades)
            self.assertEqual(prepared[0]["asset_name"], "PNC Financial Services Group, Inc. (The) Common Stock")
            self.assertNotIn("_asset_name", prepared[0])
            self.assertNotIn("_debug_only", prepared[0])


if __name__ == "__main__":
    unittest.main()
