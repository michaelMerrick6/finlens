import unittest
from pathlib import Path
import sys
from unittest.mock import patch
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from scripts.ingest_senate_official import (
    validate_paper_extraction,
    PaperFilingReviewRequired,
    parse_senate_paper_report,
    prepare_senate_trades_for_insert,
    resolve_company_ticker,
    parse_senate_html_table,
)


class SenateParserRegressionTests(unittest.TestCase):
    def test_html_retains_non_public_asset_type(self) -> None:
        soup = BeautifulSoup('<table class="table"><tbody><tr><td>1</td><td>08/18/2026</td><td>Spouse</td><td>--</td><td>Private Company</td><td>Non-Public Stock</td><td>Sale (Full)</td><td>$50,001 - $100,000</td></tr></tbody></table>', 'html.parser')
        trades = parse_senate_html_table(soup, doc_key='test', member_id='B001288', first_name='Cory', last_name='Booker', filed_date='2026-09-09', source_url='https://example.com/report')
        self.assertEqual(trades[0]['asset_type'], 'NON-PUBLIC STOCK')
        self.assertEqual(trades[0]['ticker'], 'N/A')

    def test_private_entity_names_do_not_become_false_tickers(self) -> None:
        valid_tickers = {"MH", "AWAY", "IBM", "N/A"}

        self.assertIsNone(resolve_company_ticker("MH Built to Last LLC", valid_tickers))
        self.assertIsNone(resolve_company_ticker("Not Fade Away LLC", valid_tickers))
        self.assertEqual(resolve_company_ticker("IBM Corp. (stock)", valid_tickers), "IBM")

    def test_incomplete_or_ambiguous_scan_requires_review(self):
        trade = {"transaction_date": "2026-02-05", "transaction_type": "sell", "amount_range": "$1,001 - $15,000"}
        validate_paper_extraction([trade], 1, "2026-03-13")
        for rows, count in [([trade], 2), ([trade], 0),
                            ([dict(trade, transaction_type="Unknown")], 1),
                            ([dict(trade, amount_range="Unknown")], 1),
                            ([dict(trade, transaction_date="2027-01-01")], 1)]:
            with self.subTest(rows=rows, count=count), self.assertRaises(PaperFilingReviewRequired):
                validate_paper_extraction(rows, count, "2026-03-13")

    def test_rejected_scan_does_not_write_companies(self):
        module = "scripts.ingest_senate_official."
        soup = BeautifulSoup("<html></html>", "html.parser")
        trade = {"ticker": "IBM", "transaction_date": "2026-02-05",
                 "transaction_type": "sell", "amount_range": "Unknown"}
        with patch(module + "extract_paper_image_urls", return_value=["https://example.com/scan"]), \
             patch(module + "load_paper_images", return_value=[]), \
             patch(module + "reviewed_senate_trades", return_value=None), \
             patch("senate_table_ocr.extract_document", side_effect=__import__("senate_table_ocr").TableReviewRequired("ambiguous checkbox")), \
             patch(module + "upsert_company") as write:
            with self.assertRaises(PaperFilingReviewRequired):
                parse_senate_paper_report(None, soup, doc_key="unreviewed",
                    member_id="B001277", first_name="Richard", last_name="Blumenthal",
                    filed_date="2026-03-13", source_url="https://example.com/scan",
                    valid_tickers={"IBM"})
            write.assert_not_called()

    def test_table_import_preserves_repeated_private_transactions(self):
        module = "scripts.ingest_senate_official."
        row = dict(asset_name="(S) Private LLC", transaction_date="2026-02-05",
                   transaction_type="sell", amount_range="$1,001 - $15,000")
        with patch(module + "extract_paper_image_urls", return_value=["https://example.com/scan"]), \
             patch(module + "load_paper_images", return_value=[]), \
             patch(module + "reviewed_senate_trades", return_value=None), \
             patch("senate_table_ocr.extract_document", return_value=[dict(row,page=1,row=1),dict(row,page=2,row=1)]), \
             patch(module + "upsert_company") as write:
            trades, count = parse_senate_paper_report(None, BeautifulSoup("", "html.parser"),
                doc_key="unreviewed", member_id="B001277", first_name="Richard", last_name="Blumenthal",
                filed_date="2026-03-13", source_url="https://example.com/scan", valid_tickers=set())
        self.assertEqual(count,2)
        self.assertEqual(len({trade["doc_id"] for trade in trades}),2)
        self.assertTrue(all(trade["ticker"] == "N/A" for trade in trades))
        self.assertTrue(all(trade["asset_type"] == "OTHER" for trade in trades))
        write.assert_not_called()

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
