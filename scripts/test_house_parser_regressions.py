import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.ingest_house_official import extract_transactions_from_lines


class HouseParserRegressionTests(unittest.TestCase):
    def test_keeps_distinct_subholding_rows_from_same_pdf(self):
        lines = [
            "P T R",
            "Name: Cleo Fields",
            "Status: Member",
            "State/District:LA06",
            "Microsoft Corporation - Common",
            "Stock (MSFT) [ST]",
            "P 03/12/202603/12/2026$1,001 - $15,000",
            "F S : New",
            "S O : Morgan Stanley - E*TRADE - Fields Law Firm 2, LLC",
            "Microsoft Corporation - Common",
            "Stock (MSFT) [ST]",
            "P 03/12/202603/12/2026$1,001 - $15,000",
            "F S : New",
            "S O : Morgan Stanley - E*TRADE #2",
        ]

        trades = extract_transactions_from_lines(
            lines,
            "20034179",
            "Cleo",
            "Fields",
            2026,
            [{"id": "cleofields", "first_name": "Cleo", "last_name": "Fields"}],
            [],
        )

        self.assertEqual(len(trades), 2)
        self.assertEqual([trade["doc_id"] for trade in trades], ["house-2026-20034179-0", "house-2026-20034179-1"])
        self.assertTrue(all(trade["ticker"] == "MSFT" for trade in trades))


if __name__ == "__main__":
    unittest.main()
