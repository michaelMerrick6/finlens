import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from scripts.ingest_house_official import (
    detect_house_non_public_only_lines,
    extract_transactions_from_layout_lines,
    extract_transactions_from_lines,
    parse_house_scanned_date,
)


class HouseParserRegressionTests(unittest.TestCase):
    def test_scanned_date_parser_recovers_truncated_year_digit(self):
        self.assertEqual(parse_house_scanned_date("05/21/1", 2015), "2015-05-21")
        self.assertEqual(parse_house_scanned_date("6/8/1", 2015), "2015-06-08")
        self.assertEqual(parse_house_scanned_date("11/02/4", 2015), "2014-11-02")

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

    def test_layout_parser_handles_2015_house_variant(self):
        lines = [
            "Filing ID #20003280",
            "name: Hon. Barbara J. Comstock",
            "transactionS",
            "sP ambarella, Inc. - Ordinary shares s 06/19/2015 06/18/2015 $1,001 - $15,000",
            "(aMBa)",
            "FIlINg sTaTus: New",
        ]

        trades = extract_transactions_from_layout_lines(
            lines,
            "20003280",
            "Barbara J.",
            "Comstock",
            2015,
            [{"id": "barbaracomstock", "first_name": "Barbara J.", "last_name": "Comstock"}],
            [],
        )

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["ticker"], "AMBA")
        self.assertEqual(trades[0]["transaction_type"], "sell")
        self.assertEqual(trades[0]["transaction_date"], "2015-06-19")
        self.assertEqual(trades[0]["published_date"], "2015-06-18")

    def test_layout_parser_keeps_ambarella_rows_from_multiline_house_filing(self):
        lines = [
            "FILINg STATUS: New",
            "SUBHoLDINg oF: Morgan Stanley Rollover IRA Account (1)",
            "Ambarella, Inc. - ordinary Shares P 05/15/2019 05/31/2019 $1,001 - $15,000 f",
            "c",
            "d",
            "e",
            "g",
            "(AMBA) [ST]",
            "FILINg STATUS: New",
            "SUBHoLDINg oF: Morgan Stanley Rollover IRA Account (1)",
            "Ambarella, Inc. - ordinary Shares S 06/24/2019 06/30/2019 $1,001 - $15,000 g",
            "f",
            "e",
            "d",
            "c",
            "(AMBA) [ST]",
            "FILINg STATUS: New",
            "SUBHoLDINg oF: Morgan Stanley Rollover IRA Account (1)",
            "Ambarella, Inc. - ordinary Shares P 04/16/2019 04/30/2019 $1,001 - $15,000 g",
            "f",
            "e",
            "d",
            "c",
            "(AMBA) [ST]",
        ]

        trades = extract_transactions_from_layout_lines(
            lines,
            "20016481",
            "Donna",
            "Shalala",
            2020,
            [{"id": "donnashalala", "first_name": "Donna", "last_name": "Shalala"}],
            [],
        )

        self.assertEqual(len(trades), 3)
        self.assertEqual(
            [(trade["ticker"], trade["transaction_type"], trade["transaction_date"]) for trade in trades],
            [
                ("AMBA", "buy", "2019-05-15"),
                ("AMBA", "sell", "2019-06-24"),
                ("AMBA", "buy", "2019-04-16"),
            ],
        )

    def test_standard_parser_captures_option_details_from_house_description(self):
        lines = [
            "Microsoft Corporation - Common",
            "Stock (MSFT) [OP]",
            "P 03/25/2026 04/07/2026 $50,001 - $100,000",
            "Filing Status: New",
            "Subholding Of: Morgan Stanley - Portfolio Management Active Assets Account",
            "Description: Call options; Strike price $325; Expires 06/18/2026",
        ]

        trades = extract_transactions_from_lines(
            lines,
            "20034305",
            "Josh",
            "Gottheimer",
            2026,
            [{"id": "joshgottheimer", "first_name": "Josh", "last_name": "Gottheimer"}],
            [],
        )

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["ticker"], "MSFT")
        self.assertEqual(trades[0]["asset_type"], "OP")
        self.assertEqual(
            trades[0]["asset_name"],
            "Microsoft Corporation - Common Stock [OP] | Call option | Strike $325 | Expires 2026-06-18",
        )

    def test_layout_parser_marks_option_rows_when_op_marker_is_inline(self):
        lines = [
            "JT Microsoft Corporation - Common Stock [OP] P 03/25/2026 04/08/2026 $500,001",
            "Description: Call options; Strike price $320; Expires 06/18/2026",
        ]

        trades = extract_transactions_from_layout_lines(
            lines,
            "20034305",
            "Josh",
            "Gottheimer",
            2026,
            [{"id": "joshgottheimer", "first_name": "Josh", "last_name": "Gottheimer"}],
            [],
        )

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["asset_type"], "OP")
        self.assertEqual(
            trades[0]["asset_name"],
            "Microsoft Corporation - Common Stock [OP] | Call option | Strike $320 | Expires 2026-06-18",
        )

    def test_detects_non_public_only_house_ocr_lines(self):
        self.assertTrue(
            detect_house_non_public_only_lines(
                [
                    "Massachusetts",
                    "ST FOR ISS SR C BERR",
                    "Washington",
                    "ST VAR PURP SR D BE/R/",
                    "Fulton",
                    "CNTY GA GEN FUND BE/R/",
                ]
            )
        )
        self.assertFalse(
            detect_house_non_public_only_lines(
                [
                    "Microsoft Corporation - Common Stock",
                    "Stock (MSFT) [ST]",
                    "P 03/25/2026 04/07/2026 $50,001 - $100,000",
                ]
            )
        )


if __name__ == "__main__":
    unittest.main()
