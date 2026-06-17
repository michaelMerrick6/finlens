import unittest
from unittest.mock import patch

from sec_13f_support import (
    SecTickerResolver,
    build_13f_filing_entries,
    extract_information_table_root,
    extract_report_period,
    is_unbounded_limit,
    is_supported_equity_row,
    load_available_13f_filings,
    normalize_issuer_name,
    normalize_share_class,
)


class Sec13FSupportTests(unittest.TestCase):
    def test_unbounded_limit_treats_zero_and_none_as_all_history(self) -> None:
        self.assertTrue(is_unbounded_limit(None))
        self.assertTrue(is_unbounded_limit(0))
        self.assertFalse(is_unbounded_limit(8))

    def test_normalizes_common_issuer_variants(self) -> None:
        self.assertEqual(normalize_issuer_name("ALLY FINL INC"), "ALLY FINANCIAL")
        self.assertEqual(normalize_issuer_name("BANK OF AMERICA CORP /DE/"), "BANK OF AMERICA")
        self.assertEqual(normalize_issuer_name("LOUISIANA PAC CORP"), "LOUISIANA PACIFIC")
        self.assertEqual(normalize_issuer_name("VERISIGN INC/CA"), "VERISIGN")

    def test_normalizes_share_class(self) -> None:
        self.assertEqual(normalize_share_class("CAP STK CL A"), "A")
        self.assertEqual(normalize_share_class("COM SER C"), "C")
        self.assertEqual(normalize_share_class("SPON ADR NEW"), "ADR")
        self.assertEqual(normalize_share_class("COM"), "COMMON")

    def test_filters_out_non_equity_rows(self) -> None:
        self.assertTrue(is_supported_equity_row("COM", "", "SH"))
        self.assertFalse(is_supported_equity_row("COM", "PUT", "SH"))
        self.assertFalse(is_supported_equity_row("PREFERRED STOCK", "", "SH"))
        self.assertFalse(is_supported_equity_row("COM", "", "PRN"))

    def test_resolves_class_specific_tickers(self) -> None:
        resolver = SecTickerResolver(
            [
                {"ticker": "GOOGL", "name": "Alphabet Inc.", "exchange": "Nasdaq"},
                {"ticker": "GOOG", "name": "Alphabet Inc.", "exchange": "Nasdaq"},
                {"ticker": "HEI", "name": "HEICO CORP", "exchange": "NYSE"},
                {"ticker": "HEI-A", "name": "HEICO CORP", "exchange": "NYSE"},
                {"ticker": "DEO", "name": "DIAGEO PLC", "exchange": "NYSE"},
                {"ticker": "DGEAF", "name": "DIAGEO PLC", "exchange": ""},
            ]
        )

        self.assertEqual(resolver.resolve_ticker("ALPHABET INC", "CAP STK CL A"), "GOOGL")
        self.assertEqual(resolver.resolve_ticker("ALPHABET INC", "CAP STK CL C"), "GOOG")
        self.assertEqual(resolver.resolve_ticker("HEICO CORP NEW", "CL A"), "HEI-A")
        self.assertEqual(resolver.resolve_ticker("DIAGEO PLC", "SPON ADR NEW"), "DEO")

    def test_extracts_period_and_information_table(self) -> None:
        text = """
        <SEC-DOCUMENT>
        <XML>
        <edgarSubmission>
          <formData>
            <coverPage>
              <periodOfReport>2025-12-31</periodOfReport>
            </coverPage>
          </formData>
        </edgarSubmission>
        </XML>
        <XML>
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
          <infoTable>
            <nameOfIssuer>APPLE INC</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>037833100</cusip>
            <value>100</value>
            <shrsOrPrnAmt>
              <sshPrnamt>200</sshPrnamt>
              <sshPrnamtType>SH</sshPrnamtType>
            </shrsOrPrnAmt>
          </infoTable>
        </informationTable>
        </XML>
        </SEC-DOCUMENT>
        """

        self.assertEqual(extract_report_period(text, "2026-02-17"), "2025-12-31")
        root = extract_information_table_root(text)
        self.assertIsNotNone(root)
        self.assertEqual(root.findall(".//infoTable")[0].findtext("nameOfIssuer"), "APPLE INC")

    def test_extracts_prefixed_information_table(self) -> None:
        text = """
        <SEC-DOCUMENT>
        <XML>
        <edgarSubmission>
          <formData>
            <coverPage>
              <periodOfReport>2026-03-31</periodOfReport>
            </coverPage>
          </formData>
        </edgarSubmission>
        </XML>
        <XML>
        <ns1:informationTable xmlns:ns1="http://www.sec.gov/edgar/document/thirteenf/informationtable">
          <ns1:infoTable>
            <ns1:nameOfIssuer>NVIDIA CORPORATION</ns1:nameOfIssuer>
            <ns1:titleOfClass>COM</ns1:titleOfClass>
            <ns1:cusip>67066G104</ns1:cusip>
            <ns1:value>100</ns1:value>
            <ns1:shrsOrPrnAmt>
              <ns1:sshPrnamt>200</ns1:sshPrnamt>
              <ns1:sshPrnamtType>SH</ns1:sshPrnamtType>
            </ns1:shrsOrPrnAmt>
          </ns1:infoTable>
        </ns1:informationTable>
        </XML>
        </SEC-DOCUMENT>
        """

        root = extract_information_table_root(text)

        self.assertIsNotNone(root)
        self.assertEqual(root.findall(".//infoTable")[0].findtext("nameOfIssuer"), "NVIDIA CORPORATION")

    def test_build_13f_filing_entries_dedupes_repeated_accessions(self) -> None:
        fund = {"cik": "0001167557", "name": "AQR CAPITAL MANAGEMENT LLC"}
        seen_accessions = {"0001085146-26-000240"}

        filings = build_13f_filing_entries(
            fund,
            {
                "form": ["13F-HR", "10-K", "13F-HR/A"],
                "accessionNumber": ["0001085146-26-000240", "0000000000-00-000000", "0001085146-25-000999"],
                "filingDate": ["2026-02-17", "2026-02-01", "2025-11-14"],
            },
            seen_accessions=seen_accessions,
        )

        self.assertEqual([filing["accession"] for filing in filings], ["0001085146-25-000999"])

    def test_load_available_13f_filings_includes_history_fragments(self) -> None:
        fund = {"cik": "0001167557", "name": "AQR CAPITAL MANAGEMENT LLC"}
        main_payload = {
            "filings": {
                "recent": {
                    "form": ["13F-HR", "13F-HR/A"],
                    "accessionNumber": ["0001085146-26-000240", "0001085146-25-000999"],
                    "filingDate": ["2026-02-17", "2025-11-14"],
                },
                "files": [{"name": "CIK0001167557-submissions-001.json"}],
            }
        }
        history_payload = {
            "form": ["13F-HR", "13F-HR"],
            "accessionNumber": ["0001085146-15-000123", "0001085146-25-000999"],
            "filingDate": ["2015-02-17", "2025-11-14"],
        }

        with patch("sec_13f_support.load_fund_submissions", return_value=main_payload), patch(
            "sec_13f_support.load_submission_history_file",
            return_value=history_payload,
        ):
            filings = load_available_13f_filings(None, fund, max_filings=0)

        self.assertEqual(
            [filing["accession"] for filing in filings],
            ["0001085146-26-000240", "0001085146-25-000999", "0001085146-15-000123"],
        )


if __name__ == "__main__":
    unittest.main()
