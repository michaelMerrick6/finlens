import unittest

from sec_form4_support import (
    extract_sec_accession,
    parse_feed_filed_date,
    parse_form4_xml_text,
    recent_filings_cache_covers_request,
)


class SecForm4SupportTests(unittest.TestCase):
    def test_extracts_accession_from_multiple_url_shapes(self) -> None:
        self.assertEqual(
            extract_sec_accession("https://www.sec.gov/Archives/edgar/data/2001123/000200112326000005/0002001123-26-000005.txt"),
            "0002001123-26-000005",
        )
        self.assertEqual(
            extract_sec_accession("https://www.sec.gov/Archives/edgar/data/2001123/000200112326000005/"),
            "0002001123-26-000005",
        )

    def test_prefers_feed_filed_date_and_canonicalizes_source_url(self) -> None:
        text = """
        <SEC-DOCUMENT>
        <XML>
        <ownershipDocument>
          <issuer>
            <issuerCik>0001777393</issuerCik>
            <issuerName>ChargePoint Holdings, Inc.</issuerName>
            <issuerTradingSymbol>CHPT</issuerTradingSymbol>
          </issuer>
          <reportingOwner>
            <reportingOwnerId>
              <rptOwnerName>Khetani Mansi</rptOwnerName>
            </reportingOwnerId>
            <reportingOwnerRelationship>
              <isOfficer>1</isOfficer>
              <officerTitle>Chief Accounting Officer</officerTitle>
            </reportingOwnerRelationship>
          </reportingOwner>
          <periodOfReport>2026-03-23</periodOfReport>
          <nonDerivativeTransaction>
            <transactionDate><value>2026-03-23</value></transactionDate>
            <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>100</value></transactionShares>
              <transactionPricePerShare><value>1.50</value></transactionPricePerShare>
            </transactionAmounts>
          </nonDerivativeTransaction>
          <nonDerivativeTransaction>
            <transactionDate><value>2026-03-23</value></transactionDate>
            <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>50</value></transactionShares>
              <transactionPricePerShare><value>0</value></transactionPricePerShare>
            </transactionAmounts>
          </nonDerivativeTransaction>
        </ownershipDocument>
        </XML>
        </SEC-DOCUMENT>
        """

        parsed = parse_form4_xml_text(
            text,
            fallback_source_url="https://www.sec.gov/Archives/edgar/data/2001123/000200112326000005/0002001123-26-000005.txt",
            filed_date="2026-03-24",
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["source_url"], "https://www.sec.gov/Archives/edgar/data/1777393/000200112326000005/0002001123-26-000005.txt")
        self.assertEqual(parsed["filed_date"], "2026-03-24")
        self.assertEqual(len(parsed["rows"]), 1)
        self.assertEqual(parsed["rows"][0]["published_date"], "2026-03-24")
        self.assertEqual(parsed["rows"][0]["transaction_code"], "buy")
        self.assertEqual(parsed["rows"][0]["filer_relation"], "Chief Accounting Officer")

    def test_parses_feed_filed_date(self) -> None:
        self.assertEqual(
            parse_feed_filed_date(" <b>Filed:</b> 2026-03-24 <b>AccNo:</b> 0002001123-26-000005 "),
            "2026-03-24",
        )

    def test_uses_acquired_disposed_code_when_transaction_code_missing(self) -> None:
        text = """
        <SEC-DOCUMENT>
        <XML>
        <ownershipDocument>
          <issuer>
            <issuerCik>0001777393</issuerCik>
            <issuerName>ChargePoint Holdings, Inc.</issuerName>
            <issuerTradingSymbol>CHPT</issuerTradingSymbol>
          </issuer>
          <reportingOwner>
            <reportingOwnerId>
              <rptOwnerName>Khetani Mansi</rptOwnerName>
            </reportingOwnerId>
          </reportingOwner>
          <periodOfReport>2026-03-23</periodOfReport>
          <nonDerivativeTransaction>
            <transactionDate><value>2026-03-23</value></transactionDate>
            <transactionCoding><transactionCode></transactionCode></transactionCoding>
            <transactionAmounts>
              <transactionShares><value>100</value></transactionShares>
              <transactionPricePerShare><value>1.50</value></transactionPricePerShare>
              <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
          </nonDerivativeTransaction>
        </ownershipDocument>
        </XML>
        </SEC-DOCUMENT>
        """

        parsed = parse_form4_xml_text(
            text,
            fallback_source_url="https://www.sec.gov/Archives/edgar/data/2001123/000200112326000005/0002001123-26-000005.txt",
            filed_date="2026-03-24",
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["rows"]), 1)
        self.assertEqual(parsed["rows"][0]["transaction_code"], "buy")

    def test_recent_filings_cache_only_covers_narrower_requests(self) -> None:
        payload = {
            "days": 30,
            "limit": 600,
            "pages": 10,
            "filings": [{"accession": "0002001123-26-000005"}],
        }

        self.assertTrue(recent_filings_cache_covers_request(payload, days=14, limit=400, pages=6))
        self.assertFalse(recent_filings_cache_covers_request(payload, days=30, limit=800, pages=10))
        self.assertFalse(recent_filings_cache_covers_request(payload, days=45, limit=400, pages=6))


if __name__ == "__main__":
    unittest.main()
