import unittest
from datetime import datetime

from bs4 import BeautifulSoup

from capitol_trades_support import build_bridge_doc_id, extract_official_source_url_from_html, parse_trade_page


class CapitolTradesSupportTests(unittest.TestCase):
    def test_parse_trade_page_relative_publish_dates(self) -> None:
        html = """
        <table>
          <tbody>
            <tr>
              <td>
                <a href="/politicians/T000490">David Taylor</a>
                <span class="party--republican">Republican</span>
                <span class="chamber--house">House</span>
                <span class="us-state-compact--oh">OH</span>
              </td>
              <td>
                <a href="/issuers/430515">Chevron Corp</a>
                <span class="issuer-ticker">CVX:US</span>
              </td>
              <td>
                <div class="text-size-3">13:01</div>
                <div class="text-size-2">Yesterday</div>
              </td>
              <td>
                <div class="text-size-3">12 Mar</div>
                <div class="text-size-2">2026</div>
              </td>
              <td><div class="q-value"><span>8</span></div></td>
              <td>Undisclosed</td>
              <td><span class="tx-type">sell</span></td>
              <td><span class="trade-size">1K–15K</span></td>
              <td><a href="/trades/20003795758">Detail</a></td>
            </tr>
          </tbody>
        </table>
        """

        trades = parse_trade_page(html, now=datetime(2026, 3, 24, 8, 0, 0))
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["source_document_id"], "capitol-trade-20003795758")
        self.assertEqual(trades[0]["published_date"], "2026-03-23")
        self.assertEqual(trades[0]["transaction_date"], "2026-03-12")
        self.assertEqual(trades[0]["ticker"], "CVX")
        self.assertEqual(trades[0]["transaction_type"], "sell")

    def test_extracts_official_source_url_from_detail_html(self) -> None:
        html = """
        <html>
          <body>
            <a href="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034207.pdf">Official Filing</a>
          </body>
        </html>
        """
        self.assertEqual(
            extract_official_source_url_from_html(html),
            "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034207.pdf",
        )

    def test_extracts_serialized_official_source_url_from_react_payload(self) -> None:
        html = """
        <script>
          self.__next_f.push([1,"filingUrl\\":\\"https:\\/\\/efdsearch.senate.gov\\/search\\/view\\/ptr\\/be9bb561-8290-4364-85b4-06a59ef0ec01\\/\\",\\"tradeId\\":10000065152"]);
        </script>
        """
        self.assertEqual(
            extract_official_source_url_from_html(html),
            "https://efdsearch.senate.gov/search/view/ptr/be9bb561-8290-4364-85b4-06a59ef0ec01/",
        )

    def test_builds_bridge_doc_ids_for_official_sources(self) -> None:
        self.assertEqual(
            build_bridge_doc_id(
                "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034207.pdf",
                "20003795758",
            ),
            "house-2026-20034207-capitol-20003795758",
        )
        self.assertEqual(
            build_bridge_doc_id(
                "https://efdsearch.senate.gov/search/view/paper/d337c392-e0aa-428e-be93-44a327b90d08/",
                "987654321",
            ),
            "senate-d337c392-e0aa-428e-be93-44a327b90d08-capitol-987654321",
        )


if __name__ == "__main__":
    unittest.main()
