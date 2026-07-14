import unittest

from emit_signal_events import build_fund_events
from ingest_sec_13f import apply_qoq, select_recent_distinct_periods


def make_holding(
    ticker: str,
    shares_held: int,
    *,
    value_held: int = 1000,
    report_period: str = "2025-12-31",
    published_date: str = "2026-02-14",
    fund_name: str = "Example Capital",
) -> dict:
    return {
        "fund_name": fund_name,
        "ticker": ticker,
        "report_period": report_period,
        "published_date": published_date,
        "shares_held": shares_held,
        "value_held": value_held,
        "source_url": "https://www.sec.gov/Archives/example.txt",
    }


class IngestSec13FTests(unittest.TestCase):
    def test_select_recent_distinct_periods_can_be_unbounded(self) -> None:
        parsed_periods = [
            {"report_period": "2024-03-31", "published_date": "2024-05-15"},
            {"report_period": "2024-06-30", "published_date": "2024-08-14"},
            {"report_period": "2024-09-30", "published_date": "2024-11-14"},
        ]

        selected = select_recent_distinct_periods(parsed_periods, 0)

        self.assertEqual([row["report_period"] for row in selected], ["2024-03-31", "2024-06-30", "2024-09-30"])

    def test_apply_qoq_marks_new_positions_and_full_exits(self) -> None:
        previous = [
            make_holding("AAPL", 100, value_held=20_000),
            make_holding("MSFT", 80, value_held=16_000),
        ]
        current = [
            make_holding("AAPL", 140, value_held=28_000, report_period="2026-03-31", published_date="2026-05-15"),
            make_holding("NVDA", 50, value_held=25_000, report_period="2026-03-31", published_date="2026-05-15"),
        ]

        compared = apply_qoq(current, previous)
        compared_by_ticker = {row["ticker"]: row for row in compared}

        self.assertEqual(compared_by_ticker["AAPL"]["qoq_change_shares"], 40)
        self.assertEqual(compared_by_ticker["AAPL"]["qoq_change_percent"], 40.0)
        self.assertEqual(compared_by_ticker["NVDA"]["qoq_change_shares"], 50)
        self.assertIsNone(compared_by_ticker["NVDA"]["qoq_change_percent"])
        self.assertEqual(compared_by_ticker["MSFT"]["shares_held"], 0)
        self.assertEqual(compared_by_ticker["MSFT"]["qoq_change_shares"], -80)
        self.assertEqual(compared_by_ticker["MSFT"]["qoq_change_percent"], -100.0)

    def test_build_fund_events_emits_new_and_exit_rows(self) -> None:
        rows = [
            {
                **make_holding("NVDA", 50, value_held=25_000, report_period="2026-03-31"),
                "qoq_change_shares": 50,
                "qoq_change_percent": None,
            },
            {
                **make_holding("MSFT", 0, value_held=0, report_period="2026-03-31"),
                "qoq_change_shares": -80,
                "qoq_change_percent": -100.0,
            },
        ]

        _raw, events = build_fund_events(rows)
        position_events = [event for event in events if event["signal_type"] == "fund_position_change"]
        filing_events = [event for event in events if event["signal_type"] == "fund_filing_received"]

        self.assertEqual(len(position_events), 2)
        self.assertEqual(len(filing_events), 1)
        by_ticker = {event["ticker"]: event for event in position_events}

        self.assertEqual(by_ticker["NVDA"]["direction"], "increase")
        self.assertIn("initiated", by_ticker["NVDA"]["title"].lower())
        self.assertIn("$25,000", by_ticker["NVDA"]["summary"])
        self.assertEqual(by_ticker["MSFT"]["direction"], "decrease")
        self.assertIn("exited", by_ticker["MSFT"]["title"].lower())
        self.assertEqual(filing_events[0]["actor_name"], "Example Capital")
        self.assertEqual(filing_events[0]["ticker"], "13F")
        self.assertEqual(filing_events[0]["payload"]["holding_count"], 2)

    def test_build_fund_events_treats_de_minimis_prior_as_material_new_position(self) -> None:
        rows = [
            {
                **make_holding("INTC", 202_344, value_held=8_929_441, report_period="2026-03-31"),
                "qoq_change_shares": 202_343,
                "qoq_change_percent": 20_234_300.0,
            },
        ]

        _raw, events = build_fund_events(rows)
        position_events = [event for event in events if event["signal_type"] == "fund_position_change"]
        filing_events = [event for event in events if event["signal_type"] == "fund_filing_received"]

        self.assertEqual(len(position_events), 1)
        self.assertEqual(len(filing_events), 1)
        self.assertEqual(position_events[0]["payload"]["change_type"], "new")
        self.assertIn("initiated", position_events[0]["title"].lower())
        self.assertIn("new INTC position", position_events[0]["summary"])
        self.assertNotIn("%", position_events[0]["summary"])


if __name__ == "__main__":
    unittest.main()
