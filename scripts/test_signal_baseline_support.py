import signal_baseline_support as module


def test_enrich_events_with_baseline_snapshots() -> None:
    original_fetch = module.fetch_market_price_series
    try:
        module.fetch_market_price_series = lambda ticker, earliest_date=None: {
            "ticker": ticker,
            "current_price": 110.0,
            "price_as_of": "2026-04-14",
            "provider": "test_provider",
            "points": [
                {"date": "2026-01-08", "price": 80.0},
                {"date": "2026-01-10", "price": 82.5},
                {"date": "2026-04-14", "price": 110.0},
            ],
        }

        events = [
            {
                "signal_type": "politician_trade",
                "ticker": "MSFT",
                "occurred_at": "2026-01-10",
                "published_at": "2026-01-16",
                "payload": {"asset_type": "Stock"},
            },
            {
                "signal_type": "politician_cluster",
                "ticker": "MSFT",
                "published_at": "2026-01-12",
                "payload": {"cluster_clocked_at": "2026-01-08", "cluster_actor_count": 3},
            },
        ]

        enriched = module.enrich_events_with_baseline_snapshots(events)
        politician_payload = enriched[0]["payload"]
        cluster_payload = enriched[1]["payload"]

        assert politician_payload["baseline_price"] == 82.5
        assert politician_payload["baseline_price_date"] == "2026-01-10"
        assert politician_payload["baseline_reference_date"] == "2026-01-10"
        assert politician_payload["baseline_reference_type"] == "trade_date"
        assert politician_payload["baseline_price_provider"] == "test_provider"

        assert cluster_payload["baseline_price"] == 80.0
        assert cluster_payload["baseline_price_date"] == "2026-01-08"
        assert cluster_payload["baseline_reference_date"] == "2026-01-08"
        assert cluster_payload["baseline_reference_type"] == "cluster_clocked_at"
        assert cluster_payload["baseline_price_provider"] == "test_provider"
    finally:
        module.fetch_market_price_series = original_fetch


def main() -> None:
    test_enrich_events_with_baseline_snapshots()
    print("signal baseline support tests passed")


if __name__ == "__main__":
    main()
