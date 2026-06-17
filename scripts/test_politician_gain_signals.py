import politician_gain_signals as module


def test_build_politician_gain_milestone_events() -> None:
    original_fetch = module.fetch_market_price_series
    try:
        module.fetch_market_price_series = lambda ticker, earliest_date=None: {
            "ticker": ticker,
            "instrument_type": "EQUITY",
            "current_price": 48.0,
            "price_as_of": "2026-04-14",
            "points": [
                {"date": "2026-01-10", "price": 24.0},
                {"date": "2026-02-15", "price": 34.0},
                {"date": "2026-04-14", "price": 48.0},
            ],
        }
        events = [
            {
                "id": "trade-1",
                "source": "congress",
                "signal_type": "politician_trade",
                "source_document_id": "house-2026-12345-0",
                "ticker": "PLTR",
                "actor_name": "Nancy Pelosi",
                "actor_type": "politician",
                "direction": "buy",
                "occurred_at": "2026-01-10",
                "published_at": "2026-01-16",
                "importance_score": 0.9,
                "title": "Congress trade",
                "summary": "Congress buy",
                "source_url": "https://example.com/filing",
                "payload": {
                    "member_id": "P000197",
                    "amount_range": "$100,001 - $250,000",
                    "asset_type": "Stock",
                    "asset_name": "Palantir Technologies Inc.",
                },
            }
        ]
        compiled = module.build_politician_gain_milestone_events(events)
        assert len(compiled) == 1
        event = compiled[0]
        assert event["signal_type"] == "politician_gain_milestone"
        assert event["payload"]["gain_milestone_pct"] == 100
        assert event["payload"]["gain_return_pct"] == 100.0
        assert event["payload"]["entry_price"] == 24.0
        assert event["payload"]["current_price"] == 48.0
        assert event["payload"]["holding_days"] == 94

        fast_events = [
            {
                "id": "trade-2",
                "source": "congress",
                "signal_type": "politician_trade",
                "source_document_id": "house-2026-99999-0",
                "ticker": "PLTR",
                "actor_name": "Example Member",
                "actor_type": "politician",
                "direction": "buy",
                "occurred_at": "2026-02-15",
                "published_at": "2026-02-20",
                "importance_score": 0.8,
                "title": "Congress trade",
                "summary": "Congress buy",
                "source_url": "https://example.com/filing2",
                "payload": {
                    "member_id": "X000001",
                    "amount_range": "$100,001 - $250,000",
                    "asset_type": "Stock",
                    "asset_name": "Palantir Technologies Inc.",
                },
            }
        ]
        fast_compiled = module.build_politician_gain_milestone_events(fast_events)
        assert len(fast_compiled) == 1
        fast_event = fast_compiled[0]
        assert fast_event["payload"]["gain_milestone_pct"] == 40
        assert fast_event["payload"]["holding_days"] == 58
        assert fast_event["importance_score"] >= 0.88

        future_dated_events = [
            {
                "id": "trade-3",
                "source": "congress",
                "signal_type": "politician_trade",
                "source_document_id": "house-2026-future-0",
                "ticker": "PLTR",
                "actor_name": "Future Dated Member",
                "actor_type": "politician",
                "direction": "buy",
                "occurred_at": "2026-08-31",
                "published_at": "2026-04-08",
                "importance_score": 0.8,
                "title": "Congress trade",
                "summary": "Congress buy",
                "source_url": "https://example.com/filing3",
                "payload": {
                    "member_id": "X000002",
                    "amount_range": "$100,001 - $250,000",
                    "asset_type": "Stock",
                    "asset_name": "Palantir Technologies Inc.",
                },
            }
        ]
        future_compiled = module.build_politician_gain_milestone_events(future_dated_events)
        assert len(future_compiled) == 1
        assert future_compiled[0]["payload"]["holding_days"] == 6

        stored_baseline_events = [
            {
                "id": "trade-4",
                "source": "congress",
                "signal_type": "politician_trade",
                "source_document_id": "house-2026-stored-0",
                "ticker": "PLTR",
                "actor_name": "Stored Baseline Member",
                "actor_type": "politician",
                "direction": "buy",
                "occurred_at": "2026-02-15",
                "published_at": "2026-02-20",
                "importance_score": 0.8,
                "title": "Congress trade",
                "summary": "Congress buy",
                "source_url": "https://example.com/filing4",
                "payload": {
                    "member_id": "X000003",
                    "amount_range": "$100,001 - $250,000",
                    "asset_type": "Stock",
                    "asset_name": "Palantir Technologies Inc.",
                    "baseline_price": 34.0,
                    "baseline_price_date": "2026-02-15",
                    "baseline_reference_date": "2026-02-15",
                    "baseline_reference_type": "trade_date",
                    "baseline_price_provider": "snapshot_provider",
                },
            }
        ]
        stored_baseline_compiled = module.build_politician_gain_milestone_events(stored_baseline_events)
        assert len(stored_baseline_compiled) == 1
        stored_baseline_event = stored_baseline_compiled[0]
        assert stored_baseline_event["payload"]["entry_price"] == 34.0
        assert stored_baseline_event["payload"]["baseline_price"] == 34.0
        assert stored_baseline_event["payload"]["baseline_price_provider"] == "snapshot_provider"
        assert stored_baseline_event["payload"]["gain_milestone_pct"] == 40
    finally:
        module.fetch_market_price_series = original_fetch


def test_build_cluster_gain_milestone_events() -> None:
    original_fetch = module.fetch_market_price_series
    try:
        module.fetch_market_price_series = lambda ticker, earliest_date=None: {
            "ticker": ticker,
            "instrument_type": "EQUITY",
            "current_price": 600.0,
            "price_as_of": "2026-04-14",
            "points": [
                {"date": "2026-01-08", "price": 300.0},
                {"date": "2026-04-14", "price": 600.0},
            ],
        }
        events = [
            {
                "id": "cluster-1",
                "source": "congress",
                "signal_type": "politician_cluster",
                "source_document_id": "cluster-doc-1",
                "ticker": "MSFT",
                "actor_name": "Congress cluster",
                "direction": "buy",
                "occurred_at": "2026-01-08",
                "published_at": "2026-01-08",
                "importance_score": 0.91,
                "title": "Congress cluster",
                "summary": "Congress cluster buy",
                "source_url": "https://example.com/cluster",
                "payload": {
                    "cluster_actor_count": 3,
                    "cluster_combined_lower_bound": 150001,
                    "cluster_actors": [
                        {"name": "Gil Cisneros", "source": "congress"},
                        {"name": "Josh Gottheimer", "source": "congress"},
                        {"name": "Richard McCormick", "source": "congress"},
                    ],
                    "congress_actor_count": 3,
                },
            }
        ]
        compiled = module.build_cluster_gain_milestone_events(events)
        assert len(compiled) == 1
        event = compiled[0]
        assert event["signal_type"] == "cluster_gain_milestone"
        assert event["payload"]["gain_milestone_pct"] == 100
        assert event["payload"]["gain_return_pct"] == 100.0
        assert event["payload"]["entry_price"] == 300.0
        assert event["payload"]["current_price"] == 600.0
        assert event["payload"]["cluster_combined_lower_bound"] == 150001.0
        assert event["payload"]["cluster_clocked_at"] == "2026-01-08"
        assert event["payload"]["days_since_cluster"] == 96
    finally:
        module.fetch_market_price_series = original_fetch


def main() -> None:
    test_build_politician_gain_milestone_events()
    test_build_cluster_gain_milestone_events()
    print("politician gain signals tests passed")


if __name__ == "__main__":
    main()
