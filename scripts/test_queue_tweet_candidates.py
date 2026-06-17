from queue_tweet_candidates import merge_signal_event_batches, signal_pipeline_stale_reason


def test_merge_signal_event_batches_dedupes_shared_rows() -> None:
    published_batch = [
        {
            "id": "event-1",
            "signal_type": "politician_cluster",
            "ticker": "NVDA",
            "published_at": "2026-05-01",
            "created_at": "2026-05-04T19:00:00+00:00",
        }
    ]
    created_batch = [
        {
            "id": "event-1",
            "signal_type": "politician_cluster",
            "ticker": "NVDA",
            "published_at": "2026-05-01",
            "created_at": "2026-05-04T19:00:00+00:00",
        },
        {
            "id": "event-2",
            "signal_type": "insider_cluster",
            "ticker": "AVBC",
            "published_at": "2026-04-30",
            "created_at": "2026-05-04T19:01:00+00:00",
        },
    ]

    merged = merge_signal_event_batches(published_batch, created_batch)

    assert [event["id"] for event in merged] == ["event-2", "event-1"]


def test_merge_signal_event_batches_falls_back_to_source_identity() -> None:
    first = [
        {
            "source": "insider",
            "source_document_id": "0000000000-26-000001",
            "signal_type": "insider_trade",
            "ticker": "ROKU",
            "actor_name": "Jane Doe",
            "direction": "buy",
            "published_at": "2026-04-29",
            "created_at": "2026-05-04T18:59:00+00:00",
        }
    ]
    second = [
        {
            "source": "insider",
            "source_document_id": "0000000000-26-000001",
            "signal_type": "insider_trade",
            "ticker": "ROKU",
            "actor_name": "Jane Doe",
            "direction": "buy",
            "published_at": "2026-04-29",
            "created_at": "2026-05-04T18:59:00+00:00",
        }
    ]

    merged = merge_signal_event_batches(first, second)

    assert len(merged) == 1
    assert merged[0]["source_document_id"] == "0000000000-26-000001"


def test_signal_pipeline_stale_reason_flags_missing_canonical_events() -> None:
    reason = signal_pipeline_stale_reason(
        0,
        {
            "politician_trades": "2026-05-01",
            "insider_trades": "2026-05-01",
        },
        lookback_hours=96,
    )

    assert reason is not None
    assert "No signal_events were available" in reason
    assert "politician_trades=2026-05-01" in reason


def test_signal_pipeline_stale_reason_ignores_healthy_cases() -> None:
    assert signal_pipeline_stale_reason(3, {"politician_trades": "2026-05-01"}, lookback_hours=96) is None
    assert signal_pipeline_stale_reason(0, {"politician_trades": None, "insider_trades": None}, lookback_hours=96) is None


def main() -> None:
    test_merge_signal_event_batches_dedupes_shared_rows()
    test_merge_signal_event_batches_falls_back_to_source_identity()
    test_signal_pipeline_stale_reason_flags_missing_canonical_events()
    test_signal_pipeline_stale_reason_ignores_healthy_cases()
    print("queue tweet candidate tests passed")


if __name__ == "__main__":
    main()
