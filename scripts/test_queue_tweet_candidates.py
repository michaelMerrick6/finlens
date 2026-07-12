from queue_tweet_candidates import merge_signal_event_batches, prune_stale_pending_candidates, signal_pipeline_stale_reason


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeCandidateQuery:
    def __init__(self, rows, deleted_ids, operation="select"):
        self.rows = rows
        self.deleted_ids = deleted_ids
        self.operation = operation
        self.start = 0
        self.end = len(rows) - 1
        self.ids = []

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self.start = start
        self.end = end
        return self

    def delete(self):
        self.operation = "delete"
        return self

    def in_(self, _column, ids):
        self.ids = list(ids)
        return self

    def execute(self):
        if self.operation == "delete":
            self.deleted_ids.extend(self.ids)
            return FakeResponse([])
        return FakeResponse(self.rows[self.start : self.end + 1])


class FakeSupabase:
    def __init__(self, rows):
        self.rows = rows
        self.deleted_ids = []

    def table(self, _name):
        return FakeCandidateQuery(self.rows, self.deleted_ids)


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


def test_prune_stale_pending_candidates_pages_past_first_thousand() -> None:
    valid_keys = {f"valid-{index}" for index in range(1000)}
    rows = [
        {
            "id": f"candidate-{index}",
            "candidate_key": f"valid-{index}",
            "signal_events": {"published_at": "2026-06-01"},
        }
        for index in range(1000)
    ]
    rows.append(
        {
            "id": "stale-after-page-one",
            "candidate_key": "legacy-overlap",
            "signal_events": {"published_at": "2026-06-01"},
        }
    )
    supabase = FakeSupabase(rows)

    deleted = prune_stale_pending_candidates(
        supabase,
        valid_candidate_keys=valid_keys,
        published_since="2026-05-01",
    )

    assert deleted == 1
    assert supabase.deleted_ids == ["stale-after-page-one"]


def main() -> None:
    test_merge_signal_event_batches_dedupes_shared_rows()
    test_merge_signal_event_batches_falls_back_to_source_identity()
    test_signal_pipeline_stale_reason_flags_missing_canonical_events()
    test_signal_pipeline_stale_reason_ignores_healthy_cases()
    test_prune_stale_pending_candidates_pages_past_first_thousand()
    print("queue tweet candidate tests passed")


if __name__ == "__main__":
    main()
