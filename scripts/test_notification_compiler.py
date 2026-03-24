from notification_compiler import compile_notification_events, filing_group_key


def test_grouped_same_filing_same_ticker():
    events = [
        {
            "id": "e1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034179-0",
            "ticker": "MSFT",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-12",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade: Cleo Fields bought MSFT",
            "summary": "One row",
            "source_url": "https://example.com/ptr.pdf",
            "payload": {"member_id": "F000470", "amount_range": "$1,001 - $15,000"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "e2",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034179-1",
            "ticker": "MSFT",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-12",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade: Cleo Fields bought MSFT",
            "summary": "Second row",
            "source_url": "https://example.com/ptr.pdf",
            "payload": {"member_id": "F000470", "amount_range": "$1,001 - $15,000"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events)
    grouped = [event for event in compiled if event["signal_type"] == "politician_trade_grouped"]
    assert len(grouped) == 1
    assert grouped[0]["payload"]["group_row_count"] == 2


def test_congress_cluster():
    events = [
        {
            "id": "e1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034179-0",
            "ticker": "NVDA",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-18",
            "published_at": "2026-03-18",
            "importance_score": 0.72,
            "title": "Congress trade: Thomas Kean bought NVDA",
            "summary": "One row",
            "source_url": "https://example.com/a.pdf",
            "payload": {"member_id": "K000398"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "e2",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034114-0",
            "ticker": "NVDA",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-17",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade: Cleo Fields bought NVDA",
            "summary": "One row",
            "source_url": "https://example.com/b.pdf",
            "payload": {"member_id": "F000470"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "politician_cluster"]
    assert len(clusters) == 1
    assert clusters[0]["payload"]["cluster_actor_count"] == 2


def test_insider_urls_with_same_accession_share_filing_group():
    event_a = {
        "source": "insider",
        "source_url": "https://www.sec.gov/Archives/edgar/data/1903382/000110465926032683/0001104659-26-032683.txt",
        "payload": {},
    }
    event_b = {
        "source": "insider",
        "source_url": "https://www.sec.gov/Archives/edgar/data/1111908/000110465926032683/0001104659-26-032683.txt",
        "payload": {},
    }
    assert filing_group_key(event_a) == "0001104659-26-032683"
    assert filing_group_key(event_b) == "0001104659-26-032683"


def test_actor_filing_summary_created():
    events = [
        {
            "id": "m1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "senate-b195126e-7bb2-4d54-baf5-e6fc8e7a0165-11",
            "ticker": "AAPL",
            "actor_name": "Markwayne Mullin",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2025-12-29",
            "published_at": "2026-01-16",
            "importance_score": 0.72,
            "title": "Congress trade: Markwayne Mullin bought AAPL",
            "summary": "One row",
            "source_url": "https://example.com/ptr",
            "payload": {"member_id": "M001190", "amount_range": "$100,001 - $250,000", "asset_type": "Stock"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "m2",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "senate-b195126e-7bb2-4d54-baf5-e6fc8e7a0165-12",
            "ticker": "MSFT",
            "actor_name": "Markwayne Mullin",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2025-12-29",
            "published_at": "2026-01-16",
            "importance_score": 0.72,
            "title": "Congress trade: Markwayne Mullin bought MSFT",
            "summary": "Second row",
            "source_url": "https://example.com/ptr",
            "payload": {"member_id": "M001190", "amount_range": "$100,001 - $250,000", "asset_type": "Stock"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events)
    summaries = [event for event in compiled if event["signal_type"] == "politician_filing_summary"]
    assert len(summaries) == 1
    assert summaries[0]["payload"]["summary_trade_count"] == 2
    assert summaries[0]["payload"]["summary_contains_unusual"] is True


def main():
    test_grouped_same_filing_same_ticker()
    test_congress_cluster()
    test_insider_urls_with_same_accession_share_filing_group()
    test_actor_filing_summary_created()
    print("notification compiler tests passed")


if __name__ == "__main__":
    main()
