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
    assert grouped[0]["payload"]["group_combined_lower_bound"] == 2002
    assert grouped[0]["payload"]["group_amount_ranges"] == ["$1,001 - $15,000"]
    assert grouped[0]["payload"]["group_trade_date_start"] == "2026-03-12"
    assert grouped[0]["payload"]["group_trade_date_end"] == "2026-03-12"


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
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000"},
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
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "politician_cluster"]
    assert len(clusters) == 1
    assert clusters[0]["payload"]["cluster_actor_count"] == 2
    assert clusters[0]["payload"]["cluster_combined_lower_bound"] == 115002
    assert clusters[0]["payload"]["cluster_actors"][0]["amount_range"] == "$15,001 - $50,000"
    assert clusters[0]["payload"]["cluster_clocked_at"] == "2026-03-18"


def test_congress_cluster_reappearing_later_gets_new_identity():
    base_events = [
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
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000"},
            "created_at": "2026-03-18T20:00:00+00:00",
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
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-17T20:00:00+00:00",
        },
    ]
    initial_compiled = compile_notification_events(base_events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    initial_clusters = [event for event in initial_compiled if event["signal_type"] == "politician_cluster"]
    assert len(initial_clusters) == 1

    later_events = base_events + [
        {
            "id": "e3",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034888-0",
            "ticker": "NVDA",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-24",
            "published_at": "2026-03-24",
            "importance_score": 0.72,
            "title": "Congress trade: Thomas Kean bought NVDA",
            "summary": "Another row",
            "source_url": "https://example.com/c.pdf",
            "payload": {"member_id": "K000398", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-24T20:00:00+00:00",
        },
        {
            "id": "e4",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034889-0",
            "ticker": "NVDA",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-24",
            "published_at": "2026-03-24",
            "importance_score": 0.72,
            "title": "Congress trade: Cleo Fields bought NVDA",
            "summary": "Another row",
            "source_url": "https://example.com/d.pdf",
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-24T20:00:00+00:00",
        },
    ]
    later_compiled = compile_notification_events(later_events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    later_clusters = [event for event in later_compiled if event["signal_type"] == "politician_cluster"]
    assert len(later_clusters) == 2
    cluster_dates = {event["payload"]["cluster_clocked_at"] for event in later_clusters}
    assert cluster_dates == {"2026-03-18", "2026-03-24"}
    later_cluster = [event for event in later_clusters if event["payload"]["cluster_clocked_at"] == "2026-03-24"][0]
    assert later_cluster["source_document_id"] != initial_clusters[0]["source_document_id"]


def test_congress_cluster_keeps_older_window_when_latest_ticker_event_is_noise():
    events = [
        {
            "id": "early-1",
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
            "title": "Congress trade",
            "summary": "Early cluster leg",
            "source_url": "https://example.com/a.pdf",
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000"},
            "created_at": "2026-03-18T20:00:00+00:00",
        },
        {
            "id": "early-2",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034114-0",
            "ticker": "NVDA",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-19",
            "published_at": "2026-03-19",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Early cluster leg",
            "source_url": "https://example.com/b.pdf",
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-19T20:00:00+00:00",
        },
        {
            "id": "late-noise",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20039999-0",
            "ticker": "NVDA",
            "actor_name": "Single Later Member",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-04-20",
            "published_at": "2026-04-20",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Latest ticker event is not a cluster",
            "source_url": "https://example.com/c.pdf",
            "payload": {"member_id": "S000001", "amount_range": "$1,001 - $15,000"},
            "created_at": "2026-04-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "politician_cluster"]
    assert len(clusters) == 1
    assert clusters[0]["ticker"] == "NVDA"
    assert clusters[0]["payload"]["cluster_clocked_at"] == "2026-03-19"


def test_congress_cluster_skips_unpublishable_tickers():
    events = [
        {
            "id": "e1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034179-0",
            "ticker": "N/A",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-18",
            "published_at": "2026-03-18",
            "importance_score": 0.72,
            "title": "Congress trade: Thomas Kean bought N/A",
            "summary": "One row",
            "source_url": "https://example.com/a.pdf",
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "e2",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034114-0",
            "ticker": "N/A",
            "actor_name": "Cleo Fields",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-17",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade: Cleo Fields bought N/A",
            "summary": "One row",
            "source_url": "https://example.com/b.pdf",
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "politician_cluster"]
    assert len(clusters) == 0


def test_congress_cluster_uses_ticker_specific_latest_window():
    events = [
        {
            "id": "n1",
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
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000"},
            "created_at": "2026-03-18T20:00:00+00:00",
        },
        {
            "id": "n2",
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
            "payload": {"member_id": "F000470", "amount_range": "$15,001 - $50,000"},
            "created_at": "2026-03-17T20:00:00+00:00",
        },
        {
            "id": "a1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20039999-0",
            "ticker": "AAPL",
            "actor_name": "Single Newer Member",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-04-10",
            "published_at": "2026-04-10",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Unrelated newer ticker",
            "source_url": "https://example.com/c.pdf",
            "payload": {"member_id": "S000001", "amount_range": "$1,001 - $15,000"},
            "created_at": "2026-04-10T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, congress_cluster_window_days=7, congress_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "politician_cluster"]
    assert len(clusters) == 1
    assert clusters[0]["ticker"] == "NVDA"
    assert clusters[0]["payload"]["cluster_clocked_at"] == "2026-03-18"


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


def test_cross_source_accumulation_event_created():
    events = [
        {
            "id": "p1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034188-0",
            "ticker": "NVDA",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-10",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Congress buy",
            "source_url": "https://example.com/congress",
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000", "asset_type": "Stock"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "i1",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032683::NVDA",
            "ticker": "NVDA",
            "actor_name": "Jensen Huang",
            "actor_type": "insider",
            "direction": "buy",
            "occurred_at": "2026-03-11",
            "published_at": "2026-03-18",
            "importance_score": 0.81,
            "title": "Insider trade",
            "summary": "Insider buy",
            "source_url": "https://example.com/insider",
            "payload": {"filer_name": "Jensen Huang", "value": 5000000},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "f1",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "source_document_id": "berkshire::nvda::2025-12-31",
            "ticker": "NVDA",
            "actor_name": "Berkshire Hathaway Inc",
            "actor_type": "fund",
            "direction": "increase",
            "occurred_at": "2025-12-31",
            "published_at": "2026-02-14",
            "importance_score": 0.78,
            "title": "13F update",
            "summary": "Fund increase",
            "source_url": "https://example.com/fund",
            "payload": {"fund_name": "Berkshire Hathaway Inc", "qoq_change_percent": 22.5},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    clusters = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"]
    assert len(clusters) == 1
    full_stack = clusters[0]
    assert full_stack["payload"]["congress_actor_count"] == 1
    assert full_stack["payload"]["insider_actor_count"] == 1
    assert full_stack["payload"]["fund_actor_count"] == 1
    assert full_stack["payload"]["includes_fund_source"] is True
    assert full_stack["payload"]["cluster_combined_lower_bound"] == 5100001
    assert full_stack["payload"]["cluster_window_start"] == "2026-03-17"
    assert full_stack["source_document_id"] == "cross-source::NVDA::buy::30d::120fd::2026-03-17"


def test_cross_source_accumulation_allows_congress_fund_alignment():
    events = [
        {
            "id": "p1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034188-0",
            "ticker": "NVDA",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-17",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Congress buy",
            "source_url": "https://example.com/congress",
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000", "asset_type": "Stock"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "f1",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "source_document_id": "fund::nvda::2026-03-31",
            "ticker": "NVDA",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "direction": "increase",
            "occurred_at": "2026-03-31",
            "published_at": "2026-04-15",
            "importance_score": 0.78,
            "title": "13F update",
            "summary": "Fund increase",
            "source_url": "https://example.com/fund",
            "payload": {"fund_name": "Situational Awareness LP", "qoq_change_percent": 22.5},
            "created_at": "2026-04-15T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    clusters = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"]
    assert len(clusters) == 1
    assert clusters[0]["payload"]["congress_actor_count"] == 1
    assert clusters[0]["payload"]["insider_actor_count"] == 0
    assert clusters[0]["payload"]["fund_actor_count"] == 1
    assert clusters[0]["payload"]["cluster_sources"] == ["congress", "hedge_fund"]
    assert clusters[0]["payload"]["cluster_clocked_at"] == "2026-04-15"


def test_cross_source_counts_distinct_actors_not_transactions():
    events = [
        {
            "id": f"p{index}",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": f"house-2026-repeat-{index}",
            "ticker": "NVDA",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-17",
            "published_at": f"2026-03-{17 + index:02d}",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Repeated buy by one member",
            "source_url": f"https://example.com/congress-{index}",
            "payload": {"member_id": "K000398", "amount_range": "$1,001 - $15,000"},
            "created_at": f"2026-03-{17 + index:02d}T20:00:00+00:00",
        }
        for index in range(2)
    ]
    events.append(
        {
            "id": "f1",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "source_document_id": "fund::nvda::2026-03-31",
            "ticker": "NVDA",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "direction": "increase",
            "occurred_at": "2026-03-31",
            "published_at": "2026-04-15",
            "importance_score": 0.78,
            "title": "13F update",
            "summary": "Fund increase",
            "source_url": "https://example.com/fund",
            "payload": {"fund_name": "Situational Awareness LP", "qoq_change_percent": 22.5},
            "created_at": "2026-04-15T20:00:00+00:00",
        }
    )

    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    cluster = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"][0]
    assert cluster["payload"]["congress_actor_count"] == 1
    assert cluster["payload"]["fund_actor_count"] == 1
    assert cluster["payload"]["cluster_actor_count"] == 2


def test_cross_source_distribution_allows_congress_fund_alignment():
    events = [
        {
            "id": "p1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034188-0",
            "ticker": "PLTR",
            "actor_name": "John Hickenlooper",
            "actor_type": "politician",
            "direction": "sell",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-22",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Congress sell",
            "source_url": "https://example.com/congress",
            "payload": {"member_id": "H001077", "amount_range": "$15,001 - $50,000", "asset_type": "Stock"},
            "created_at": "2026-05-22T20:00:00+00:00",
        },
        {
            "id": "f1",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "source_document_id": "fund::pltr::2026-03-31",
            "ticker": "PLTR",
            "actor_name": "Citadel Advisors LLC",
            "actor_type": "fund",
            "direction": "decrease",
            "occurred_at": "2026-03-31",
            "published_at": "2026-05-15",
            "importance_score": 0.78,
            "title": "13F update",
            "summary": "Fund decrease",
            "source_url": "https://example.com/fund",
            "payload": {"fund_name": "Citadel Advisors LLC", "change_type": "decrease", "qoq_change_percent": -22.5},
            "created_at": "2026-05-15T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    clusters = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"]
    assert len(clusters) == 1
    assert clusters[0]["direction"] == "sell"
    assert clusters[0]["payload"]["congress_actor_count"] == 1
    assert clusters[0]["payload"]["fund_actor_count"] == 1
    assert clusters[0]["payload"]["cluster_sources"] == ["congress", "hedge_fund"]


def test_insider_cluster_skips_small_two_actor_noise():
    events = [
        {
            "id": "i1",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032683::ABC",
            "ticker": "ABC",
            "actor_name": "First Insider",
            "actor_type": "insider",
            "direction": "sell",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-21",
            "importance_score": 0.74,
            "title": "Insider trade",
            "summary": "Insider sell",
            "source_url": "https://example.com/insider1",
            "payload": {"filer_name": "First Insider", "value": 25000},
            "created_at": "2026-05-21T20:00:00+00:00",
        },
        {
            "id": "i2",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032684::ABC",
            "ticker": "ABC",
            "actor_name": "Second Insider",
            "actor_type": "insider",
            "direction": "sell",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-22",
            "importance_score": 0.74,
            "title": "Insider trade",
            "summary": "Insider sell",
            "source_url": "https://example.com/insider2",
            "payload": {"filer_name": "Second Insider", "value": 30000},
            "created_at": "2026-05-22T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, insider_cluster_window_days=10, insider_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "insider_cluster"]
    assert len(clusters) == 0


def test_repeated_transactions_from_one_insider_do_not_create_cluster():
    events = []
    for index in range(6):
        events.append(
            {
                "id": f"repeat-{index}",
                "source": "insider",
                "signal_type": "insider_trade",
                "source_document_id": f"repeat-accession-{index}::RPT",
                "ticker": "RPT",
                "actor_name": "Same Insider",
                "actor_type": "insider",
                "direction": "sell",
                "occurred_at": f"2026-05-{10 + index:02d}",
                "published_at": f"2026-05-{10 + index:02d}",
                "importance_score": 0.84,
                "title": "Insider trade",
                "summary": "Repeated sale",
                "source_url": f"https://example.com/repeat-{index}",
                "payload": {
                    "filer_name": "Same Insider",
                    "transaction_date": f"2026-05-{10 + index:02d}",
                    "transaction_code": "S",
                    "amount": 1000 + index,
                    "price": 10,
                    "value": 10000 + index,
                },
                "created_at": f"2026-05-{10 + index:02d}T20:00:00+00:00",
            }
        )

    compiled = compile_notification_events(events, insider_cluster_window_days=10)
    assert [event for event in compiled if event["signal_type"] == "insider_cluster"] == []


def test_related_form4_reporting_owners_do_not_inflate_insider_cluster():
    related_events = []
    for index, actor_name in enumerate(
        [
            "BCP Buzz Holdings L.P.",
            "BTOA - NQ L.L.C.",
            "BX Buzz ML-1 GP LLC",
            "BX Buzz ML-1 Holdco L.P.",
            "Blackstone Holdings III GP Management L.L.C.",
        ]
    ):
        related_events.append(
            {
                "id": f"bmbl-related-{index}",
                "source": "insider",
                "signal_type": "insider_trade",
                "source_document_id": f"0001193125-26-16{index:04d}::BMBL",
                "ticker": "BMBL",
                "actor_name": actor_name,
                "actor_type": "insider",
                "direction": "sell",
                "occurred_at": "2026-06-16",
                "published_at": "2026-06-18",
                "importance_score": 0.84,
                "title": "Insider trade",
                "summary": "Related Blackstone reporting owner sale",
                "source_url": f"https://www.sec.gov/Archives/edgar/data/0000000000/00011931252616{index:04d}/xslF345X05/form4.xml",
                "payload": {
                    "amount": 7477500,
                    "price": 3.7751,
                    "value": 28228310.25,
                    "transaction_date": "2026-06-16",
                    "transaction_code": "S",
                    "filer_relation": "10% Owner",
                },
                "created_at": "2026-06-18T20:00:00+00:00",
            }
        )

    compiled_without_distinct_leg = compile_notification_events(
        related_events,
        insider_cluster_window_days=10,
        insider_cluster_min_members=2,
    )
    assert [event for event in compiled_without_distinct_leg if event["signal_type"] == "insider_cluster"] == []

    distinct_event = {
        "id": "bmbl-distinct-1",
        "source": "insider",
        "signal_type": "insider_trade",
        "source_document_id": "0001193125-26-169999::BMBL",
        "ticker": "BMBL",
        "actor_name": "Independent Insider",
        "actor_type": "insider",
        "direction": "sell",
        "occurred_at": "2026-06-17",
        "published_at": "2026-06-18",
        "importance_score": 0.84,
        "title": "Insider trade",
        "summary": "Separate economic sale",
        "source_url": "https://www.sec.gov/Archives/edgar/data/0000000000/000119312526169999/xslF345X05/form4.xml",
        "payload": {
            "amount": 100000,
            "price": 20,
            "value": 2000000,
            "transaction_date": "2026-06-17",
            "transaction_code": "S",
            "filer_relation": "Director",
        },
        "created_at": "2026-06-18T21:00:00+00:00",
    }

    compiled = compile_notification_events(
        related_events + [distinct_event],
        insider_cluster_window_days=10,
        insider_cluster_min_members=2,
    )
    clusters = [event for event in compiled if event["signal_type"] == "insider_cluster"]
    assert clusters == []


def test_insider_cluster_keeps_older_material_window_when_latest_ticker_event_is_noise():
    events = [
        {
            "id": "early-i1",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032683::CRWV",
            "ticker": "CRWV",
            "actor_name": "First Insider",
            "actor_type": "insider",
            "direction": "sell",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-21",
            "importance_score": 0.74,
            "title": "Insider trade",
            "summary": "Early material cluster leg",
            "source_url": "https://example.com/insider1",
            "payload": {"filer_name": "First Insider", "value": 750000},
            "created_at": "2026-05-21T20:00:00+00:00",
        },
        {
            "id": "early-i2",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032684::CRWV",
            "ticker": "CRWV",
            "actor_name": "Second Insider",
            "actor_type": "insider",
            "direction": "sell",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-22",
            "importance_score": 0.74,
            "title": "Insider trade",
            "summary": "Early material cluster leg",
            "source_url": "https://example.com/insider2",
            "payload": {"filer_name": "Second Insider", "value": 500000},
            "created_at": "2026-05-22T20:00:00+00:00",
        },
        {
            "id": "late-i-noise",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032699::CRWV",
            "ticker": "CRWV",
            "actor_name": "Third Insider",
            "actor_type": "insider",
            "direction": "sell",
            "occurred_at": "2026-06-15",
            "published_at": "2026-06-16",
            "importance_score": 0.74,
            "title": "Insider trade",
            "summary": "Latest ticker event is not a cluster",
            "source_url": "https://example.com/insider3",
            "payload": {"filer_name": "Third Insider", "value": 10000},
            "created_at": "2026-06-16T20:00:00+00:00",
        },
    ]
    for index in range(3, 6):
        events.insert(
            -1,
            {
                "id": f"early-i{index}",
                "source": "insider",
                "signal_type": "insider_trade",
                "source_document_id": f"0001104659-26-03268{index}::CRWV",
                "ticker": "CRWV",
                "actor_name": f"Insider {index}",
                "actor_type": "insider",
                "direction": "sell",
                "occurred_at": "2026-05-20",
                "published_at": f"2026-05-{20 + index}",
                "importance_score": 0.78,
                "title": "Insider trade",
                "summary": "Early material cluster leg",
                "source_url": f"https://example.com/insider{index}",
                "payload": {"filer_name": f"Insider {index}", "value": 100000 + index},
                "created_at": f"2026-05-{20 + index}T20:00:00+00:00",
            },
        )
    compiled = compile_notification_events(events, insider_cluster_window_days=10, insider_cluster_min_members=2)
    clusters = [event for event in compiled if event["signal_type"] == "insider_cluster"]
    assert len(clusters) == 1
    assert clusters[0]["ticker"] == "CRWV"
    assert clusters[0]["payload"]["cluster_clocked_at"] == "2026-05-25"
    assert clusters[0]["payload"]["cluster_actor_count"] == 5
    assert clusters[0]["payload"]["cluster_window_start"] == "2026-05-21"


def test_cross_source_cluster_keeps_older_fund_alignment_window():
    events = [
        {
            "id": "p-early",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034188-0",
            "ticker": "AMD",
            "actor_name": "Josh Gottheimer",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-01",
            "published_at": "2026-03-01",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Congress buy",
            "source_url": "https://example.com/congress",
            "payload": {"member_id": "G000583", "amount_range": "$15,001 - $50,000", "asset_type": "Stock"},
            "created_at": "2026-03-01T20:00:00+00:00",
        },
        {
            "id": "f-early",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "source_document_id": "fund::amd::2026-03-31",
            "ticker": "AMD",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "direction": "increase",
            "occurred_at": "2026-03-31",
            "published_at": "2026-03-02",
            "importance_score": 0.78,
            "title": "13F update",
            "summary": "Fund increase",
            "source_url": "https://example.com/fund",
            "payload": {"fund_name": "Situational Awareness LP", "change_type": "increase", "qoq_change_percent": 22.5},
            "created_at": "2026-03-02T20:00:00+00:00",
        },
        {
            "id": "i-late",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032683::AMD",
            "ticker": "AMD",
            "actor_name": "Later Insider",
            "actor_type": "insider",
            "direction": "buy",
            "occurred_at": "2026-05-20",
            "published_at": "2026-05-20",
            "importance_score": 0.81,
            "title": "Insider trade",
            "summary": "Later source family on same ticker",
            "source_url": "https://example.com/insider",
            "payload": {"filer_name": "Later Insider", "value": 500000},
            "created_at": "2026-05-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    clusters = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"]
    cluster_dates = {event["payload"]["cluster_clocked_at"] for event in clusters}
    assert cluster_dates == {"2026-03-02", "2026-05-20"}
    early_cluster = [event for event in clusters if event["payload"]["cluster_clocked_at"] == "2026-03-02"][0]
    assert early_cluster["payload"]["cluster_sources"] == ["congress", "hedge_fund"]


def test_cross_source_accumulation_skips_unpublishable_tickers():
    events = [
        {
            "id": "p1",
            "source": "congress",
            "signal_type": "politician_trade",
            "source_document_id": "house-2026-20034188-0",
            "ticker": "N/A",
            "actor_name": "Thomas Kean",
            "actor_type": "politician",
            "direction": "buy",
            "occurred_at": "2026-03-10",
            "published_at": "2026-03-17",
            "importance_score": 0.72,
            "title": "Congress trade",
            "summary": "Congress buy",
            "source_url": "https://example.com/congress",
            "payload": {"member_id": "K000398", "amount_range": "$100,001 - $250,000", "asset_type": "Stock"},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
        {
            "id": "i1",
            "source": "insider",
            "signal_type": "insider_trade",
            "source_document_id": "0001104659-26-032683::NA",
            "ticker": "N/A",
            "actor_name": "Jensen Huang",
            "actor_type": "insider",
            "direction": "buy",
            "occurred_at": "2026-03-11",
            "published_at": "2026-03-18",
            "importance_score": 0.81,
            "title": "Insider trade",
            "summary": "Insider buy",
            "source_url": "https://example.com/insider",
            "payload": {"filer_name": "Jensen Huang", "value": 5000000},
            "created_at": "2026-03-20T20:00:00+00:00",
        },
    ]
    compiled = compile_notification_events(events, cross_source_window_days=30, fund_window_days=120)
    clusters = [event for event in compiled if event["signal_type"] == "cross_source_accumulation"]
    assert len(clusters) == 0


def main():
    test_grouped_same_filing_same_ticker()
    test_congress_cluster()
    test_congress_cluster_reappearing_later_gets_new_identity()
    test_congress_cluster_skips_unpublishable_tickers()
    test_congress_cluster_uses_ticker_specific_latest_window()
    test_insider_urls_with_same_accession_share_filing_group()
    test_actor_filing_summary_created()
    test_cross_source_accumulation_event_created()
    test_cross_source_accumulation_allows_congress_fund_alignment()
    test_cross_source_counts_distinct_actors_not_transactions()
    test_cross_source_distribution_allows_congress_fund_alignment()
    test_insider_cluster_skips_small_two_actor_noise()
    test_repeated_transactions_from_one_insider_do_not_create_cluster()
    test_related_form4_reporting_owners_do_not_inflate_insider_cluster()
    test_congress_cluster_keeps_older_window_when_latest_ticker_event_is_noise()
    test_insider_cluster_keeps_older_material_window_when_latest_ticker_event_is_noise()
    test_cross_source_cluster_keeps_older_fund_alignment_window()
    test_cross_source_accumulation_skips_unpublishable_tickers()
    print("notification compiler tests passed")


if __name__ == "__main__":
    main()
