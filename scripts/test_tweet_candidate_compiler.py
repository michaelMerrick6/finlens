from tweet_candidate_compiler import build_broadcast_candidates, build_tweet_candidates


def test_congress_cluster_candidate() -> None:
    event = {
        "id": "cluster-1",
        "source": "congress",
        "signal_type": "politician_cluster",
        "ticker": "NVDA",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.92,
        "payload": {
            "compiled_notification_event": True,
            "cluster_actor_count": 3,
            "cluster_window_days": 7,
            "cluster_actors": [
                {"name": "Nancy Pelosi", "amount_range": "$100,001 - $250,000"},
                {"name": "Josh Gottheimer", "amount_range": "$15,001 - $50,000"},
                {"name": "Thomas Kean", "amount_range": "$15,001 - $50,000"},
            ],
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    candidate = rows[0]
    assert candidate["rule_key"] == "congress_cluster"
    assert "NVDA" in candidate["draft_text"]
    assert "$100,001 - $250,000" in candidate["draft_text"]
    assert "Combined floor:" in candidate["draft_text"]


def test_compiled_insider_cluster_candidate() -> None:
    event = {
        "id": "insider-cluster-1",
        "source": "insider",
        "signal_type": "insider_cluster",
        "ticker": "AVBC",
        "direction": "buy",
        "published_at": "2026-04-30",
        "importance_score": 0.9,
        "payload": {
            "compiled_notification_event": True,
            "cluster_actor_count": 5,
            "cluster_window_days": 7,
            "cluster_total_value": 950000,
            "cluster_event_ids": ["a", "b", "c", "d", "e"],
            "cluster_actors": [
                {"name": "Alice Example", "relation": "Director"},
                {"name": "Bob Example", "relation": "CEO"},
                {"name": "Cara Example", "relation": "CFO"},
                {"name": "Dan Example", "relation": "Director"},
                {"name": "Eve Example", "relation": "Officer"},
            ],
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    candidate = rows[0]
    assert candidate["rule_key"] == "insider_cluster"
    assert "AVBC" in candidate["draft_text"]
    assert "Estimated total value: $950,000" in candidate["draft_text"]
    assert candidate["payload"]["cluster_event_ids"] == ["a", "b", "c", "d", "e"]


def test_broadcast_queue_ignores_legacy_compiled_insider_clusters() -> None:
    event = {
        "id": "legacy-insider-cluster",
        "source": "insider",
        "signal_type": "insider_cluster",
        "ticker": "AVBC",
        "direction": "buy",
        "published_at": "2026-04-30",
        "importance_score": 0.94,
        "payload": {
            "compiled_notification_event": True,
            "cluster_actor_count": 8,
            "cluster_window_days": 10,
            "cluster_actors": [{"name": f"Insider {index}"} for index in range(8)],
        },
    }

    rows = build_broadcast_candidates([event])

    assert rows == []


def test_broadcast_queue_keeps_canonical_compiled_insider_cluster() -> None:
    event = {
        "id": "canonical-insider-cluster",
        "source": "insider",
        "source_document_id": "insider-cluster::AVBC::buy::10d::2026-04-21",
        "signal_type": "insider_cluster",
        "ticker": "AVBC",
        "direction": "buy",
        "published_at": "2026-04-30",
        "importance_score": 0.94,
        "payload": {
            "compiled_notification_event": True,
            "cluster_actor_count": 5,
            "cluster_window_days": 10,
            "cluster_window_start": "2026-04-21",
            "cluster_actors": [{"name": f"Insider {index}"} for index in range(5)],
        },
    }

    rows = build_broadcast_candidates([event])

    assert {row["channel"] for row in rows} == {"twitter", "discord_premium"}
    assert {row["candidate_key"] for row in rows} == {
        "broadcast::insider_cluster::avbc::buy::2026-04-21"
    }


def test_notable_politician_trade_candidate() -> None:
    event = {
        "id": "pelosi-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "AAPL",
        "actor_name": "Nancy Pelosi",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.9,
        "payload": {
            "member_id": "P000197",
            "amount_range": "$15,001 - $50,000",
            "asset_type": "Stock",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "notable_politician_trade"


def test_committee_relevance_candidate() -> None:
    event = {
        "id": "committee-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "RTX",
        "actor_name": "Markwayne Mullin",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.9,
        "payload": {
            "member_id": "M001190",
            "amount_range": "$15,001 - $50,000",
            "asset_type": "Stock",
            "member_committee_themes": ["defense"],
            "member_committee_roles": [{"name": "Committee on Armed Services"}],
            "asset_name": "RTX Corporation",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "committee_relevance_buy"


def test_first_quantum_politician_buy_candidate() -> None:
    event = {
        "id": "quantum-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "IONQ",
        "actor_name": "Greg Steube",
        "direction": "buy",
        "occurred_at": "2026-03-18",
        "published_at": "2026-04-14",
        "importance_score": 0.72,
        "payload": {
            "member_id": "S001214",
            "amount_range": "$1,001 - $15,000",
            "asset_type": "ST",
            "asset_name": "IonQ, Inc. Common Stock",
            "transaction_date": "2026-03-18",
            "is_first_congress_ticker_buy": True,
            "is_first_congress_actor_ticker_buy": True,
            "prior_congress_ticker_buy_count": 0,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "first_quantum_politician_buy"
    assert "Theme: Quantum" in rows[0]["draft_text"]
    assert "Trade date: Mar 18, 2026" in rows[0]["draft_text"]


def test_large_politician_buy_candidate() -> None:
    event = {
        "id": "large-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "MSFT",
        "actor_name": "Thomas Kean",
        "direction": "buy",
        "occurred_at": "2026-03-12",
        "published_at": "2026-03-20",
        "importance_score": 0.9,
        "payload": {
            "member_id": "K000398",
            "amount_range": "$100,001 - $250,000",
            "asset_type": "Stock",
            "transaction_date": "2026-03-12",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "large_politician_buy"
    assert rows[0]["payload"]["trade_date"] == "2026-03-12"
    assert "Trade date: Mar 12, 2026" in rows[0]["draft_text"]
    assert "Filed: Mar 20, 2026" in rows[0]["draft_text"]


def test_grouped_insider_buy_candidate() -> None:
    event = {
        "id": "group-1",
        "source": "insider",
        "signal_type": "insider_trade_grouped",
        "ticker": "BHM",
        "actor_name": "Kamfar Ramin",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.9,
        "payload": {
            "compiled_notification_event": True,
            "group_row_count": 2,
            "group_combined_lower_bound": 525000,
            "group_trade_date_start": "2026-03-18",
            "group_trade_date_end": "2026-03-19",
            "group_event_ids": ["group-1a", "group-1b"],
            "asset_name": "Rigetti Computing, Inc.",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "grouped_insider_buy"
    assert rows[0]["payload"]["group_combined_lower_bound"] == 525000
    assert rows[0]["payload"]["group_event_ids"] == ["group-1a", "group-1b"]
    assert "Combined value: $525,000" in rows[0]["draft_text"]
    assert "Trade dates:" in rows[0]["draft_text"]


def test_grouped_congress_buy_candidate_includes_ranges() -> None:
    event = {
        "id": "group-congress-1",
        "source": "congress",
        "signal_type": "politician_trade_grouped",
        "ticker": "DWX",
        "actor_name": "Michael T. McCaul",
        "direction": "buy",
        "published_at": "2026-04-08",
        "importance_score": 0.94,
        "payload": {
            "compiled_notification_event": True,
            "group_row_count": 5,
            "group_combined_lower_bound": 50001,
            "group_amount_ranges": ["$1,001 - $15,000", "$15,001 - $50,000"],
            "group_trade_date_start": "2026-03-24",
            "group_trade_date_end": "2026-03-27",
            "group_event_ids": ["group-1", "group-2", "group-3", "group-4", "group-5"],
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "grouped_congress_buy"
    assert "Disclosed ranges: $1,001 - $15,000, $15,001 - $50,000" in rows[0]["draft_text"]
    assert "Combined floor: $50,001+" in rows[0]["draft_text"]
    assert "Trade dates:" in rows[0]["draft_text"]


def test_grouped_sell_is_not_candidate() -> None:
    event = {
        "id": "group-2",
        "source": "congress",
        "signal_type": "politician_trade_grouped",
        "ticker": "MSFT",
        "actor_name": "Cleo Fields",
        "direction": "sell",
        "published_at": "2026-03-20",
        "importance_score": 0.95,
        "payload": {
            "compiled_notification_event": True,
            "group_row_count": 3,
        },
    }
    rows = build_tweet_candidates([event])
    assert rows == []


def test_semantic_candidate_dedupes_alternate_sec_paths() -> None:
    base_payload = {
        "compiled_notification_event": True,
        "group_row_count": 5,
        "asset_name": "Rigetti Computing, Inc.",
    }
    events = [
        {
            "id": "dup-1",
            "source": "insider",
            "signal_type": "insider_trade_grouped",
            "ticker": "RGTI",
            "actor_name": "Stahl Murray",
            "direction": "buy",
            "published_at": "2026-03-19",
            "importance_score": 0.8,
            "source_document_id": "https://www.sec.gov/Archives/edgar/data/919567/000143774926009184/0001437749-26-009184.txt::group::RGTI::buy::stahlmurray",
            "payload": dict(base_payload),
        },
        {
            "id": "dup-2",
            "source": "insider",
            "signal_type": "insider_trade_grouped",
            "ticker": "RGTI",
            "actor_name": "Stahl Murray",
            "direction": "buy",
            "published_at": "2026-03-19",
            "importance_score": 0.8,
            "source_document_id": "0001437749-26-009184::group::RGTI::buy::stahlmurray",
            "payload": dict(base_payload),
        },
    ]
    rows = build_tweet_candidates(events, minimum_importance=0.8)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "grouped_insider_buy"


def test_large_politician_buy_is_not_blocked_by_generic_score_floor() -> None:
    event = {
        "id": "large-floor-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "LBRDK",
        "actor_name": "John W Hickenlooper",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.82,
        "payload": {
            "member_id": "H000273",
            "amount_range": "$250,001 - $500,000",
            "asset_type": "Stock",
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "large_politician_buy"


def test_substantial_insider_sell_candidate() -> None:
    event = {
        "id": "sell-1",
        "source": "insider",
        "signal_type": "insider_trade_grouped",
        "ticker": "VICR",
        "actor_name": "Vinciarelli Patrizio",
        "direction": "sell",
        "published_at": "2026-03-20",
        "importance_score": 0.6,
        "payload": {
            "group_row_count": 4,
            "insider_holding_reduction_pct": 0.24,
            "insider_total_sell_value": 2500000,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "substantial_insider_sell"


def test_substantial_insider_buy_candidate() -> None:
    event = {
        "id": "buy-1",
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "NVDA",
        "actor_name": "Example Insider",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.7,
        "payload": {
            "insider_holding_increase_pct": 0.8,
            "insider_total_buy_value": 750000,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "substantial_insider_buy"


def test_cross_source_accumulation_candidate() -> None:
    event = {
        "id": "align-1",
        "source": "cross_source",
        "signal_type": "cross_source_accumulation",
        "ticker": "NVDA",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.94,
        "payload": {
            "compiled_notification_event": True,
            "congress_actor_count": 2,
            "insider_actor_count": 2,
            "fund_actor_count": 1,
            "includes_fund_source": True,
            "cluster_actors": [
                {"name": "Thomas Kean", "source": "congress", "amount_range": "$100,001 - $250,000"},
                {"name": "Cleo Fields", "source": "congress", "amount_range": "$50,001 - $100,000"},
                {"name": "Jensen Huang"},
            ],
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "cross_source_accumulation"
    assert "Congress floor:" in rows[0]["draft_text"]


def test_politician_gain_milestone_candidate() -> None:
    event = {
        "id": "gain-1",
        "source": "congress",
        "signal_type": "politician_gain_milestone",
        "ticker": "PLTR",
        "actor_name": "Nancy Pelosi",
        "direction": "buy",
        "published_at": "2026-04-14",
        "importance_score": 0.94,
        "payload": {
            "member_id": "P000197",
            "amount_range": "$100,001 - $250,000",
            "gain_return_pct": 118.4,
            "gain_milestone_pct": 100,
            "entry_price": 22.14,
            "current_price": 48.35,
            "holding_days": 95,
            "trade_date": "2026-01-10",
            "price_as_of": "2026-04-14",
            "estimated_gain_lower_bound": 118401,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "politician_gain_milestone"
    assert "Disclosed range:" in rows[0]["draft_text"]
    assert "Entry / current:" in rows[0]["draft_text"]
    assert "Estimated gain floor:" in rows[0]["draft_text"]
    assert "Window:" in rows[0]["draft_text"]


def test_politician_gain_milestone_does_not_become_large_buy() -> None:
    event = {
        "id": "gain-2",
        "source": "congress",
        "signal_type": "politician_gain_milestone",
        "ticker": "INTC",
        "actor_name": "Michael T. McCaul",
        "direction": "buy",
        "occurred_at": "2026-04-14",
        "published_at": "2026-04-14",
        "importance_score": 0.91,
        "payload": {
            "base_signal_type": "politician_trade",
            "amount_range": "$250,001 - $500,000",
            "gain_return_pct": 66.26,
            "gain_milestone_pct": 50,
            "entry_price": 38.38,
            "current_price": 63.81,
            "holding_days": 160,
            "trade_date": "2025-11-05",
            "price_as_of": "2026-04-14",
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "politician_gain_milestone"
    assert "Trade date: Nov 5, 2025" in rows[0]["draft_text"]
    assert "Large Congress buy" not in rows[0]["title"]


def test_cluster_gain_milestone_candidate() -> None:
    event = {
        "id": "cluster-gain-1",
        "source": "congress",
        "signal_type": "cluster_gain_milestone",
        "ticker": "MSFT",
        "actor_name": "Congress cluster",
        "direction": "buy",
        "published_at": "2026-04-14",
        "importance_score": 0.95,
        "payload": {
            "cluster_type": "politician_cluster",
            "cluster_actor_count": 3,
            "cluster_combined_lower_bound": 567005,
            "congress_actor_count": 3,
            "gain_return_pct": 54.2,
            "gain_milestone_pct": 50,
            "entry_price": 390.25,
            "current_price": 601.76,
            "days_since_cluster": 96,
            "cluster_clocked_at": "2026-01-08",
            "price_as_of": "2026-04-14",
            "estimated_gain_lower_bound": 307316,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "cluster_gain_milestone"
    assert rows[0]["payload"]["broadcast_category"] == "updates"
    assert "Tracked cluster floor:" in rows[0]["draft_text"]
    assert "Estimated gain floor:" in rows[0]["draft_text"]
    assert "Cluster date:" in rows[0]["draft_text"]
    assert "Window:" in rows[0]["draft_text"]


def test_entity_style_insider_sell_is_filtered() -> None:
    event = {
        "id": "sell-entity-1",
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "DELL",
        "actor_name": "Slta Iv (Gp), L.L.C.",
        "direction": "sell",
        "published_at": "2026-03-20",
        "importance_score": 0.6,
        "payload": {
            "insider_holding_reduction_pct": 0.35,
            "insider_total_sell_value": 5000000,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert rows == []


def test_broadcast_candidates_expand_to_x_and_discord() -> None:
    event = {
        "id": "cluster-2",
        "source": "congress",
        "signal_type": "politician_cluster",
        "ticker": "PYPL",
        "direction": "sell",
        "published_at": "2026-03-24",
        "importance_score": 0.84,
        "payload": {
            "compiled_notification_event": True,
            "cluster_actor_count": 2,
            "cluster_window_days": 7,
            "cluster_actors": [
                {"name": "Chuck Fleischmann"},
                {"name": "Angus S King, Jr."},
            ],
        },
    }
    rows = build_broadcast_candidates([event], minimum_importance=0.88)
    channels = {row["channel"] for row in rows}
    assert channels == {"twitter", "discord_premium"}


def test_meaningful_insider_change_candidate_added_at_25_percent() -> None:
    event = {
        "id": "insider-change-1",
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "SHOP",
        "actor_name": "Example Insider",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.7,
        "payload": {
            "insider_holding_increase_pct": 0.3,
            "insider_total_buy_value": 400000,
        },
    }
    rows = build_tweet_candidates([event], minimum_importance=0.88)
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "meaningful_insider_change"
    assert rows[0]["score"] < 0.7


def test_insider_cluster_suppresses_individual_broadcasts() -> None:
    events = [
        {
            "id": "jan-1",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Arabia John V",
            "direction": "buy",
            "published_at": "2026-03-24",
            "importance_score": 0.8,
            "payload": {
                "insider_holding_increase_pct": 8.0,
                "insider_total_buy_value": 1200000,
            },
        },
        {
            "id": "jan-2",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Brinker Scott M",
            "direction": "buy",
            "published_at": "2026-03-24",
            "importance_score": 0.8,
            "payload": {
                "insider_new_position_after_buy": True,
                "insider_total_buy_value": 2000000,
            },
        },
    ]
    for index in range(3, 6):
        events.append(
            {
                "id": f"jan-{index}",
                "source": "insider",
                "signal_type": "insider_trade",
                "ticker": "JAN",
                "actor_name": f"Example Insider {index}",
                "direction": "buy",
                "published_at": "2026-03-24",
                "importance_score": 0.8,
                "payload": {
                    "insider_holding_increase_pct": 0.5,
                    "insider_total_buy_value": 1000000 + index,
                },
            }
        )
    rows = build_broadcast_candidates(events, minimum_importance=0.88)
    rule_keys = {row["rule_key"] for row in rows}
    assert "insider_cluster" in rule_keys
    assert "substantial_insider_buy" not in rule_keys
    assert len({row["candidate_key"] for row in rows if row["rule_key"] == "insider_cluster"}) == 1


def test_insider_cluster_finds_multiple_windows_for_same_ticker() -> None:
    events = [
        {
            "id": "jan-window-1",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Arabia John V",
            "direction": "buy",
            "published_at": "2026-03-01",
            "importance_score": 0.8,
            "payload": {
                "insider_holding_increase_pct": 0.8,
                "insider_total_buy_value": 900000,
            },
        },
        {
            "id": "jan-window-2",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Brinker Scott M",
            "direction": "buy",
            "published_at": "2026-03-03",
            "importance_score": 0.82,
            "payload": {
                "insider_new_position_after_buy": True,
                "insider_total_buy_value": 1200000,
            },
        },
        {
            "id": "jan-window-3",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Sandstrom Katherine M",
            "direction": "buy",
            "published_at": "2026-03-20",
            "importance_score": 0.84,
            "payload": {
                "insider_new_position_after_buy": True,
                "insider_total_buy_value": 1500000,
            },
        },
        {
            "id": "jan-window-4",
            "source": "insider",
            "signal_type": "insider_trade",
            "ticker": "JAN",
            "actor_name": "Ward Example",
            "direction": "buy",
            "published_at": "2026-03-24",
            "importance_score": 0.79,
            "payload": {
                "insider_holding_increase_pct": 0.5,
                "insider_total_buy_value": 700000,
            },
        },
    ]
    for window_name, published_at, start_value in (
        ("early", "2026-03-05", 2000000),
        ("late", "2026-03-25", 3000000),
    ):
        for index in range(3):
            events.append(
                {
                    "id": f"jan-window-{window_name}-{index}",
                    "source": "insider",
                    "signal_type": "insider_trade",
                    "ticker": "JAN",
                    "actor_name": f"{window_name.title()} Insider {index}",
                    "direction": "buy",
                    "published_at": published_at,
                    "importance_score": 0.82,
                    "payload": {
                        "insider_holding_increase_pct": 0.5,
                        "insider_total_buy_value": start_value + index,
                    },
                }
            )
    rows = build_broadcast_candidates(events, minimum_importance=0.88)
    cluster_keys = {row["candidate_key"] for row in rows if row["rule_key"] == "insider_cluster"}
    assert len(cluster_keys) == 2


def test_related_form4_reporting_owners_do_not_create_fake_cluster() -> None:
    events = []
    for index, actor_name in enumerate(
        [
            "BCP Buzz Holdings L.P.",
            "BTOA - NQ L.L.C.",
            "BX Buzz ML-1 GP LLC",
            "BX Buzz ML-1 Holdco L.P.",
            "Blackstone Holdings III GP Management L.L.C.",
        ]
    ):
        events.append(
            {
                "id": f"bmbl-related-{index}",
                "source": "insider",
                "signal_type": "insider_trade_grouped",
                "ticker": "BMBL",
                "actor_name": actor_name,
                "direction": "sell",
                "occurred_at": "2026-06-16",
                "published_at": "2026-06-18",
                "importance_score": 0.84,
                "payload": {
                    "group_row_count": 7,
                    "group_combined_lower_bound": 28228310.0,
                    "group_trade_date_start": "2026-06-16",
                    "group_trade_date_end": "2026-06-16",
                    "insider_holding_reduction_pct": 0.5,
                    "value": 9419587.99,
                },
            }
        )

    rows = build_broadcast_candidates(events, minimum_importance=0.88)
    assert "insider_cluster" not in {row["rule_key"] for row in rows}


def main() -> None:
    test_congress_cluster_candidate()
    test_compiled_insider_cluster_candidate()
    test_broadcast_queue_ignores_legacy_compiled_insider_clusters()
    test_broadcast_queue_keeps_canonical_compiled_insider_cluster()
    test_notable_politician_trade_candidate()
    test_committee_relevance_candidate()
    test_first_quantum_politician_buy_candidate()
    test_large_politician_buy_candidate()
    test_grouped_insider_buy_candidate()
    test_grouped_congress_buy_candidate_includes_ranges()
    test_grouped_sell_is_not_candidate()
    test_semantic_candidate_dedupes_alternate_sec_paths()
    test_large_politician_buy_is_not_blocked_by_generic_score_floor()
    test_substantial_insider_sell_candidate()
    test_substantial_insider_buy_candidate()
    test_cross_source_accumulation_candidate()
    test_politician_gain_milestone_candidate()
    test_politician_gain_milestone_does_not_become_large_buy()
    test_cluster_gain_milestone_candidate()
    test_entity_style_insider_sell_is_filtered()
    test_broadcast_candidates_expand_to_x_and_discord()
    test_meaningful_insider_change_candidate_added_at_25_percent()
    test_insider_cluster_suppresses_individual_broadcasts()
    test_insider_cluster_finds_multiple_windows_for_same_ticker()
    test_related_form4_reporting_owners_do_not_create_fake_cluster()
    print("tweet candidate compiler tests passed")


if __name__ == "__main__":
    main()
