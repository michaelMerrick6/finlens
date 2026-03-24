from tweet_candidate_compiler import build_tweet_candidates


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
                {"name": "Nancy Pelosi"},
                {"name": "Josh Gottheimer"},
                {"name": "Thomas Kean"},
            ],
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    candidate = rows[0]
    assert candidate["rule_key"] == "congress_cluster"
    assert "NVDA" in candidate["draft_text"]


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


def test_large_politician_buy_candidate() -> None:
    event = {
        "id": "large-1",
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "MSFT",
        "actor_name": "Thomas Kean",
        "direction": "buy",
        "published_at": "2026-03-20",
        "importance_score": 0.9,
        "payload": {
            "member_id": "K000398",
            "amount_range": "$100,001 - $250,000",
            "asset_type": "Stock",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "large_politician_buy"


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
            "asset_name": "Rigetti Computing, Inc.",
        },
    }
    rows = build_tweet_candidates([event])
    assert len(rows) == 1
    assert rows[0]["rule_key"] == "grouped_insider_buy"


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


def main() -> None:
    test_congress_cluster_candidate()
    test_notable_politician_trade_candidate()
    test_committee_relevance_candidate()
    test_large_politician_buy_candidate()
    test_grouped_insider_buy_candidate()
    test_grouped_sell_is_not_candidate()
    test_semantic_candidate_dedupes_alternate_sec_paths()
    test_large_politician_buy_is_not_blocked_by_generic_score_floor()
    test_substantial_insider_sell_candidate()
    test_entity_style_insider_sell_is_filtered()
    print("tweet candidate compiler tests passed")


if __name__ == "__main__":
    main()
