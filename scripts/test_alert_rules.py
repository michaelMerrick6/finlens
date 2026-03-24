from alert_rules import classify_event_behavior, describe_behavior_reasons, follow_mode_matches, parse_amount_lower_bound


def test_parse_amount_lower_bound() -> None:
    assert parse_amount_lower_bound("$100,001 - $250,000") == 100001
    assert parse_amount_lower_bound("Over $1,000,000") == 1000000
    assert parse_amount_lower_bound(None) == 0


def test_non_clean_congress_asset_is_suppressed() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "US-TREAS",
        "actor_name": "Thomas Kean",
        "direction": "buy",
        "payload": {
            "asset_type": "Stock",
            "amount_range": "$50,001 - $100,000",
            "member_id": "K000398",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["suppressed"] is True
    assert "non_clean_asset" in behavior["reasons"]


def test_large_politician_trade_is_unusual() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "NVDA",
        "actor_name": "Thomas Kean",
        "direction": "buy",
        "payload": {
            "asset_type": "Stock",
            "amount_range": "$100,001 - $250,000",
            "member_id": "K000398",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "large_politician_trade" in behavior["reasons"]


def test_theme_ticker_uses_lower_unusual_threshold() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "IONQ",
        "actor_name": "Thomas Kean",
        "direction": "buy",
        "payload": {
            "asset_type": "Stock",
            "amount_range": "$50,001 - $100,000",
            "member_id": "K000398",
            "asset_name": "IONQ Inc. Class A Common Stock",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "quantum" in behavior["themes"]
    assert "priority_theme_ticker" in behavior["reasons"]


def test_committee_relevance_makes_mid_sized_trade_unusual() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "LMT",
        "actor_name": "Example Member",
        "direction": "buy",
        "payload": {
            "asset_type": "Stock",
            "amount_range": "$15,001 - $50,000",
            "member_id": "E000001",
            "asset_name": "Lockheed Martin Corporation",
            "member_committee_themes": ["defense"],
            "member_committee_roles": [{"name": "Committee on Armed Services"}],
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "committee_relevance" in behavior["reasons"]
    assert "defense" in behavior["committee_match_themes"]


def test_small_insider_sell_is_activity_only() -> None:
    event = {
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "SOFI",
        "actor_name": "Keough Kelli",
        "direction": "sell",
        "payload": {
            "value": 165066,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is False


def test_follow_mode_matches() -> None:
    behavior = {"activity": True, "unusual": False}
    assert follow_mode_matches("activity", behavior) is True
    assert follow_mode_matches("both", behavior) is True
    assert follow_mode_matches("unusual", behavior) is False


def test_filing_summary_behavior() -> None:
    event = {
        "signal_type": "politician_filing_summary",
        "payload": {
            "summary_contains_activity": True,
            "summary_contains_unusual": True,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "actor_filing_summary" in behavior["reasons"]


def test_behavior_reason_labels_are_human_readable() -> None:
    labels = describe_behavior_reasons(
        {
            "reasons": [
                "large_politician_trade",
                "committee_relevance",
                "priority_theme_ticker",
                "theme_quantum",
            ]
        }
    )
    assert "Large politician trade" in labels
    assert "Committee relevance" in labels
    assert "Priority theme ticker" in labels
    assert "Quantum" in labels


def main() -> None:
    test_parse_amount_lower_bound()
    test_non_clean_congress_asset_is_suppressed()
    test_large_politician_trade_is_unusual()
    test_theme_ticker_uses_lower_unusual_threshold()
    test_committee_relevance_makes_mid_sized_trade_unusual()
    test_small_insider_sell_is_activity_only()
    test_follow_mode_matches()
    test_filing_summary_behavior()
    test_behavior_reason_labels_are_human_readable()
    print("alert rules tests passed")


if __name__ == "__main__":
    main()
