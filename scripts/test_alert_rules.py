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


def test_first_quantum_position_is_unusual_even_when_small() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_trade",
        "ticker": "IONQ",
        "actor_name": "Greg Steube",
        "direction": "buy",
        "payload": {
            "asset_type": "ST",
            "asset_name": "IonQ, Inc. Common Stock",
            "member_id": "S001214",
            "amount_range": "$1,001 - $15,000",
            "is_first_congress_actor_ticker_buy": True,
            "is_first_congress_ticker_buy": True,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "new_quantum_position" in behavior["reasons"] or "first_quantum_congress_buy" in behavior["reasons"]


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


def test_substantial_insider_position_reduction_is_unusual() -> None:
    event = {
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "SOFI",
        "actor_name": "Keough Kelli",
        "direction": "sell",
        "payload": {
            "value": 165066,
            "insider_holding_reduction_pct": 0.61,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["unusual"] is True
    assert "substantial_insider_position_reduction" in behavior["reasons"]


def test_substantial_insider_position_increase_is_unusual() -> None:
    event = {
        "source": "insider",
        "signal_type": "insider_trade",
        "ticker": "NVDA",
        "actor_name": "Example Insider",
        "direction": "buy",
        "payload": {
            "value": 125000,
            "insider_holding_increase_pct": 0.75,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["unusual"] is True
    assert "substantial_insider_position_increase" in behavior["reasons"]


def test_cross_source_accumulation_is_unusual() -> None:
    event = {
        "source": "cross_source",
        "signal_type": "cross_source_accumulation",
        "ticker": "NVDA",
        "actor_name": "Thomas Kean, Jensen Huang",
        "payload": {
            "includes_fund_source": True,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "cross_source_accumulation" in behavior["reasons"]
    assert "cross_source_full_stack" in behavior["reasons"]


def test_politician_gain_milestone_is_unusual() -> None:
    event = {
        "source": "congress",
        "signal_type": "politician_gain_milestone",
        "ticker": "NVDA",
        "actor_name": "Nancy Pelosi",
        "direction": "buy",
        "payload": {
            "member_id": "P000197",
            "amount_range": "$100,001 - $250,000",
            "gain_return_pct": 118.4,
            "gain_milestone_pct": 100,
            "asset_type": "Stock",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "politician_gain_milestone" in behavior["reasons"]
    assert "triple_digit_gain" in behavior["reasons"]


def test_cluster_gain_milestone_is_unusual() -> None:
    event = {
        "source": "congress",
        "signal_type": "cluster_gain_milestone",
        "ticker": "MSFT",
        "actor_name": "Congress cluster",
        "direction": "buy",
        "payload": {
            "cluster_type": "politician_cluster",
            "cluster_actor_count": 3,
            "cluster_combined_lower_bound": 150001,
            "gain_return_pct": 58.2,
            "gain_milestone_pct": 50,
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["unusual"] is True
    assert "cluster_gain_milestone" in behavior["reasons"]
    assert "congress_cluster" in behavior["reasons"]


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


def test_fund_filing_deadline_reminder_is_activity() -> None:
    event = {
        "source": "hedge_fund",
        "signal_type": "fund_filing_deadline_reminder",
        "ticker": "13F",
        "actor_name": "Situational Awareness LP",
        "actor_type": "fund",
        "direction": "reminder",
        "payload": {
            "fund_name": "Situational Awareness LP",
            "report_period": "2026-06-30",
            "expected_filing_due_date": "2026-08-14",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["suppressed"] is False
    assert "fund_filing_deadline_reminder" in behavior["reasons"]


def test_fund_filing_received_is_activity() -> None:
    event = {
        "source": "hedge_fund",
        "signal_type": "fund_filing_received",
        "ticker": "13F",
        "actor_name": "Situational Awareness LP",
        "actor_type": "fund",
        "direction": "filed",
        "payload": {
            "fund_name": "Situational Awareness LP",
            "report_period": "2026-03-31",
            "filing_type": "13F-HR",
        },
    }
    behavior = classify_event_behavior(event)
    assert behavior["activity"] is True
    assert behavior["suppressed"] is False
    assert "fund_filing_received" in behavior["reasons"]


def test_behavior_reason_labels_are_human_readable() -> None:
    labels = describe_behavior_reasons(
        {
            "reasons": [
                "large_politician_trade",
                "committee_relevance",
                "new_quantum_position",
                "priority_theme_ticker",
                "theme_quantum",
            ]
        }
    )
    assert "Large politician trade" in labels
    assert "Committee relevance" in labels
    assert "New quantum position" in labels
    assert "Priority theme ticker" in labels
    assert "Quantum" in labels


def main() -> None:
    test_parse_amount_lower_bound()
    test_non_clean_congress_asset_is_suppressed()
    test_large_politician_trade_is_unusual()
    test_theme_ticker_uses_lower_unusual_threshold()
    test_first_quantum_position_is_unusual_even_when_small()
    test_committee_relevance_makes_mid_sized_trade_unusual()
    test_small_insider_sell_is_activity_only()
    test_substantial_insider_position_reduction_is_unusual()
    test_substantial_insider_position_increase_is_unusual()
    test_cross_source_accumulation_is_unusual()
    test_politician_gain_milestone_is_unusual()
    test_cluster_gain_milestone_is_unusual()
    test_follow_mode_matches()
    test_filing_summary_behavior()
    test_fund_filing_deadline_reminder_is_activity()
    test_fund_filing_received_is_activity()
    test_behavior_reason_labels_are_human_readable()
    print("alert rules tests passed")


if __name__ == "__main__":
    main()
