import queue_alert_deliveries as module


def test_queue_owner_sms_signal_deliveries() -> None:
    module.SMS_CLUSTER_PHONE = "+15551234567"
    module.SMS_CLUSTER_MIN_IMPORTANCE = 0.84

    events = [
        {
            "id": "cluster-1",
            "signal_type": "politician_cluster",
            "importance_score": 0.91,
            "payload": {"cluster_actor_count": 3},
        },
        {
            "id": "cross-1",
            "signal_type": "cross_source_accumulation",
            "importance_score": 0.9,
            "payload": {"cluster_actor_count": 2},
        },
        {
            "id": "small-1",
            "signal_type": "politician_cluster",
            "importance_score": 0.7,
            "payload": {"cluster_actor_count": 2},
        },
        {
            "id": "plain-1",
            "signal_type": "politician_trade",
            "importance_score": 0.95,
            "payload": {},
        },
        {
            "id": "gain-1",
            "signal_type": "politician_gain_milestone",
            "importance_score": 0.9,
            "payload": {"gain_milestone_pct": 100},
        },
        {
            "id": "cluster-gain-1",
            "signal_type": "cluster_gain_milestone",
            "importance_score": 0.91,
            "payload": {"gain_milestone_pct": 50},
        },
    ]

    deliveries = module.queue_owner_sms_signal_deliveries(events)
    assert len(deliveries) == 4
    assert {row["signal_event_id"] for row in deliveries} == {"cluster-1", "cross-1", "gain-1", "cluster-gain-1"}
    assert all(row["channel"] == "sms" for row in deliveries)
    assert all(row["destination"] == "+15551234567" for row in deliveries)
    assert all((row.get("payload") or {}).get("reason") == "owner_signal_sms" for row in deliveries)


def test_cluster_alert_watchlist_queues_clusters_without_individual_follow() -> None:
    events = [
        {
            "id": "cluster-1",
            "signal_type": "politician_cluster",
            "importance_score": 0.91,
            "payload": {"cluster_actor_count": 3},
        },
        {
            "id": "plain-1",
            "signal_type": "politician_trade",
            "importance_score": 0.95,
            "payload": {},
        },
    ]
    subscriptions = [
        {
            "id": "subscription-1",
            "watchlist_id": "watchlist-1",
            "channel": "email",
            "destination": "alerts@example.com",
            "minimum_importance": 0.75,
        }
    ]

    deliveries = module.queue_subscription_deliveries(
        events,
        subscriptions,
        {},
        {},
        {"watchlist-1": {"email"}},
    )

    assert len(deliveries) == 1
    assert deliveries[0]["signal_event_id"] == "cluster-1"
    assert (deliveries[0].get("payload") or {}).get("reasons") == ["watchlist_cluster_match"]


def test_cluster_alert_watchlist_only_queues_selected_channels() -> None:
    events = [
        {
            "id": "cluster-1",
            "signal_type": "politician_cluster",
            "importance_score": 0.91,
            "payload": {"cluster_actor_count": 3},
        },
    ]
    subscriptions = [
        {
            "id": "subscription-email",
            "watchlist_id": "watchlist-1",
            "channel": "email",
            "destination": "alerts@example.com",
            "minimum_importance": 0.75,
        },
        {
            "id": "subscription-sms",
            "watchlist_id": "watchlist-1",
            "channel": "sms",
            "destination": "+15551234567",
            "minimum_importance": 0.75,
        },
    ]

    deliveries = module.queue_subscription_deliveries(
        events,
        subscriptions,
        {},
        {},
        {"watchlist-1": {"email"}},
    )

    assert len(deliveries) == 1
    assert deliveries[0]["channel"] == "email"


def test_fund_filing_reminder_matches_fund_follow() -> None:
    events = [
        {
            "id": "fund-reminder-1",
            "source": "hedge_fund",
            "signal_type": "fund_filing_deadline_reminder",
            "ticker": "13F",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "importance_score": 0.64,
            "payload": {
                "fund_name": "Situational Awareness LP",
                "report_period": "2026-06-30",
                "expected_filing_due_date": "2026-08-14",
            },
        }
    ]
    subscriptions = [
        {
            "id": "subscription-1",
            "watchlist_id": "watchlist-1",
            "channel": "email",
            "destination": "alerts@example.com",
            "minimum_importance": 0.5,
        }
    ]
    watchlist_actors = {
        "fund:situationalawarenesslp": [
            {
                "watchlist_id": "watchlist-1",
                "match_type": "actor",
                "alert_mode": "activity",
                "actor_match_key": "fund:situationalawarenesslp",
            }
        ]
    }

    deliveries = module.queue_subscription_deliveries(
        events,
        subscriptions,
        {},
        watchlist_actors,
        {},
    )

    assert len(deliveries) == 1
    assert deliveries[0]["signal_event_id"] == "fund-reminder-1"
    assert (deliveries[0].get("payload") or {}).get("reasons") == ["watchlist_actor_match"]


def test_fund_filing_received_matches_fund_follow() -> None:
    events = [
        {
            "id": "fund-filing-1",
            "source": "hedge_fund",
            "signal_type": "fund_filing_received",
            "ticker": "13F",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "importance_score": 0.72,
            "payload": {
                "fund_name": "Situational Awareness LP",
                "report_period": "2026-03-31",
                "filing_type": "13F-HR",
            },
        }
    ]
    subscriptions = [
        {
            "id": "subscription-1",
            "watchlist_id": "watchlist-1",
            "channel": "email",
            "destination": "alerts@example.com",
            "minimum_importance": 0.5,
        }
    ]
    watchlist_actors = {
        "fund:situationalawarenesslp": [
            {
                "watchlist_id": "watchlist-1",
                "match_type": "actor",
                "alert_mode": "activity",
                "actor_match_key": "fund:situationalawarenesslp",
            }
        ]
    }

    deliveries = module.queue_subscription_deliveries(
        events,
        subscriptions,
        {},
        watchlist_actors,
        {},
    )

    assert len(deliveries) == 1
    assert deliveries[0]["signal_event_id"] == "fund-filing-1"
    assert (deliveries[0].get("payload") or {}).get("reasons") == ["watchlist_actor_match"]


def test_fund_position_change_does_not_match_fund_follow() -> None:
    events = [
        {
            "id": "fund-position-1",
            "source": "hedge_fund",
            "signal_type": "fund_position_change",
            "ticker": "NVDA",
            "actor_name": "Situational Awareness LP",
            "actor_type": "fund",
            "importance_score": 0.82,
            "payload": {
                "fund_name": "Situational Awareness LP",
                "change_type": "new",
            },
        }
    ]
    subscriptions = [
        {
            "id": "subscription-1",
            "watchlist_id": "watchlist-1",
            "channel": "email",
            "destination": "alerts@example.com",
            "minimum_importance": 0.5,
        }
    ]
    watchlist_actors = {
        "fund:situationalawarenesslp": [
            {
                "watchlist_id": "watchlist-1",
                "match_type": "actor",
                "alert_mode": "activity",
                "actor_match_key": "fund:situationalawarenesslp",
            }
        ]
    }

    deliveries = module.queue_subscription_deliveries(
        events,
        subscriptions,
        {},
        watchlist_actors,
        {},
    )

    assert len(deliveries) == 0


def main() -> None:
    test_queue_owner_sms_signal_deliveries()
    test_cluster_alert_watchlist_queues_clusters_without_individual_follow()
    test_cluster_alert_watchlist_only_queues_selected_channels()
    test_fund_filing_reminder_matches_fund_follow()
    test_fund_filing_received_matches_fund_follow()
    test_fund_position_change_does_not_match_fund_follow()
    print("queue alert deliveries tests passed")


if __name__ == "__main__":
    main()
