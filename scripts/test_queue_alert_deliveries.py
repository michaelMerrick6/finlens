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
        {
            "watchlist-1": {
                "user_id": "user-1",
                "channels": {"email"},
            }
        },
    )

    assert len(deliveries) == 1
    assert deliveries[0]["signal_event_id"] == "cluster-1"
    assert deliveries[0]["_cluster_alert_user_id"] == "user-1"
    assert deliveries[0]["_importance_score"] == 0.91
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
        {
            "watchlist-1": {
                "user_id": "user-1",
                "channels": {"email"},
            }
        },
    )

    assert len(deliveries) == 1
    assert deliveries[0]["channel"] == "email"


def test_capped_cluster_delivery_rpc_preserves_channels_and_limit() -> None:
    class FakeResponse:
        data = [
            {
                "deliveries_queued": 4,
                "cluster_events_reserved": 2,
                "cluster_events_suppressed": 1,
            }
        ]

        def execute(self):
            return self

    class FakeSupabase:
        rpc_name = ""
        rpc_params = {}

        def rpc(self, name, params):
            self.rpc_name = name
            self.rpc_params = params
            return FakeResponse()

    fake_supabase = FakeSupabase()
    module.CLUSTER_ALERT_DAILY_LIMIT = 5
    deliveries = [
        {
            "signal_event_id": "cluster-1",
            "subscription_id": "subscription-email",
            "delivery_key": "email-key",
            "channel": "email",
            "destination": "alerts@example.com",
            "status": "pending",
            "payload": {"reasons": ["watchlist_cluster_match"]},
            "_cluster_alert_user_id": "user-1",
            "_importance_score": 0.94,
        },
        {
            "signal_event_id": "cluster-1",
            "subscription_id": "subscription-sms",
            "delivery_key": "sms-key",
            "channel": "sms",
            "destination": "+15551234567",
            "status": "pending",
            "payload": {"reasons": ["watchlist_cluster_match"]},
            "_cluster_alert_user_id": "user-1",
            "_importance_score": 0.94,
        },
    ]

    result = module.queue_capped_cluster_deliveries(fake_supabase, deliveries)

    assert fake_supabase.rpc_name == "queue_cluster_alert_deliveries_capped"
    assert fake_supabase.rpc_params["p_daily_limit"] == 5
    assert len(fake_supabase.rpc_params["p_deliveries"]) == 2
    assert {row["channel"] for row in fake_supabase.rpc_params["p_deliveries"]} == {"email", "sms"}
    assert all(row["user_id"] == "user-1" for row in fake_supabase.rpc_params["p_deliveries"])
    assert all("_cluster_alert_user_id" not in row for row in fake_supabase.rpc_params["p_deliveries"])
    assert result == {
        "deliveries_queued": 4,
        "cluster_events_reserved": 2,
        "cluster_events_suppressed": 1,
    }


def test_politician_trade_matches_politician_follow_by_member_id() -> None:
    events = [
        {
            "id": "trade-1",
            "source": "congress",
            "signal_type": "politician_trade",
            "ticker": "NVDA",
            "actor_name": "Ro Khanna",
            "actor_type": "politician",
            "importance_score": 0.2,
            "payload": {
                "member_id": "K000389",
                "politician_name": "Ro Khanna",
                "asset_type": "Stock",
                "amount_range": "$1,001 - $15,000",
            },
        }
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
    watchlist_actors = {
        "politician:k000389": [
            {
                "watchlist_id": "watchlist-1",
                "match_type": "actor",
                "alert_mode": "activity",
                "actor_match_key": "politician:k000389",
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
    assert deliveries[0]["signal_event_id"] == "trade-1"
    assert (deliveries[0].get("payload") or {}).get("reasons") == ["watchlist_actor_match"]
    assert (deliveries[0].get("payload") or {}).get("matched_actor_keys") == ["politician:k000389"]


def test_politician_filing_summary_matches_politician_follow_by_member_id() -> None:
    events = [
        {
            "id": "summary-1",
            "source": "congress",
            "signal_type": "politician_filing_summary",
            "ticker": "MULTI",
            "actor_name": "Ro Khanna",
            "actor_type": "politician",
            "importance_score": 0.1,
            "payload": {
                "member_id": "K000389",
                "politician_name": "Ro Khanna",
                "base_signal_type": "politician_trade",
                "summary_contains_activity": True,
                "summary_event_ids": ["trade-1", "trade-2"],
            },
        },
        {
            "id": "trade-1",
            "source": "congress",
            "signal_type": "politician_trade",
            "ticker": "NVDA",
            "actor_name": "Ro Khanna",
            "actor_type": "politician",
            "importance_score": 0.2,
            "payload": {
                "member_id": "K000389",
                "politician_name": "Ro Khanna",
                "asset_type": "Stock",
                "amount_range": "$1,001 - $15,000",
            },
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
    watchlist_actors = {
        "politician:k000389": [
            {
                "watchlist_id": "watchlist-1",
                "match_type": "actor",
                "alert_mode": "activity",
                "actor_match_key": "politician:k000389",
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
    assert deliveries[0]["signal_event_id"] == "summary-1"
    assert (deliveries[0].get("payload") or {}).get("reasons") == ["watchlist_actor_match"]
    assert (deliveries[0].get("payload") or {}).get("matched_actor_keys") == ["politician:k000389"]


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
    test_capped_cluster_delivery_rpc_preserves_channels_and_limit()
    test_politician_trade_matches_politician_follow_by_member_id()
    test_politician_filing_summary_matches_politician_follow_by_member_id()
    test_fund_filing_reminder_matches_fund_follow()
    test_fund_filing_received_matches_fund_follow()
    test_fund_position_change_does_not_match_fund_follow()
    print("queue alert deliveries tests passed")


if __name__ == "__main__":
    main()
