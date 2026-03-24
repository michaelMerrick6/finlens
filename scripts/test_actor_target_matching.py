from notification_targets import event_actor_match_keys


def test_grouped_insider_event_matches_name():
    event = {
        "actor_type": "insider",
        "actor_name": "Kamfar Ramin",
        "payload": {
            "filer_name": "Kamfar Ramin",
            "compiled_notification_event": True,
            "base_signal_type": "insider_trade",
            "group_row_count": 2,
        },
    }
    keys = event_actor_match_keys(event)
    assert "insider:kamfarramin" in keys


def test_cluster_event_matches_politician_members():
    event = {
        "actor_type": "cluster",
        "actor_name": "Thomas Kean, Cleo Fields",
        "payload": {
            "compiled_notification_event": True,
            "base_signal_type": "politician_trade",
            "cluster_actors": [
                {"name": "Thomas Kean", "member_id": "K000398"},
                {"name": "Cleo Fields", "member_id": "F000470"},
            ],
        },
    }
    keys = event_actor_match_keys(event)
    assert "politician:k000398" in keys
    assert "politician:f000470" in keys
    assert "politician:thomaskean" in keys
    assert "politician:cleofields" in keys


def main():
    test_grouped_insider_event_matches_name()
    test_cluster_event_matches_politician_members()
    print("actor target matching tests passed")


if __name__ == "__main__":
    main()
