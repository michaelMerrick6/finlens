import hashlib
import os
from collections import defaultdict
from datetime import timedelta

from alert_rules import classify_event_behavior, follow_mode_matches
from congress_trade_history import enrich_events_with_congress_buy_history
from congress_relevance import enrich_events_with_member_roles
from insider_holdings import enrich_events_with_insider_position_changes
from notification_targets import event_actor_match_keys
from notification_compiler import is_compiled_notification_event
from pipeline_support import emit_summary, get_supabase_client, utc_now


LOOKBACK_HOURS = int(os.environ.get("ALERT_QUEUE_LOOKBACK_HOURS", "48"))
GLOBAL_MIN_IMPORTANCE = float(os.environ.get("DISCORD_GLOBAL_MIN_IMPORTANCE", "0.8"))
GLOBAL_WEBHOOK_URL = os.environ.get("DISCORD_GLOBAL_WEBHOOK_URL", os.environ.get("DISCORD_WEBHOOK_URL", "")).strip()
SMS_CLUSTER_PHONE = os.environ.get("SMS_CLUSTER_PHONE", "").strip()
SMS_CLUSTER_MIN_IMPORTANCE = float(os.environ.get("SMS_CLUSTER_MIN_IMPORTANCE", "0.84"))
CLUSTER_ALERT_DAILY_LIMIT = 5
CONGRESS_CLUSTER_WINDOW_DAYS = int(os.environ.get("CONGRESS_CLUSTER_WINDOW_DAYS", "10"))
CROSS_SOURCE_CLUSTER_WINDOW_DAYS = int(os.environ.get("CROSS_SOURCE_CLUSTER_WINDOW_DAYS", "45"))
FUND_ALIGNMENT_WINDOW_DAYS = int(os.environ.get("FUND_ALIGNMENT_WINDOW_DAYS", "120"))
OWNER_SMS_SIGNAL_TYPES = {
    "politician_cluster",
    "cross_source_accumulation",
    "politician_gain_milestone",
    "cluster_gain_milestone",
}
CLUSTER_ALERT_SIGNAL_TYPES = {
    "politician_cluster",
    "insider_cluster",
    "cross_source_accumulation",
}


def stable_id(parts: list[str]) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def fetch_recent_signal_events(supabase):
    # Alerts should be queued for newly-created signal events. Pulling the full
    # 45-120 day cluster/fund windows here made production scan and sort too
    # much of signal_events, causing statement timeouts after capture succeeded.
    since_ts = (utc_now() - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    rows: list[dict] = []
    page_size = 1000
    start = 0
    while True:
        # Do not order in the DB. The queued delivery key is idempotent, so
        # processing order is not semantically important and avoiding ORDER BY
        # keeps the scheduled path below PostgREST statement timeouts.
        response = (
            supabase.table("signal_events")
            .select("*")
            .gte("created_at", since_ts)
            .range(start, start + page_size - 1)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return rows


def fetch_watchlist_tickers(supabase):
    try:
        response = supabase.table("watchlist_tickers").select("watchlist_id,ticker,alert_mode").execute()
        rows = response.data or []
    except Exception:
        response = supabase.table("watchlist_tickers").select("watchlist_id,ticker").execute()
        rows = response.data or []
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_ticker[(row.get("ticker") or "").upper()].append(
            {
                "watchlist_id": row["watchlist_id"],
                "match_type": "ticker",
                "alert_mode": row.get("alert_mode") or "both",
            }
        )
    return by_ticker


def fetch_watchlist_actors(supabase):
    try:
        response = supabase.table("watchlist_actors").select("watchlist_id,actor_type,actor_key,alert_mode").execute()
        rows = response.data or []
    except Exception:
        try:
            response = supabase.table("watchlist_actors").select("watchlist_id,actor_type,actor_key").execute()
            rows = response.data or []
        except Exception:
            return {}
    by_match_key: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        watchlist_id = row.get("watchlist_id")
        actor_type = str(row.get("actor_type") or "").strip().lower()
        actor_key = str(row.get("actor_key") or "").strip().lower()
        if not watchlist_id or not actor_type or not actor_key:
            continue
        by_match_key[f"{actor_type}:{actor_key}"].append(
            {
                "watchlist_id": watchlist_id,
                "match_type": "actor",
                "alert_mode": row.get("alert_mode") or "both",
                "actor_match_key": f"{actor_type}:{actor_key}",
            }
        )
    return by_match_key


def fetch_subscriptions(supabase):
    response = (
        supabase.table("alert_subscriptions")
        .select("*")
        .eq("active", True)
        .in_("channel", ["email", "sms", "discord"])
        .execute()
    )
    return response.data or []


def fetch_cluster_alert_watchlists(supabase):
    try:
        profiles = (
            supabase.table("profiles")
            .select("id,cluster_alert_channels")
            .eq("cluster_alerts_enabled", True)
            .execute()
        ).data or []
    except Exception:
        return {}

    channels_by_user_id = {
        str(row.get("id") or "").strip(): {
            str(channel or "").strip().lower()
            for channel in (row.get("cluster_alert_channels") or [])
            if str(channel or "").strip().lower() in {"email", "sms"}
        }
        for row in profiles
        if str(row.get("id") or "").strip()
    }
    user_ids = set(channels_by_user_id)
    if not user_ids:
        return {}

    watchlists = supabase.table("watchlists").select("id,user_id,owner_type,owner_key").execute().data or []
    cluster_watchlists = {}
    for row in watchlists:
        user_id = str(row.get("user_id") or row.get("owner_key") or "").strip()
        if not user_id or user_id not in user_ids:
            continue
        if not row.get("user_id") and str(row.get("owner_type") or "").strip() != "auth_user":
            continue
        cluster_watchlists[str(row["id"])] = {
            "user_id": user_id,
            "channels": channels_by_user_id.get(user_id, set()),
        }
    return cluster_watchlists


def event_matches_subscription(event: dict, subscription: dict, *, behavior: dict, allow_activity_override: bool = False) -> bool:
    event_types = subscription.get("event_types") or []
    if event_types:
        values = {str(value).lower() for value in event_types}
        event_signal_type = str(event.get("signal_type") or "").lower()
        event_source = str(event.get("source") or "").lower()
        base_signal_type = str((event.get("payload") or {}).get("base_signal_type") or "").lower()
        if event_signal_type not in values and event_source not in values and base_signal_type not in values:
            return False
    if allow_activity_override and behavior.get("activity"):
        return True
    if float(event.get("importance_score") or 0) < float(subscription.get("minimum_importance") or 0):
        return False
    return True


def queue_subscription_deliveries(events, subscriptions, watchlist_tickers, watchlist_actors, cluster_alert_watchlists=None):
    queued_by_key: dict[str, dict] = {}
    actor_summary_coverage: dict[str, str] = {}
    subscriptions_by_watchlist: dict[str, list[dict]] = defaultdict(list)
    global_subscriptions: list[dict] = []

    for event in events:
        signal_type = str(event.get("signal_type") or "").lower()
        payload = event.get("payload") or {}
        if signal_type in {"politician_filing_summary", "insider_filing_summary"}:
            for base_event_id in payload.get("summary_event_ids") or []:
                actor_summary_coverage[str(base_event_id)] = str(event["id"])

    for subscription in subscriptions:
        watchlist_id = subscription.get("watchlist_id")
        if watchlist_id:
            subscriptions_by_watchlist[watchlist_id].append(subscription)
        else:
            global_subscriptions.append(subscription)

    for event in events:
        signal_type = str(event.get("signal_type") or "").lower()
        behavior = classify_event_behavior(event)
        if behavior.get("suppressed"):
            continue
        for subscription in global_subscriptions:
            if not event_matches_subscription(event, subscription, behavior=behavior):
                continue
            destination = (subscription.get("destination") or "").strip()
            if not destination:
                continue
            delivery_key = stable_id([event["id"], subscription["id"], subscription["channel"]])
            queued_by_key[delivery_key] = {
                "signal_event_id": event["id"],
                "subscription_id": subscription["id"],
                "delivery_key": delivery_key,
                "channel": subscription["channel"],
                "destination": destination,
                "status": "pending",
                "payload": {"reason": "global_subscription", "behavior": behavior},
            }

        event_ticker = (event.get("ticker") or "").upper()
        matched_follow_rows: list[dict] = list(watchlist_tickers.get(event_ticker, []))
        for actor_match_key in event_actor_match_keys(event):
            matched_follow_rows.extend(watchlist_actors.get(actor_match_key, []))

        if str(event["id"]) in actor_summary_coverage:
            matched_follow_rows = [row for row in matched_follow_rows if row.get("match_type") != "actor"]

        matched_by_watchlist: dict[str, list[dict]] = defaultdict(list)
        for follow_row in matched_follow_rows:
            if follow_mode_matches(follow_row.get("alert_mode"), behavior):
                matched_by_watchlist[follow_row["watchlist_id"]].append(follow_row)
        if signal_type in CLUSTER_ALERT_SIGNAL_TYPES:
            for watchlist_id, target in (cluster_alert_watchlists or {}).items():
                matched_by_watchlist[watchlist_id].append(
                    {
                        "watchlist_id": watchlist_id,
                        "match_type": "cluster",
                        "alert_mode": "both",
                        "channels": target.get("channels") or set(),
                        "user_id": target.get("user_id"),
                    }
                )

        for watchlist_id, matched_rows in matched_by_watchlist.items():
            allow_activity_override = any(
                str(row.get("alert_mode") or "").lower() in {"activity", "both"} and behavior.get("activity")
                for row in matched_rows
            )
            for subscription in subscriptions_by_watchlist.get(watchlist_id, []):
                channel_matched_rows = [
                    row
                    for row in matched_rows
                    if row.get("match_type") != "cluster" or subscription.get("channel") in (row.get("channels") or set())
                ]
                if not channel_matched_rows:
                    continue
                if not event_matches_subscription(
                    event,
                    subscription,
                    behavior=behavior,
                    allow_activity_override=allow_activity_override,
                ):
                    continue
                destination = (subscription.get("destination") or "").strip()
                if not destination:
                    continue
                delivery_key = stable_id([event["id"], subscription["id"], subscription["channel"]])
                reasons = sorted({f"watchlist_{row.get('match_type')}_match" for row in channel_matched_rows})
                modes = sorted({str(row.get("alert_mode") or "both").lower() for row in channel_matched_rows})
                actor_keys = sorted(
                    {
                        str(row.get("actor_match_key") or "")
                        for row in channel_matched_rows
                        if str(row.get("actor_match_key") or "").strip()
                    }
                )
                cluster_user_id = next(
                    (
                        str(row.get("user_id") or "").strip()
                        for row in channel_matched_rows
                        if row.get("match_type") == "cluster" and str(row.get("user_id") or "").strip()
                    ),
                    "",
                )

                queued_by_key[delivery_key] = {
                    "signal_event_id": event["id"],
                    "subscription_id": subscription["id"],
                    "delivery_key": delivery_key,
                    "channel": subscription["channel"],
                    "destination": destination,
                    "status": "pending",
                    "payload": {
                        "reasons": reasons or ["watchlist_match"],
                        "behavior": behavior,
                        "matched_follow_modes": modes,
                        "matched_actor_keys": actor_keys,
                    },
                }
                if cluster_user_id:
                    queued_by_key[delivery_key]["_cluster_alert_user_id"] = cluster_user_id
                    queued_by_key[delivery_key]["_importance_score"] = float(event.get("importance_score") or 0)
    return list(queued_by_key.values())


def queue_capped_cluster_deliveries(supabase, deliveries: list[dict]) -> dict[str, int]:
    if not deliveries:
        return {
            "deliveries_queued": 0,
            "cluster_events_reserved": 0,
            "cluster_events_suppressed": 0,
        }

    payload = [
        {
            "user_id": delivery["_cluster_alert_user_id"],
            "importance_score": delivery.get("_importance_score") or 0,
            **{
                key: value
                for key, value in delivery.items()
                if key not in {"_cluster_alert_user_id", "_importance_score"}
            },
        }
        for delivery in deliveries
    ]
    response = supabase.rpc(
        "queue_cluster_alert_deliveries_capped",
        {
            "p_deliveries": payload,
            "p_daily_limit": CLUSTER_ALERT_DAILY_LIMIT,
        },
    ).execute()
    result = (response.data or [{}])[0]
    return {
        "deliveries_queued": int(result.get("deliveries_queued") or 0),
        "cluster_events_reserved": int(result.get("cluster_events_reserved") or 0),
        "cluster_events_suppressed": int(result.get("cluster_events_suppressed") or 0),
    }


def queue_global_discord_deliveries(events, subscriptions):
    has_db_global_discord = any(
        not subscription.get("watchlist_id") and subscription.get("channel") == "discord"
        for subscription in subscriptions
    )
    if has_db_global_discord:
        return []
    if not GLOBAL_WEBHOOK_URL:
        return []

    queued = []
    for event in events:
        if float(event.get("importance_score") or 0) < GLOBAL_MIN_IMPORTANCE:
            continue
        queued.append(
            {
                "signal_event_id": event["id"],
                "subscription_id": None,
                "delivery_key": stable_id([event["id"], "global_discord", "discord"]),
                "channel": "discord",
                "destination": GLOBAL_WEBHOOK_URL,
                "status": "pending",
                "payload": {"reason": "global_threshold"},
            }
        )
    return queued


def queue_owner_sms_signal_deliveries(events):
    if not SMS_CLUSTER_PHONE:
        return []

    queued = []
    for event in events:
        signal_type = str(event.get("signal_type") or "").strip().lower()
        if signal_type not in OWNER_SMS_SIGNAL_TYPES:
            continue
        if float(event.get("importance_score") or 0) < SMS_CLUSTER_MIN_IMPORTANCE:
            continue

        queued.append(
            {
                "signal_event_id": event["id"],
                "subscription_id": None,
                "delivery_key": stable_id([event["id"], "global_cluster_sms", "sms"]),
                "channel": "sms",
                "destination": SMS_CLUSTER_PHONE,
                "status": "pending",
                "payload": {
                    "reason": "owner_signal_sms",
                    "behavior": classify_event_behavior(event),
                },
            }
        )
    return queued


def owner_sms_subscription_ids(subscriptions: list[dict]) -> set[str]:
    matching_ids: set[str] = set()
    for subscription in subscriptions:
        if subscription.get("watchlist_id") is not None:
            continue
        if str(subscription.get("channel") or "").strip().lower() != "sms":
            continue
        event_types = {str(value).strip().lower() for value in (subscription.get("event_types") or []) if str(value).strip()}
        if OWNER_SMS_SIGNAL_TYPES & event_types:
            subscription_id = str(subscription.get("id") or "").strip()
            if subscription_id:
                matching_ids.add(subscription_id)
    return matching_ids


def main():
    print("Queueing alert deliveries...")
    supabase = get_supabase_client()
    db_events = fetch_recent_signal_events(supabase)
    raw_events = [event for event in db_events if not is_compiled_notification_event(event)]
    raw_events = enrich_events_with_member_roles(raw_events)
    raw_events = enrich_events_with_insider_position_changes(raw_events)
    raw_events = enrich_events_with_congress_buy_history(raw_events, supabase)
    compiled_events = [event for event in db_events if is_compiled_notification_event(event)]
    events = raw_events + compiled_events
    watchlist_tickers = fetch_watchlist_tickers(supabase)
    watchlist_actors = fetch_watchlist_actors(supabase)
    subscriptions = fetch_subscriptions(supabase)
    cluster_alert_watchlists = fetch_cluster_alert_watchlists(supabase)
    owner_subscription_ids = owner_sms_subscription_ids(subscriptions)

    subscription_deliveries = queue_subscription_deliveries(
        events,
        subscriptions,
        watchlist_tickers,
        watchlist_actors,
        cluster_alert_watchlists,
    )
    cluster_deliveries = [
        delivery for delivery in subscription_deliveries if delivery.get("_cluster_alert_user_id")
    ]
    standard_deliveries = [
        delivery for delivery in subscription_deliveries if not delivery.get("_cluster_alert_user_id")
    ]
    standard_deliveries.extend(queue_global_discord_deliveries(events, subscriptions))
    standard_deliveries.extend(queue_owner_sms_signal_deliveries(events))

    if standard_deliveries:
        supabase.table("alert_deliveries").upsert(standard_deliveries, on_conflict="delivery_key").execute()
    capped_result = queue_capped_cluster_deliveries(supabase, cluster_deliveries)
    deliveries_queued = len(standard_deliveries) + capped_result["deliveries_queued"]

    summary = {
        "raw_signal_events_seen": len(raw_events),
        "signal_events_seen": len(events),
        "compiled_signal_events_seen": len(compiled_events),
        "watchlist_subscriptions_seen": len(subscriptions),
        "cluster_alert_watchlists_seen": len(cluster_alert_watchlists),
        "watchlist_actor_targets_seen": sum(len(values) for values in watchlist_actors.values()),
        "deliveries_queued": deliveries_queued,
        "cluster_alert_daily_limit": CLUSTER_ALERT_DAILY_LIMIT,
        "cluster_alert_events_reserved": capped_result["cluster_events_reserved"],
        "cluster_alert_events_suppressed": capped_result["cluster_events_suppressed"],
        "lookback_hours": LOOKBACK_HOURS,
        "congress_cluster_window_days": CONGRESS_CLUSTER_WINDOW_DAYS,
        "cross_source_cluster_window_days": CROSS_SOURCE_CLUSTER_WINDOW_DAYS,
        "fund_alignment_window_days": FUND_ALIGNMENT_WINDOW_DAYS,
        "compiled_cluster_events": sum(1 for event in events if event.get("signal_type") == "politician_cluster"),
        "compiled_insider_cluster_events": sum(1 for event in events if event.get("signal_type") == "insider_cluster"),
        "compiled_cross_source_events": sum(
            1 for event in events if event.get("signal_type") == "cross_source_accumulation"
        ),
        "compiled_politician_gain_events": sum(
            1 for event in events if event.get("signal_type") == "politician_gain_milestone"
        ),
        "compiled_cluster_gain_events": sum(
            1 for event in events if event.get("signal_type") == "cluster_gain_milestone"
        ),
        "global_discord_enabled": bool(GLOBAL_WEBHOOK_URL),
        "owner_sms_enabled": bool(SMS_CLUSTER_PHONE) or bool(owner_subscription_ids),
        "owner_sms_min_importance": SMS_CLUSTER_MIN_IMPORTANCE,
        "owner_sms_deliveries_queued": sum(
            1
            for delivery in standard_deliveries
            if delivery.get("channel") == "sms"
            and (
                (delivery.get("payload") or {}).get("reason") == "owner_signal_sms"
                or str(delivery.get("subscription_id") or "") in owner_subscription_ids
            )
        ),
    }
    emit_summary(summary)
    print(f"Queued {deliveries_queued} deliveries from {len(events)} recent signal events.")


if __name__ == "__main__":
    main()
