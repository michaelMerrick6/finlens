import hashlib
import os
from collections import defaultdict
from datetime import timedelta

from alert_rules import classify_event_behavior, follow_mode_matches
from congress_relevance import enrich_events_with_member_roles
from notification_targets import event_actor_match_keys
from notification_compiler import compile_notification_events, is_compiled_notification_event
from pipeline_support import emit_summary, get_supabase_client, utc_now


LOOKBACK_HOURS = int(os.environ.get("ALERT_QUEUE_LOOKBACK_HOURS", "48"))
GLOBAL_MIN_IMPORTANCE = float(os.environ.get("DISCORD_GLOBAL_MIN_IMPORTANCE", "0.8"))
GLOBAL_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
CONGRESS_CLUSTER_WINDOW_DAYS = int(os.environ.get("CONGRESS_CLUSTER_WINDOW_DAYS", "7"))
CONGRESS_CLUSTER_MIN_MEMBERS = int(os.environ.get("CONGRESS_CLUSTER_MIN_MEMBERS", "2"))


def stable_id(parts: list[str]) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def fetch_recent_signal_events(supabase):
    effective_hours = max(LOOKBACK_HOURS, CONGRESS_CLUSTER_WINDOW_DAYS * 24)
    since_ts = (utc_now() - timedelta(hours=effective_hours)).isoformat()
    response = (
        supabase.table("signal_events")
        .select("*")
        .gte("created_at", since_ts)
        .order("created_at", desc=True)
        .limit(1000)
        .execute()
    )
    return response.data or []


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
        .execute()
    )
    return response.data or []


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


def queue_subscription_deliveries(events, subscriptions, watchlist_tickers, watchlist_actors):
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

        for watchlist_id, matched_rows in matched_by_watchlist.items():
            allow_activity_override = any(
                str(row.get("alert_mode") or "").lower() in {"activity", "both"} and behavior.get("activity")
                for row in matched_rows
            )
            for subscription in subscriptions_by_watchlist.get(watchlist_id, []):
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
                reasons = sorted({f"watchlist_{row.get('match_type')}_match" for row in matched_rows})
                modes = sorted({str(row.get("alert_mode") or "both").lower() for row in matched_rows})
                actor_keys = sorted(
                    {
                        str(row.get("actor_match_key") or "")
                        for row in matched_rows
                        if str(row.get("actor_match_key") or "").strip()
                    }
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
    return list(queued_by_key.values())


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


def main():
    print("Queueing alert deliveries...")
    supabase = get_supabase_client()
    db_events = fetch_recent_signal_events(supabase)
    raw_events = [event for event in db_events if not is_compiled_notification_event(event)]
    raw_events = enrich_events_with_member_roles(raw_events)
    events = compile_notification_events(
        raw_events,
        congress_cluster_window_days=CONGRESS_CLUSTER_WINDOW_DAYS,
        congress_cluster_min_members=CONGRESS_CLUSTER_MIN_MEMBERS,
    )
    compiled_events = [event for event in events if is_compiled_notification_event(event)]
    if compiled_events:
        supabase.table("signal_events").upsert(
            compiled_events,
            on_conflict="source,source_document_id",
        ).execute()
    watchlist_tickers = fetch_watchlist_tickers(supabase)
    watchlist_actors = fetch_watchlist_actors(supabase)
    subscriptions = fetch_subscriptions(supabase)

    deliveries = []
    deliveries.extend(queue_subscription_deliveries(events, subscriptions, watchlist_tickers, watchlist_actors))
    deliveries.extend(queue_global_discord_deliveries(events, subscriptions))

    if deliveries:
        supabase.table("alert_deliveries").upsert(deliveries, on_conflict="delivery_key").execute()

    summary = {
        "raw_signal_events_seen": len(raw_events),
        "signal_events_seen": len(events),
        "compiled_notification_events_upserted": len(compiled_events),
        "watchlist_subscriptions_seen": len(subscriptions),
        "watchlist_actor_targets_seen": sum(len(values) for values in watchlist_actors.values()),
        "deliveries_queued": len(deliveries),
        "lookback_hours": LOOKBACK_HOURS,
        "congress_cluster_window_days": CONGRESS_CLUSTER_WINDOW_DAYS,
        "compiled_cluster_events": sum(1 for event in events if event.get("signal_type") == "politician_cluster"),
        "global_discord_enabled": bool(GLOBAL_WEBHOOK_URL),
    }
    emit_summary(summary)
    print(f"Queued {len(deliveries)} deliveries from {len(events)} recent signal events.")


if __name__ == "__main__":
    main()
