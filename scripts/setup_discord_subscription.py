import argparse
from typing import Iterable

from notification_targets import normalize_actor_key, resolve_politician_target
from pipeline_support import get_supabase_client


def parse_csv(values: str | None) -> list[str]:
    if not values:
        return []
    return [value.strip() for value in values.split(",") if value.strip()]


def ensure_watchlist(supabase, owner_key: str, name: str) -> str:
    def lookup() -> str | None:
        existing = (
            supabase.table("watchlists")
            .select("id")
            .eq("owner_type", "user")
            .eq("owner_key", owner_key)
            .eq("name", name)
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]["id"]
        return None

    existing_id = lookup()
    if existing_id:
        return existing_id

    try:
        created = (
            supabase.table("watchlists")
            .insert({"owner_type": "user", "owner_key": owner_key, "name": name, "active": True})
            .execute()
        )
        return created.data[0]["id"]
    except Exception:
        existing_id = lookup()
        if existing_id:
            return existing_id
        raise


def ensure_watchlist_tickers(supabase, watchlist_id: str, tickers: Iterable[str]) -> int:
    payload = [{"watchlist_id": watchlist_id, "ticker": ticker.upper()} for ticker in tickers]
    if not payload:
        return 0
    supabase.table("watchlist_tickers").upsert(payload, on_conflict="watchlist_id,ticker").execute()
    return len(payload)


def ensure_watchlist_actors(supabase, watchlist_id: str, actors: Iterable[dict]) -> int:
    payload = [
        {
            "watchlist_id": watchlist_id,
            "actor_type": actor["actor_type"],
            "actor_key": actor["actor_key"],
            "actor_name": actor["actor_name"],
            "metadata": actor.get("metadata") or {},
        }
        for actor in actors
    ]
    if not payload:
        return 0
    supabase.table("watchlist_actors").upsert(payload, on_conflict="watchlist_id,actor_type,actor_key").execute()
    return len(payload)


def load_congress_members(supabase) -> list[dict]:
    response = supabase.table("congress_members").select("id, first_name, last_name, chamber, active, state, party").execute()
    return response.data or []


def build_politician_targets(values: Iterable[str], members: list[dict]) -> list[dict]:
    payload: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    for value in values:
        resolved = resolve_politician_target(value, members)
        if resolved:
            actor_name = f"{resolved.get('first_name') or ''} {resolved.get('last_name') or ''}".strip()
            candidates = [
                {
                    "actor_type": "politician",
                    "actor_key": str(resolved["id"]).lower(),
                    "actor_name": actor_name or value,
                    "metadata": {
                        "member_id": resolved["id"],
                        "state": resolved.get("state"),
                        "party": resolved.get("party"),
                        "resolved_from": value,
                    },
                },
                {
                    "actor_type": "politician",
                    "actor_key": normalize_actor_key(actor_name or value),
                    "actor_name": actor_name or value,
                    "metadata": {
                        "member_id": resolved["id"],
                        "state": resolved.get("state"),
                        "party": resolved.get("party"),
                        "resolved_from": value,
                    },
                },
            ]
        else:
            candidates = [
                {
                    "actor_type": "politician",
                    "actor_key": normalize_actor_key(value),
                    "actor_name": value,
                    "metadata": {"resolved_from": value, "resolution": "name_only"},
                }
            ]

        for candidate in candidates:
            actor_key = candidate.get("actor_key") or ""
            dedupe_key = (candidate["actor_type"], actor_key)
            if not actor_key or dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            payload.append(candidate)
    return payload


def build_named_actor_targets(actor_type: str, values: Iterable[str]) -> list[dict]:
    payload: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    for value in values:
        actor_key = normalize_actor_key(value)
        dedupe_key = (actor_type, actor_key)
        if not actor_key or dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        payload.append(
            {
                "actor_type": actor_type,
                "actor_key": actor_key,
                "actor_name": value,
                "metadata": {"resolved_from": value},
            }
        )
    return payload


def ensure_subscription(
    supabase,
    *,
    watchlist_id: str | None,
    destination: str,
    minimum_importance: float,
    event_types: list[str],
) -> str:
    payload = {
        "watchlist_id": watchlist_id,
        "channel": "discord",
        "destination": destination,
        "minimum_importance": minimum_importance,
        "event_types": event_types,
        "active": True,
    }
    existing_rows = (
        supabase.table("alert_subscriptions")
        .select("id, watchlist_id")
        .eq("channel", "discord")
        .eq("destination", destination)
        .execute()
    )
    existing = None
    for row in existing_rows.data or []:
        if row.get("watchlist_id") == watchlist_id:
            existing = row
            break

    if existing:
        subscription_id = existing["id"]
        supabase.table("alert_subscriptions").update(payload).eq("id", subscription_id).execute()
        return subscription_id

    created = supabase.table("alert_subscriptions").insert(payload).execute()
    return created.data[0]["id"]


def main():
    parser = argparse.ArgumentParser(description="Create or update a Discord alert subscription for Vail.")
    parser.add_argument("--webhook-url", required=True, help="Discord webhook URL for delivery.")
    parser.add_argument("--minimum-importance", type=float, default=0.8, help="Minimum importance score required.")
    parser.add_argument("--event-types", default="", help="Comma-separated event/source filters, e.g. insider_trade,politician_trade")
    parser.add_argument("--global", dest="global_subscription", action="store_true", help="Create a global subscription across all signal events.")
    parser.add_argument("--owner-key", help="Required for watchlist subscriptions. Logical user identifier.")
    parser.add_argument("--watchlist-name", default="Default", help="Watchlist name for watchlist subscriptions.")
    parser.add_argument("--tickers", default="", help="Comma-separated tickers for the watchlist.")
    parser.add_argument("--politicians", default="", help="Comma-separated politician names or member ids to follow.")
    parser.add_argument("--insiders", default="", help="Comma-separated insider names to follow.")
    parser.add_argument("--funds", default="", help="Comma-separated fund names to follow.")
    args = parser.parse_args()

    supabase = get_supabase_client()
    event_types = parse_csv(args.event_types)

    if args.global_subscription:
        subscription_id = ensure_subscription(
            supabase,
            watchlist_id=None,
            destination=args.webhook_url,
            minimum_importance=args.minimum_importance,
            event_types=event_types,
        )
        print({"mode": "global", "subscription_id": subscription_id})
        return

    if not args.owner_key:
        parser.error("--owner-key is required unless --global is used.")

    tickers = parse_csv(args.tickers)
    politician_values = parse_csv(args.politicians)
    insider_values = parse_csv(args.insiders)
    fund_values = parse_csv(args.funds)

    members = load_congress_members(supabase) if politician_values else []
    actor_targets = []
    actor_targets.extend(build_politician_targets(politician_values, members))
    actor_targets.extend(build_named_actor_targets("insider", insider_values))
    actor_targets.extend(build_named_actor_targets("fund", fund_values))

    watchlist_id = ensure_watchlist(supabase, args.owner_key, args.watchlist_name)
    watchlist_ticker_count = ensure_watchlist_tickers(supabase, watchlist_id, tickers)
    watchlist_actor_count = ensure_watchlist_actors(supabase, watchlist_id, actor_targets)
    subscription_id = ensure_subscription(
        supabase,
        watchlist_id=watchlist_id,
        destination=args.webhook_url,
        minimum_importance=args.minimum_importance,
        event_types=event_types,
    )
    print(
        {
            "mode": "watchlist",
            "watchlist_id": watchlist_id,
            "watchlist_tickers_upserted": watchlist_ticker_count,
            "watchlist_actors_upserted": watchlist_actor_count,
            "subscription_id": subscription_id,
        }
    )


if __name__ == "__main__":
    main()
