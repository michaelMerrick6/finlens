import argparse
import json

from alert_rules import classify_event_behavior, describe_behavior_reasons, follow_mode_matches
from congress_relevance import enrich_events_with_member_roles
from notification_compiler import compile_notification_events, is_compiled_notification_event
from notification_targets import normalize_actor_key, resolve_politician_target
from pipeline_support import get_supabase_client


def fetch_recent_signal_events(supabase, *, limit: int) -> list[dict]:
    response = (
        supabase.table("signal_events")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []


def load_congress_members(supabase) -> list[dict]:
    response = supabase.table("congress_members").select("id, first_name, last_name, active, state, party").execute()
    return response.data or []


def follow_match_keys(
    *,
    ticker: str | None,
    politician: str | None,
    insider: str | None,
    members: list[dict],
) -> dict:
    payload = {
        "ticker": (ticker or "").strip().upper(),
        "politician_keys": set(),
        "insider_keys": set(),
    }
    if politician:
        resolved = resolve_politician_target(politician, members)
        if resolved:
            payload["politician_keys"].add(f"politician:{str(resolved['id']).lower()}")
            actor_name = f"{resolved.get('first_name') or ''} {resolved.get('last_name') or ''}".strip()
            if actor_name:
                payload["politician_keys"].add(f"politician:{normalize_actor_key(actor_name)}")
        payload["politician_keys"].add(f"politician:{normalize_actor_key(politician)}")
    if insider:
        payload["insider_keys"].add(f"insider:{normalize_actor_key(insider)}")
    return payload


def event_actor_keys(event: dict) -> set[str]:
    from notification_targets import event_actor_match_keys

    return event_actor_match_keys(event)


def event_matches_follow(event: dict, *, mode: str, match_config: dict) -> tuple[bool, list[str], dict]:
    behavior = classify_event_behavior(event)
    if behavior.get("suppressed"):
        return False, [], behavior
    if not follow_mode_matches(mode, behavior):
        return False, [], behavior

    reasons = []
    ticker = match_config.get("ticker")
    if ticker and (event.get("ticker") or "").upper() == ticker:
        reasons.append("ticker")

    actor_keys = event_actor_keys(event)
    if actor_keys & match_config.get("politician_keys", set()):
        reasons.append("politician")
    if actor_keys & match_config.get("insider_keys", set()):
        reasons.append("insider")

    return bool(reasons), reasons, behavior


def build_actor_summary_coverage(events: list[dict]) -> dict[str, str]:
    coverage = {}
    for event in events:
        signal_type = str(event.get("signal_type") or "").lower()
        payload = event.get("payload") or {}
        if signal_type in {"politician_filing_summary", "insider_filing_summary"}:
            for base_event_id in payload.get("summary_event_ids") or []:
                coverage[str(base_event_id)] = str(event["id"])
    return coverage


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview alert matches for a proposed Vail follow.")
    parser.add_argument("--ticker", default="", help="Ticker to preview.")
    parser.add_argument("--politician", default="", help="Politician name or member id to preview.")
    parser.add_argument("--insider", default="", help="Insider name to preview.")
    parser.add_argument("--mode", default="unusual", choices=["activity", "unusual", "both"], help="Follow alert mode.")
    parser.add_argument("--limit", type=int, default=1000, help="Max recent signal events to scan.")
    parser.add_argument("--match-limit", type=int, default=20, help="Max matches to print.")
    args = parser.parse_args()

    if not args.ticker and not args.politician and not args.insider:
        parser.error("Provide at least one of --ticker, --politician, or --insider.")

    supabase = get_supabase_client()
    members = load_congress_members(supabase) if args.politician else []
    match_config = follow_match_keys(
        ticker=args.ticker,
        politician=args.politician,
        insider=args.insider,
        members=members,
    )
    db_events = fetch_recent_signal_events(supabase, limit=args.limit)
    raw_events = [event for event in db_events if not is_compiled_notification_event(event)]
    raw_events = enrich_events_with_member_roles(raw_events)
    events = compile_notification_events(raw_events)
    actor_summary_coverage = build_actor_summary_coverage(events)

    matches = []
    for event in events:
        matched, reasons, behavior = event_matches_follow(event, mode=args.mode, match_config=match_config)
        if not matched:
            continue
        if str(event["id"]) in actor_summary_coverage and any(reason in {"politician", "insider"} for reason in reasons):
            continue
        payload = event.get("payload") or {}
        matches.append(
            {
                "signal_type": event.get("signal_type"),
                "title": event.get("title"),
                "ticker": event.get("ticker"),
                "actor_name": event.get("actor_name"),
                "direction": event.get("direction"),
                "importance_score": event.get("importance_score"),
                "published_at": event.get("published_at"),
                "match_reasons": reasons,
                "behavior": behavior,
                "behavior_labels": describe_behavior_reasons(behavior),
                "amount_range": payload.get("amount_range"),
                "value": payload.get("value"),
                "source_url": event.get("source_url"),
            }
        )

    print(
        json.dumps(
            {
                "mode": args.mode,
                "follow": {
                    "ticker": args.ticker or None,
                    "politician": args.politician or None,
                    "insider": args.insider or None,
                },
                "events_scanned": len(events),
                "matches_found": len(matches),
                "matches": matches[: args.match_limit],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
