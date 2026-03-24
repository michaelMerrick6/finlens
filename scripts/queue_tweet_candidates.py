import os
from datetime import timedelta

from congress_relevance import enrich_events_with_member_roles
from insider_holdings import enrich_events_with_insider_sell_reductions
from pipeline_support import emit_summary, get_supabase_client, utc_now
from signal_policy import load_signal_policy
from tweet_candidate_compiler import build_tweet_candidates


LOOKBACK_HOURS = int(os.environ.get("TWEET_CANDIDATE_LOOKBACK_HOURS", "96"))
POLICY = load_signal_policy()
TWEET_POLICY = POLICY.get("tweet_candidates") or {}
MINIMUM_IMPORTANCE = float(TWEET_POLICY.get("minimum_importance") or os.environ.get("TWEET_CANDIDATE_MIN_IMPORTANCE", "0.88"))
MINIMUM_GROUP_COUNT = int(TWEET_POLICY.get("minimum_group_count") or os.environ.get("TWEET_CANDIDATE_MIN_GROUP_COUNT", "2"))


def tweet_candidate_table_exists(supabase) -> bool:
    try:
        supabase.table("tweet_candidates").select("id", count="exact", head=True).limit(1).execute()
        return True
    except Exception:
        return False


def fetch_recent_signal_events(supabase) -> list[dict]:
    since_date = (utc_now() - timedelta(hours=LOOKBACK_HOURS)).date().isoformat()
    response = (
        supabase.table("signal_events")
        .select("*")
        .gte("published_at", since_date)
        .order("published_at", desc=True)
        .order("created_at", desc=True)
        .limit(1000)
        .execute()
    )
    return response.data or []


def prune_stale_pending_candidates(supabase, *, valid_candidate_keys: set[str], published_since: str) -> int:
    response = (
        supabase.table("tweet_candidates")
        .select("id,candidate_key,signal_events!inner(published_at)")
        .eq("status", "pending_review")
        .execute()
    )
    rows = response.data or []
    stale_ids: list[str] = []
    for row in rows:
        signal_event = row.get("signal_events") or {}
        published_at = str(signal_event.get("published_at") or "").strip()[:10]
        if not published_at or published_at < published_since:
            continue
        if str(row.get("candidate_key") or "") not in valid_candidate_keys:
            stale_ids.append(str(row["id"]))

    deleted = 0
    for index in range(0, len(stale_ids), 100):
        chunk = stale_ids[index : index + 100]
        if not chunk:
            continue
        supabase.table("tweet_candidates").delete().in_("id", chunk).execute()
        deleted += len(chunk)
    return deleted


def main():
    print("Queueing tweet candidates...")
    supabase = get_supabase_client()

    if not tweet_candidate_table_exists(supabase):
        emit_summary(
            {
                "tweet_candidates_enabled": False,
                "reason": "tweet_candidates_table_missing",
                "lookback_hours": LOOKBACK_HOURS,
            }
        )
        print("Tweet candidate queue skipped: tweet_candidates table is not available.")
        return

    events = fetch_recent_signal_events(supabase)
    events = enrich_events_with_member_roles(events)
    events = enrich_events_with_insider_sell_reductions(events)
    published_since = (utc_now() - timedelta(hours=LOOKBACK_HOURS)).date().isoformat()
    candidates = build_tweet_candidates(
        events,
        minimum_importance=MINIMUM_IMPORTANCE,
        minimum_group_count=MINIMUM_GROUP_COUNT,
    )
    valid_candidate_keys = {str(candidate["candidate_key"]) for candidate in candidates}
    stale_deleted = prune_stale_pending_candidates(
        supabase,
        valid_candidate_keys=valid_candidate_keys,
        published_since=published_since,
    )

    if candidates:
        supabase.table("tweet_candidates").upsert(candidates, on_conflict="channel,candidate_key").execute()

    summary = {
        "tweet_candidates_enabled": True,
        "signal_events_seen": len(events),
        "tweet_candidates_upserted": len(candidates),
        "tweet_candidates_deleted": stale_deleted,
        "lookback_hours": LOOKBACK_HOURS,
        "published_since": published_since,
        "minimum_importance": MINIMUM_IMPORTANCE,
        "minimum_group_count": MINIMUM_GROUP_COUNT,
    }
    emit_summary(summary)
    print(f"Tweet candidate queue complete: {len(candidates)} candidates upserted, {stale_deleted} stale candidates deleted.")


if __name__ == "__main__":
    main()
