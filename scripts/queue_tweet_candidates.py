import os
import uuid
from datetime import timedelta

from congress_trade_history import enrich_events_with_congress_buy_history
from congress_relevance import enrich_events_with_member_roles
from insider_holdings import enrich_events_with_insider_position_changes
from pipeline_support import emit_summary, get_supabase_client, utc_now
from signal_policy import load_signal_policy
from tweet_candidate_compiler import build_broadcast_candidates


LOOKBACK_HOURS = int(os.environ.get("TWEET_CANDIDATE_LOOKBACK_HOURS", "2160"))
RECOVERY_LOOKBACK_HOURS = int(
    os.environ.get("TWEET_CANDIDATE_RECOVERY_LOOKBACK_HOURS", str(max(LOOKBACK_HOURS, 336)))
)
_SIGNAL_EVENTS_FETCH_LIMIT_RAW = str(os.environ.get("TWEET_CANDIDATE_SIGNAL_EVENT_LIMIT", "")).strip()
SIGNAL_EVENTS_FETCH_LIMIT = int(_SIGNAL_EVENTS_FETCH_LIMIT_RAW) if _SIGNAL_EVENTS_FETCH_LIMIT_RAW else None
REQUIRE_SIGNAL_EVENT_FRESHNESS = os.environ.get("TWEET_CANDIDATE_REQUIRE_SIGNAL_EVENT_FRESHNESS", "1").strip() != "0"
ENABLE_INSIDER_POSITION_ENRICHMENT = (
    os.environ.get("TWEET_CANDIDATE_ENABLE_INSIDER_POSITION_ENRICHMENT", "0").strip() == "1"
)
ENABLE_CONGRESS_BUY_HISTORY_ENRICHMENT = (
    os.environ.get("TWEET_CANDIDATE_ENABLE_CONGRESS_BUY_HISTORY", "0").strip() == "1"
)
POLICY = load_signal_policy()
TWEET_POLICY = POLICY.get("tweet_candidates") or {}
MINIMUM_IMPORTANCE = float(TWEET_POLICY.get("minimum_importance") or os.environ.get("TWEET_CANDIDATE_MIN_IMPORTANCE", "0.88"))
MINIMUM_GROUP_COUNT = int(TWEET_POLICY.get("minimum_group_count") or os.environ.get("TWEET_CANDIDATE_MIN_GROUP_COUNT", "2"))
QUEUE_STAGE_TOTAL = 10
UPSTREAM_SIGNAL_TABLES = {
    "politician_trades": "published_date",
    "insider_trades": "published_date",
}


def emit_progress(step: str, stage_number: int, **details) -> None:
    payload = {
        "step": step,
        "stage_number": stage_number,
        "stage_total": QUEUE_STAGE_TOTAL,
        "progress_percent": round((stage_number / QUEUE_STAGE_TOTAL) * 100, 1),
    }
    if details:
        payload["details"] = details
    import json

    print(f"PROGRESS_JSON:{json.dumps(payload, sort_keys=True)}", flush=True)


def tweet_candidate_table_exists(supabase) -> bool:
    try:
        supabase.table("tweet_candidates").select("id", count="exact", head=True).limit(1).execute()
        return True
    except Exception:
        return False


def fetch_signal_events_since(supabase, *, column: str, since_value: str) -> list[dict]:
    rows: list[dict] = []
    page_size = 1000
    start = 0
    while True:
        if SIGNAL_EVENTS_FETCH_LIMIT is not None and start >= SIGNAL_EVENTS_FETCH_LIMIT:
            break
        end = start + page_size - 1
        if SIGNAL_EVENTS_FETCH_LIMIT is not None:
            end = min(end, SIGNAL_EVENTS_FETCH_LIMIT - 1)
        response = (
            supabase.table("signal_events")
            .select("*")
            .gte(column, since_value)
            .order(column, desc=True)
            .order("created_at", desc=True)
            .range(start, end)
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


def signal_event_identity(event: dict) -> tuple[str, ...]:
    event_id = str(event.get("id") or "").strip()
    if event_id:
        return ("id", event_id)

    source = str(event.get("source") or "").strip().lower()
    source_document_id = str(event.get("source_document_id") or "").strip()
    if source and source_document_id:
        return ("source_document_id", source, source_document_id)

    return (
        "fallback",
        source,
        str(event.get("signal_type") or "").strip().lower(),
        str(event.get("ticker") or "").strip().upper(),
        str(event.get("actor_name") or "").strip(),
        str(event.get("direction") or "").strip().lower(),
        str(event.get("published_at") or "").strip(),
    )


def signal_event_sort_key(event: dict) -> tuple[str, str, str]:
    return (
        str(event.get("created_at") or ""),
        str(event.get("published_at") or ""),
        str(event.get("id") or ""),
    )


def merge_signal_event_batches(*batches: list[dict]) -> list[dict]:
    merged: dict[tuple[str, ...], dict] = {}
    for batch in batches:
        for event in batch:
            merged[signal_event_identity(event)] = event
    return sorted(merged.values(), key=signal_event_sort_key, reverse=True)


def fetch_recent_signal_events(supabase) -> tuple[list[dict], str, str]:
    published_since = (utc_now() - timedelta(hours=RECOVERY_LOOKBACK_HOURS)).date().isoformat()
    created_since = (utc_now() - timedelta(hours=RECOVERY_LOOKBACK_HOURS)).isoformat()
    published_rows = fetch_signal_events_since(supabase, column="published_at", since_value=published_since)
    created_rows = fetch_signal_events_since(supabase, column="created_at", since_value=created_since)
    return merge_signal_event_batches(published_rows, created_rows), published_since, created_since


def fetch_recent_upstream_dates(supabase, *, since_date: str) -> dict[str, str | None]:
    latest_by_table: dict[str, str | None] = {}
    for table, date_column in UPSTREAM_SIGNAL_TABLES.items():
        response = (
            supabase.table(table)
            .select(date_column)
            .gte(date_column, since_date)
            .order(date_column, desc=True)
            .limit(1)
            .execute()
        )
        latest_value = None
        if response.data:
            latest_value = str(response.data[0].get(date_column) or "").strip()[:10] or None
        latest_by_table[table] = latest_value
    return latest_by_table


def signal_pipeline_stale_reason(signal_event_count: int, upstream_latest: dict[str, str | None], *, lookback_hours: int) -> str | None:
    fresh_sources = {table: value for table, value in upstream_latest.items() if value}
    if signal_event_count > 0 or not fresh_sources:
        return None
    source_summary = ", ".join(f"{table}={value}" for table, value in sorted(fresh_sources.items()))
    return (
        f"No signal_events were available in the last {lookback_hours} hours even though upstream tables "
        f"have fresh rows ({source_summary})."
    )


def missing_insider_role_source_urls(events: list[dict]) -> list[str]:
    urls: set[str] = set()
    for event in events:
        if str(event.get("source") or "").strip().lower() != "insider":
            continue
        payload = event.get("payload") or {}
        if str(payload.get("filer_relation") or "").strip():
            continue
        source_url = str(event.get("source_url") or payload.get("source_url") or "").strip()
        if source_url:
            urls.add(source_url)
    return sorted(urls)


def fetch_insider_roles_for_source_urls(supabase, source_urls: list[str]) -> list[dict]:
    rows: list[dict] = []
    for index in range(0, len(source_urls), 200):
        chunk = source_urls[index : index + 200]
        if not chunk:
            continue
        response = (
            supabase.table("insider_trades")
            .select("source_url,ticker,filer_name,filer_relation,transaction_date,published_date")
            .in_("source_url", chunk)
            .execute()
        )
        rows.extend(response.data or [])
    return rows


def enrich_events_with_insider_roles(events: list[dict], insider_rows: list[dict]) -> list[dict]:
    by_source_url: dict[str, str] = {}
    by_identity: dict[tuple[str, str, str, str], str] = {}
    for row in insider_rows:
        relation = str(row.get("filer_relation") or "").strip()
        if not relation:
            continue
        source_url = str(row.get("source_url") or "").strip()
        if source_url:
            by_source_url[source_url] = relation
        identity = (
            str(row.get("ticker") or "").strip().upper(),
            str(row.get("filer_name") or "").strip(),
            str(row.get("transaction_date") or "").strip()[:10],
            str(row.get("published_date") or "").strip()[:10],
        )
        by_identity[identity] = relation

    enriched: list[dict] = []
    for event in events:
        if str(event.get("source") or "").strip().lower() != "insider":
            enriched.append(event)
            continue
        payload = dict(event.get("payload") or {})
        existing_relation = str(payload.get("filer_relation") or "").strip()
        if existing_relation:
            enriched.append(event)
            continue

        event_source_url = str(event.get("source_url") or payload.get("source_url") or "").strip()
        relation = by_source_url.get(event_source_url)
        if not relation:
            identity = (
                str(event.get("ticker") or "").strip().upper(),
                str(event.get("actor_name") or "").strip(),
                str(event.get("occurred_at") or "").strip()[:10],
                str(event.get("published_at") or "").strip()[:10],
            )
            relation = by_identity.get(identity)
        if not relation:
            enriched.append(event)
            continue

        payload["filer_relation"] = relation
        enriched_event = dict(event)
        enriched_event["payload"] = payload
        enriched.append(enriched_event)
    return enriched


def enrich_compiled_congress_amounts(events: list[dict]) -> list[dict]:
    by_event_id = {str(event.get("id") or ""): event for event in events if str(event.get("id") or "").strip()}
    by_source_document_id = {
        str(event.get("source_document_id") or ""): event
        for event in events
        if str(event.get("source_document_id") or "").strip()
    }
    enriched: list[dict] = []
    for event in events:
        signal_type = str(event.get("signal_type") or "").strip().lower()
        if signal_type not in {"politician_cluster", "cross_source_accumulation"}:
            enriched.append(event)
            continue

        payload = dict(event.get("payload") or {})
        cluster_event_ids = [str(value).strip() for value in (payload.get("cluster_event_ids") or []) if str(value).strip()]
        actor_rows = []
        changed = False
        for actor_row in payload.get("cluster_actors") or []:
            next_row = dict(actor_row)
            source = str(next_row.get("source") or "").strip().lower()
            actor_type = str(next_row.get("actor_type") or "").strip().lower()
            if (
                not str(next_row.get("amount_range") or "").strip()
                and source in {"", "congress"}
                and actor_type in {"", "politician", "congress"}
            ):
                member_id = str(next_row.get("member_id") or "").strip()
                actor_name = str(next_row.get("name") or "").strip()
                actor_source_document_id = str(next_row.get("source_document_id") or "").strip()
                if actor_source_document_id:
                    raw_event = by_source_document_id.get(actor_source_document_id)
                    if raw_event and str(raw_event.get("source") or "").strip().lower() == "congress":
                        raw_payload = raw_event.get("payload") or {}
                        amount_range = str(raw_payload.get("amount_range") or "").strip()
                        if amount_range:
                            next_row["amount_range"] = amount_range
                            changed = True
                            actor_rows.append(next_row)
                            continue
                for event_id in cluster_event_ids:
                    raw_event = by_event_id.get(event_id)
                    if not raw_event or str(raw_event.get("source") or "").strip().lower() != "congress":
                        continue
                    raw_payload = raw_event.get("payload") or {}
                    raw_member_id = str(raw_payload.get("member_id") or "").strip()
                    raw_actor_name = str(raw_event.get("actor_name") or "").strip()
                    amount_range = str(raw_payload.get("amount_range") or "").strip()
                    if not amount_range:
                        continue
                    if member_id and raw_member_id and member_id == raw_member_id:
                        next_row["amount_range"] = amount_range
                        changed = True
                        break
                    if actor_name and raw_actor_name and actor_name == raw_actor_name:
                        next_row["amount_range"] = amount_range
                        changed = True
                        break
            actor_rows.append(next_row)

        if not changed:
            enriched.append(event)
            continue

        payload["cluster_actors"] = actor_rows
        enriched_event = dict(event)
        enriched_event["payload"] = payload
        enriched.append(enriched_event)
    return enriched


def fetch_existing_candidates(supabase, *, candidate_keys: set[str]) -> dict[tuple[str, str], dict]:
    existing: dict[tuple[str, str], dict] = {}
    key_list = sorted(str(value) for value in candidate_keys if str(value).strip())
    for index in range(0, len(key_list), 200):
        chunk = key_list[index : index + 200]
        if not chunk:
            continue
        response = (
            supabase.table("tweet_candidates")
            .select("id,channel,candidate_key,status,title,draft_text,review_notes,reviewed_by,reviewed_at,posted_at,external_post_id")
            .in_("candidate_key", chunk)
            .execute()
        )
        for row in response.data or []:
            key = (str(row.get("channel") or ""), str(row.get("candidate_key") or ""))
            existing[key] = row
    return existing


def preserve_review_state(candidates: list[dict], existing_candidates: dict[tuple[str, str], dict]) -> int:
    preserved = 0
    for candidate in candidates:
        key = (str(candidate.get("channel") or ""), str(candidate.get("candidate_key") or ""))
        existing = existing_candidates.get(key)
        if not existing:
            candidate["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"tweet-candidate::{key[0]}::{key[1]}"))
            continue
        existing_id = existing.get("id")
        if existing_id:
            candidate["id"] = existing_id
        else:
            candidate["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"tweet-candidate::{key[0]}::{key[1]}"))
        if existing.get("status") == "pending_review":
            continue
        candidate["status"] = existing.get("status")
        candidate["title"] = existing.get("title") or candidate.get("title")
        candidate["draft_text"] = existing.get("draft_text") or candidate.get("draft_text")
        candidate["review_notes"] = existing.get("review_notes")
        candidate["reviewed_by"] = existing.get("reviewed_by")
        candidate["reviewed_at"] = existing.get("reviewed_at")
        candidate["posted_at"] = existing.get("posted_at")
        candidate["external_post_id"] = existing.get("external_post_id")
        preserved += 1
    return preserved


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
    print("Queueing tweet candidates...", flush=True)
    supabase = get_supabase_client()

    if not tweet_candidate_table_exists(supabase):
        emit_summary(
            {
                "tweet_candidates_enabled": False,
                "reason": "tweet_candidates_table_missing",
                "lookback_hours": LOOKBACK_HOURS,
            }
        )
        print("Tweet candidate queue skipped: tweet_candidates table is not available.", flush=True)
        return

    emit_progress(
        "Fetching recent signal events",
        1,
        lookback_hours=LOOKBACK_HOURS,
        recovery_lookback_hours=RECOVERY_LOOKBACK_HOURS,
    )
    events, published_since, created_since = fetch_recent_signal_events(supabase)
    print(f"Loaded {len(events)} signal events from the lookback window.", flush=True)
    upstream_latest: dict[str, str | None] = {}
    if not events:
        freshness_since = (utc_now() - timedelta(hours=LOOKBACK_HOURS)).date().isoformat()
        upstream_latest = fetch_recent_upstream_dates(supabase, since_date=freshness_since)
        stale_reason = signal_pipeline_stale_reason(
            len(events),
            upstream_latest,
            lookback_hours=LOOKBACK_HOURS,
        )
        if stale_reason:
            print(stale_reason, flush=True)
            if REQUIRE_SIGNAL_EVENT_FRESHNESS:
                raise RuntimeError(stale_reason)
    emit_progress("Resolving insider roles", 2, signal_events_seen=len(events))
    insider_role_rows = fetch_insider_roles_for_source_urls(supabase, missing_insider_role_source_urls(events))
    print(f"Resolved {len(insider_role_rows)} insider role rows.", flush=True)
    events = enrich_events_with_insider_roles(events, insider_role_rows)
    emit_progress("Enriching Congress cluster amounts", 3, insider_role_rows_seen=len(insider_role_rows))
    events = enrich_compiled_congress_amounts(events)
    emit_progress("Enriching member roles", 4)
    events = enrich_events_with_member_roles(events)
    emit_progress("Enriching insider position changes", 5)
    if ENABLE_INSIDER_POSITION_ENRICHMENT:
        events = enrich_events_with_insider_position_changes(events)
    else:
        print("Skipping insider position enrichment in the default tweet candidate path.", flush=True)
    emit_progress("Enriching Congress buy history", 6)
    if ENABLE_CONGRESS_BUY_HISTORY_ENRICHMENT:
        events = enrich_events_with_congress_buy_history(events, supabase)
    else:
        print("Skipping Congress buy-history enrichment in the default tweet candidate path.", flush=True)
    emit_progress("Building broadcast candidates", 7, published_since=published_since)
    candidates = build_broadcast_candidates(
        events,
        minimum_importance=MINIMUM_IMPORTANCE,
        minimum_group_count=MINIMUM_GROUP_COUNT,
    )
    print(f"Built {len(candidates)} broadcast candidates.", flush=True)
    valid_candidate_keys = {str(candidate["candidate_key"]) for candidate in candidates}
    emit_progress("Preserving existing review state", 8, candidate_count=len(candidates))
    existing_candidates = fetch_existing_candidates(supabase, candidate_keys=valid_candidate_keys)
    preserved_count = preserve_review_state(candidates, existing_candidates)
    emit_progress("Pruning stale pending candidates", 9, preserved_count=preserved_count)
    stale_deleted = prune_stale_pending_candidates(
        supabase,
        valid_candidate_keys=valid_candidate_keys,
        published_since=published_since,
    )

    if candidates:
        emit_progress("Upserting candidate rows", 10, stale_deleted=stale_deleted)
        supabase.table("tweet_candidates").upsert(candidates, on_conflict="channel,candidate_key").execute()

    summary = {
        "tweet_candidates_enabled": True,
        "signal_events_seen": len(events),
        "insider_role_rows_seen": len(insider_role_rows),
        "tweet_candidates_upserted": len(candidates),
        "tweet_candidates_deleted": stale_deleted,
        "tweet_candidate_statuses_preserved": preserved_count,
        "lookback_hours": LOOKBACK_HOURS,
        "recovery_lookback_hours": RECOVERY_LOOKBACK_HOURS,
        "published_since": published_since,
        "created_since": created_since,
        "minimum_importance": MINIMUM_IMPORTANCE,
        "minimum_group_count": MINIMUM_GROUP_COUNT,
        "signal_event_fetch_limit": SIGNAL_EVENTS_FETCH_LIMIT or "all",
        "upstream_latest_dates": upstream_latest,
        "insider_position_enrichment_enabled": ENABLE_INSIDER_POSITION_ENRICHMENT,
        "congress_buy_history_enrichment_enabled": ENABLE_CONGRESS_BUY_HISTORY_ENRICHMENT,
    }
    emit_summary(summary)
    print(
        f"Tweet candidate queue complete: {len(candidates)} candidates upserted, {stale_deleted} stale candidates deleted.",
        flush=True,
    )


if __name__ == "__main__":
    main()
