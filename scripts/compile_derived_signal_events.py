import os
from datetime import timedelta

from notification_compiler import compile_notification_events, is_compiled_notification_event
from pipeline_support import emit_summary, get_supabase_client, utc_now
from politician_gain_signals import (
    CLUSTER_GAIN_LOOKBACK_DAYS,
    POLITICIAN_GAIN_LOOKBACK_DAYS,
    build_cluster_gain_milestone_events,
    build_politician_gain_milestone_events,
)
from signal_baseline_support import enrich_events_with_baseline_snapshots


LOOKBACK_HOURS = int(os.environ.get("ALERT_QUEUE_LOOKBACK_HOURS", "48"))
CONGRESS_CLUSTER_WINDOW_DAYS = int(os.environ.get("CONGRESS_CLUSTER_WINDOW_DAYS", "10"))
CONGRESS_CLUSTER_MIN_MEMBERS = int(os.environ.get("CONGRESS_CLUSTER_MIN_MEMBERS", "2"))
INSIDER_CLUSTER_WINDOW_DAYS = int(os.environ.get("INSIDER_CLUSTER_WINDOW_DAYS", "10"))
INSIDER_CLUSTER_MIN_MEMBERS = int(os.environ.get("INSIDER_CLUSTER_MIN_MEMBERS", "2"))
CROSS_SOURCE_CLUSTER_WINDOW_DAYS = int(os.environ.get("CROSS_SOURCE_CLUSTER_WINDOW_DAYS", "45"))
FUND_ALIGNMENT_WINDOW_DAYS = int(os.environ.get("FUND_ALIGNMENT_WINDOW_DAYS", "120"))
UPSERT_CHUNK_SIZE = int(os.environ.get("DERIVED_SIGNAL_UPSERT_CHUNK_SIZE", "100"))
RAW_SIGNAL_TYPES = ["politician_trade", "insider_trade", "fund_position_change"]
CLUSTER_SIGNAL_TYPES = ["politician_cluster", "insider_cluster", "cross_source_accumulation"]
ENABLE_GAIN_MILESTONES = os.environ.get("DERIVED_SIGNAL_ENABLE_GAIN_MILESTONES", "0").strip() == "1"
ENABLE_BASELINE_ENRICHMENT = os.environ.get("DERIVED_SIGNAL_ENRICH_BASELINES", "0").strip() == "1"


def fetch_signal_events_since(supabase, *, column: str, since_value: str, signal_types: list[str]):
    rows: list[dict] = []
    page_size = 1000
    start = 0
    while True:
        # Production PostgREST statement timeouts were triggered by broad
        # multi-column ORDER BY scans. The compiler does not require database
        # ordering because batches are merged and sorted in memory below.
        response = (
            supabase.table("signal_events")
            .select("*")
            .in_("signal_type", signal_types)
            .gte(column, since_value)
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


def signal_event_identity(event: dict) -> tuple[str, ...]:
    event_id = str(event.get("id") or "").strip()
    if event_id:
        return ("id", event_id)
    return (
        str(event.get("source") or "").strip().lower(),
        str(event.get("source_document_id") or "").strip(),
    )


def merge_signal_event_batches(*batches: list[dict]) -> list[dict]:
    merged: dict[tuple[str, ...], dict] = {}
    for batch in batches:
        for event in batch:
            merged[signal_event_identity(event)] = event
    return sorted(
        merged.values(),
        key=lambda event: (str(event.get("created_at") or ""), str(event.get("published_at") or ""), str(event.get("id") or "")),
        reverse=True,
    )


def fetch_recent_signal_events(supabase):
    actor_signal_hours = max(
        LOOKBACK_HOURS,
        CONGRESS_CLUSTER_WINDOW_DAYS * 24,
        INSIDER_CLUSTER_WINDOW_DAYS * 24,
        CROSS_SOURCE_CLUSTER_WINDOW_DAYS * 24,
    )
    actor_published_since = (utc_now() - timedelta(hours=actor_signal_hours)).date().isoformat()
    fund_published_since = (utc_now() - timedelta(days=FUND_ALIGNMENT_WINDOW_DAYS)).date().isoformat()
    created_since = (utc_now() - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    actor_published_rows = fetch_signal_events_since(
        supabase,
        column="published_at",
        since_value=actor_published_since,
        signal_types=["politician_trade", "insider_trade"],
    )
    fund_published_rows = fetch_signal_events_since(
        supabase,
        column="published_at",
        since_value=fund_published_since,
        signal_types=["fund_position_change"],
    )
    created_rows = fetch_signal_events_since(
        supabase,
        column="created_at",
        since_value=created_since,
        signal_types=RAW_SIGNAL_TYPES,
    )
    return merge_signal_event_batches(actor_published_rows, fund_published_rows, created_rows)


def fetch_recent_politician_buy_signal_events(supabase):
    since_date = (utc_now() - timedelta(days=POLITICIAN_GAIN_LOOKBACK_DAYS)).date().isoformat()
    rows: list[dict] = []
    start = 0
    page_size = 1000
    while True:
        # Avoid a wide ordered scan; callers only need the recent set.
        response = (
            supabase.table("signal_events")
            .select("*")
            .eq("source", "congress")
            .eq("signal_type", "politician_trade")
            .eq("direction", "buy")
            .gte("occurred_at", since_date)
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


def fetch_recent_cluster_signal_events(supabase):
    since_date = (utc_now() - timedelta(days=CLUSTER_GAIN_LOOKBACK_DAYS)).date().isoformat()
    rows: list[dict] = []
    start = 0
    page_size = 1000
    while True:
        # Avoid ordering in the DB; this query can span many recent cluster rows
        # and ordering has caused production statement timeouts.
        response = (
            supabase.table("signal_events")
            .select("*")
            .in_("signal_type", CLUSTER_SIGNAL_TYPES)
            .gte("published_at", since_date)
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


def chunked_upsert_signal_events(supabase, rows: list[dict]) -> None:
    if not rows:
        return
    now_iso = utc_now().isoformat()
    for start in range(0, len(rows), UPSERT_CHUNK_SIZE):
        chunk = []
        for row in rows[start : start + UPSERT_CHUNK_SIZE]:
            prepared = dict(row)
            prepared.setdefault("created_at", now_iso)
            prepared["updated_at"] = now_iso
            chunk.append(prepared)
        supabase.table("signal_events").upsert(
            chunk,
            on_conflict="source,source_document_id",
        ).execute()


def main():
    print("Compiling derived signal events...")
    supabase = get_supabase_client()

    db_events = fetch_recent_signal_events(supabase)
    raw_events = [event for event in db_events if not is_compiled_notification_event(event)]
    print(f"Loaded {len(raw_events)} raw signal events for derived compilation.", flush=True)

    compiled_events = compile_notification_events(
        raw_events,
        congress_cluster_window_days=CONGRESS_CLUSTER_WINDOW_DAYS,
        congress_cluster_min_members=CONGRESS_CLUSTER_MIN_MEMBERS,
        insider_cluster_window_days=INSIDER_CLUSTER_WINDOW_DAYS,
        insider_cluster_min_members=INSIDER_CLUSTER_MIN_MEMBERS,
        cross_source_window_days=CROSS_SOURCE_CLUSTER_WINDOW_DAYS,
        fund_window_days=FUND_ALIGNMENT_WINDOW_DAYS,
    )

    politician_buy_events: list[dict] = []
    cluster_signal_events: list[dict] = []
    if ENABLE_GAIN_MILESTONES:
        politician_buy_events = fetch_recent_politician_buy_signal_events(supabase)
        cluster_signal_events = fetch_recent_cluster_signal_events(supabase)
        print(
            "Loaded supporting gain datasets: "
            f"{len(politician_buy_events)} politician buys, {len(cluster_signal_events)} cluster signals.",
            flush=True,
        )
        compiled_events.extend(build_politician_gain_milestone_events(politician_buy_events))
        compiled_events.extend(build_cluster_gain_milestone_events(cluster_signal_events))
    else:
        print("Skipping gain-milestone compilation in the default derived signal path.", flush=True)

    if ENABLE_BASELINE_ENRICHMENT:
        compiled_events = enrich_events_with_baseline_snapshots(compiled_events)
    else:
        print("Skipping baseline enrichment for derived signals in the default path.", flush=True)

    if compiled_events:
        print(f"Upserting {len(compiled_events)} derived signal events in chunks of {UPSERT_CHUNK_SIZE}.", flush=True)
        chunked_upsert_signal_events(supabase, compiled_events)

    summary = {
        "raw_signal_events_seen": len(raw_events),
        "compiled_signal_events_upserted": len(compiled_events),
        "congress_cluster_window_days": CONGRESS_CLUSTER_WINDOW_DAYS,
        "insider_cluster_window_days": INSIDER_CLUSTER_WINDOW_DAYS,
        "cross_source_cluster_window_days": CROSS_SOURCE_CLUSTER_WINDOW_DAYS,
        "fund_alignment_window_days": FUND_ALIGNMENT_WINDOW_DAYS,
        "compiled_grouped_congress_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "politician_trade_grouped"
        ),
        "compiled_grouped_insider_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "insider_trade_grouped"
        ),
        "compiled_cluster_events": sum(1 for event in compiled_events if event.get("signal_type") == "politician_cluster"),
        "compiled_insider_cluster_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "insider_cluster"
        ),
        "compiled_cross_source_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "cross_source_accumulation"
        ),
        "compiled_politician_gain_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "politician_gain_milestone"
        ),
        "compiled_cluster_gain_events": sum(
            1 for event in compiled_events if event.get("signal_type") == "cluster_gain_milestone"
        ),
        "derived_upsert_chunk_size": UPSERT_CHUNK_SIZE,
        "gain_milestones_enabled": ENABLE_GAIN_MILESTONES,
        "derived_baseline_enrichment_enabled": ENABLE_BASELINE_ENRICHMENT,
    }
    emit_summary(summary)
    print(
        "Derived signal compilation complete: "
        f"{summary['compiled_signal_events_upserted']} compiled rows "
        f"from {summary['raw_signal_events_seen']} recent raw signal events."
    )


if __name__ == "__main__":
    main()
