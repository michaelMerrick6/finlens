import hashlib
import re
from collections import defaultdict
from datetime import date, timedelta
from uuid import NAMESPACE_URL, uuid5

from notification_targets import normalize_actor_key
from alert_rules import classify_event_behavior


GROUPABLE_SIGNAL_TYPES = {"politician_trade", "insider_trade"}
FILING_SUMMARY_SIGNAL_TYPES = {"politician_filing_summary", "insider_filing_summary"}


def stable_id(parts: list[str]) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def stable_uuid(parts: list[str]) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(parts)))


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def extract_sec_accession(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    match = re.search(r"(\d{10}-\d{2}-\d{6})", raw)
    if match:
        return match.group(1)
    match = re.search(r"/(\d{10}\d{6})/", raw)
    if match:
        compact = match.group(1)
        return f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"
    return None


def actor_match_key(event: dict) -> str:
    payload = event.get("payload") or {}
    member_id = str(payload.get("member_id") or "").strip().lower()
    if member_id:
        return member_id
    return normalize_actor_key(event.get("actor_name") or "")


def filing_group_key(event: dict) -> str:
    source = str(event.get("source") or "").strip().lower()
    source_document_id = str(event.get("source_document_id") or "").strip()
    payload = event.get("payload") or {}

    if source == "congress" and source_document_id:
        return re.sub(r"-\d+$", "", source_document_id)

    if source == "insider":
        source_url = str(event.get("source_url") or payload.get("source_url") or "").strip()
        accession = extract_sec_accession(source_url)
        if accession:
            return accession
        if source_url:
            return source_url

    return source_document_id


def grouped_source_document_id(group_key: str, event: dict) -> str:
    ticker = (event.get("ticker") or "").upper()
    direction = str(event.get("direction") or "unknown").lower()
    actor_key = actor_match_key(event) or normalize_actor_key(event.get("actor_name") or "unknown")
    return f"{group_key}::group::{ticker}::{direction}::{actor_key}"


def event_trade_count(event: dict) -> int:
    payload = event.get("payload") or {}
    return int(payload.get("group_row_count") or 1)


def dedupe_group_rows(source: str, rows: list[dict]) -> list[dict]:
    if source != "insider":
        return rows

    deduped: dict[tuple[str, str, str, str, str, str, str], dict] = {}
    for row in rows:
        payload = row.get("payload") or {}
        key = (
            actor_match_key(row),
            (row.get("ticker") or "").upper(),
            str(row.get("direction") or "").lower(),
            str(row.get("occurred_at") or ""),
            str(payload.get("amount") or ""),
            str(payload.get("price") or ""),
            str(payload.get("value") or ""),
        )
        deduped[key] = row
    return list(deduped.values())


def build_grouped_event(group_key: str, events: list[dict]) -> dict:
    base = events[0]
    row_count = len(events)
    ticker = (base.get("ticker") or "").upper()
    direction = str(base.get("direction") or "unknown").lower()
    actor_name = base.get("actor_name") or "Unknown"
    source = str(base.get("source") or "").lower()
    actor_type = base.get("actor_type") or "unknown"
    published_at = max((event.get("published_at") for event in events if event.get("published_at")), default=base.get("published_at"))
    occurred_at = max((event.get("occurred_at") for event in events if event.get("occurred_at")), default=base.get("occurred_at"))
    source_urls = [event.get("source_url") for event in events if event.get("source_url")]
    importance = max(float(event.get("importance_score") or 0) for event in events)
    importance = round(min(0.99, importance + min(0.12, 0.04 * (row_count - 1))), 2)

    if source == "congress":
        title = f"Congress filing: {actor_name} reported {row_count} {ticker} {direction}s"
        summary = f"{actor_name} reported {row_count} congressional {direction} trades in {ticker} in the same filing."
        signal_type = "politician_trade_grouped"
    else:
        title = f"Insider filing: {actor_name} reported {row_count} {ticker} {direction}s"
        summary = f"{actor_name} reported {row_count} insider {direction} trades in {ticker} in the same filing."
        signal_type = "insider_trade_grouped"

    payload = dict(base.get("payload") or {})
    payload.update(
        {
            "compiled_notification_event": True,
            "base_signal_type": base.get("signal_type"),
            "base_source": source,
            "group_type": "same_filing_same_ticker",
            "group_row_count": row_count,
            "group_source_document_id": group_key,
            "group_event_ids": [event["id"] for event in events],
            "group_source_document_ids": [event.get("source_document_id") for event in events],
        }
    )

    return {
        "id": stable_uuid(["notification", signal_type, grouped_source_document_id(group_key, base)]),
        "source": source,
        "signal_type": signal_type,
        "source_document_id": grouped_source_document_id(group_key, base),
        "ticker": ticker,
        "actor_name": actor_name,
        "actor_type": actor_type,
        "direction": direction,
        "occurred_at": occurred_at,
        "published_at": published_at,
        "importance_score": importance,
        "title": title,
        "summary": summary,
        "source_url": source_urls[0] if source_urls else base.get("source_url"),
        "payload": payload,
        "created_at": max((event.get("created_at") for event in events if event.get("created_at")), default=base.get("created_at")),
    }


def compile_grouped_events(events: list[dict]) -> list[dict]:
    grouped_events: list[dict] = []
    group_buckets: dict[tuple[str, str, str, str, str], list[dict]] = defaultdict(list)

    for event in events:
        signal_type = str(event.get("signal_type") or "").lower()
        if signal_type not in GROUPABLE_SIGNAL_TYPES:
            grouped_events.append(event)
            continue

        group_buckets[
            (
                str(event.get("source") or "").lower(),
                filing_group_key(event),
                (event.get("ticker") or "").upper(),
                actor_match_key(event),
                str(event.get("direction") or "unknown").lower(),
            )
        ].append(event)

    for (source, group_key, _ticker, _actor_key, _direction), rows in group_buckets.items():
        unique_rows = dedupe_group_rows(source, rows)
        if len(unique_rows) == 1:
            grouped_events.append(unique_rows[0])
            continue
        grouped_events.append(build_grouped_event(group_key, unique_rows))

    return grouped_events


def build_filing_summary_event(group_key: str, events: list[dict]) -> dict | None:
    if not events:
        return None

    base = events[0]
    source = str(base.get("source") or "").lower()
    signal_type = "politician_filing_summary" if source == "congress" else "insider_filing_summary"
    actor_name = base.get("actor_name") or "Unknown"
    actor_type = base.get("actor_type") or "unknown"
    actor_key = actor_match_key(base) or normalize_actor_key(actor_name)
    entries = []
    tickers = []
    trade_count = 0
    unusual_count = 0
    unusual_event_ids = []

    for event in events:
        behavior = classify_event_behavior(event)
        if behavior.get("suppressed"):
            continue
        payload = event.get("payload") or {}
        count = event_trade_count(event)
        ticker = (event.get("ticker") or "").upper()
        direction = str(event.get("direction") or "").lower()
        trade_count += count
        if ticker:
            tickers.append(ticker)
            entries.append(
                {
                    "ticker": ticker,
                    "direction": direction,
                    "trade_count": count,
                    "amount_range": payload.get("amount_range"),
                    "value": payload.get("value"),
                    "signal_type": event.get("signal_type"),
                    "event_id": event["id"],
                }
            )
        if behavior.get("unusual"):
            unusual_count += count
            unusual_event_ids.append(event["id"])

    if trade_count <= 1:
        return None

    unique_tickers = []
    for ticker in tickers:
        if ticker not in unique_tickers:
            unique_tickers.append(ticker)

    if source == "congress":
        title = f"Congress filing: {actor_name} filed {trade_count} trades"
        summary_prefix = f"{actor_name} reported {trade_count} congressional trades"
    else:
        title = f"Insider filing: {actor_name} filed {trade_count} trades"
        summary_prefix = f"{actor_name} reported {trade_count} insider trades"

    summary_suffix = ", ".join(
        f"{entry['ticker']} {entry['direction']}" if entry.get("direction") else entry["ticker"] for entry in entries[:5]
    )
    summary = summary_prefix
    if unique_tickers:
        summary += f" across {len(unique_tickers)} ticker{'s' if len(unique_tickers) != 1 else ''}"
    if summary_suffix:
        summary += f": {summary_suffix}."
    else:
        summary += "."

    importance = max(float(event.get("importance_score") or 0) for event in events)
    if unusual_count:
        importance = round(min(0.99, importance + 0.08), 2)
    else:
        importance = round(min(0.99, importance + min(0.08, 0.02 * max(trade_count - 1, 0))), 2)

    base_payload = dict(base.get("payload") or {})
    base_payload.update(
        {
            "compiled_notification_event": True,
            "base_signal_type": "politician_trade" if source == "congress" else "insider_trade",
            "base_source": source,
            "summary_type": "same_filing_actor_summary",
            "summary_filing_key": group_key,
            "summary_trade_count": trade_count,
            "summary_ticker_count": len(unique_tickers),
            "summary_tickers": unique_tickers[:10],
            "summary_entries": entries[:10],
            "summary_event_ids": [event["id"] for event in events],
            "summary_contains_activity": True,
            "summary_contains_unusual": bool(unusual_count),
            "summary_unusual_event_ids": unusual_event_ids,
        }
    )

    return {
        "id": stable_uuid(["notification", signal_type, f"{group_key}::summary::{actor_key}"]),
        "source": source,
        "signal_type": signal_type,
        "source_document_id": f"{group_key}::summary::{actor_key}",
        "ticker": "MULTI",
        "actor_name": actor_name,
        "actor_type": actor_type,
        "direction": None,
        "occurred_at": max((event.get("occurred_at") for event in events if event.get("occurred_at")), default=base.get("occurred_at")),
        "published_at": max((event.get("published_at") for event in events if event.get("published_at")), default=base.get("published_at")),
        "importance_score": importance,
        "title": title,
        "summary": summary,
        "source_url": base.get("source_url"),
        "payload": base_payload,
        "created_at": max((event.get("created_at") for event in events if event.get("created_at")), default=base.get("created_at")),
    }


def compile_filing_summary_events(grouped_events: list[dict]) -> list[dict]:
    summary_buckets: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for event in grouped_events:
        signal_type = str(event.get("signal_type") or "").lower()
        if signal_type not in GROUPABLE_SIGNAL_TYPES and signal_type not in {"politician_trade_grouped", "insider_trade_grouped"}:
            continue
        source = str(event.get("source") or "").lower()
        group_key = filing_group_key(event)
        actor_key = actor_match_key(event)
        summary_buckets[(source, group_key, actor_key)].append(event)

    summary_events = []
    for (_source, group_key, _actor_key), rows in summary_buckets.items():
        summary_event = build_filing_summary_event(group_key, rows)
        if summary_event:
            summary_events.append(summary_event)
    return summary_events


def build_cluster_event(ticker: str, direction: str, events: list[dict], *, window_days: int) -> dict:
    actor_rows = []
    seen_actor_keys: set[str] = set()
    for event in sorted(events, key=lambda row: (row.get("published_at") or "", row.get("actor_name") or "")):
        key = actor_match_key(event)
        if not key or key in seen_actor_keys:
            continue
        seen_actor_keys.add(key)
        payload = event.get("payload") or {}
        actor_rows.append(
            {
                "name": event.get("actor_name"),
                "member_id": payload.get("member_id"),
                "published_at": event.get("published_at"),
                "source_document_id": event.get("source_document_id"),
            }
        )

    latest_published = max((event.get("published_at") for event in events if event.get("published_at")), default=None)
    created_at = max((event.get("created_at") for event in events if event.get("created_at")), default=None)
    actor_names = [row["name"] for row in actor_rows if row.get("name")]
    actor_count = len(actor_rows)
    title = f"Congress cluster: {actor_count} members {direction} {ticker} in {window_days} days"
    summary = f"{actor_count} distinct Congress members reported {direction} trades in {ticker} within {window_days} days: {', '.join(actor_names[:5])}."
    importance = round(min(0.99, 0.84 + min(0.12, 0.04 * max(actor_count - 2, 0))), 2)

    payload = {
        "compiled_notification_event": True,
        "base_signal_type": "politician_trade",
        "base_source": "congress",
        "cluster_type": "congress_same_ticker_same_direction",
        "cluster_actor_count": actor_count,
        "cluster_window_days": window_days,
        "cluster_actors": actor_rows,
        "cluster_event_ids": [event["id"] for event in events],
    }
    actor_hash = stable_id(sorted(seen_actor_keys))
    source_document_id = f"cluster::{ticker}::{direction}::{window_days}d::{actor_hash}"

    return {
        "id": stable_uuid(["notification", "politician_cluster", source_document_id]),
        "source": "congress",
        "signal_type": "politician_cluster",
        "source_document_id": source_document_id,
        "ticker": ticker,
        "actor_name": ", ".join(actor_names[:5]),
        "actor_type": "cluster",
        "direction": direction,
        "occurred_at": latest_published,
        "published_at": latest_published,
        "importance_score": importance,
        "title": title,
        "summary": summary,
        "source_url": None,
        "payload": payload,
        "created_at": created_at,
    }


def compile_congress_cluster_events(
    grouped_events: list[dict], *, window_days: int = 7, min_members: int = 2
) -> list[dict]:
    cluster_candidates = []
    today = max((parse_iso_date(event.get("published_at")) for event in grouped_events if event.get("published_at")), default=None)
    if today is None:
        return []
    cutoff = today - timedelta(days=window_days - 1)

    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for event in grouped_events:
        if str(event.get("source") or "").lower() != "congress":
            continue
        direction = str(event.get("direction") or "").lower()
        if direction not in {"buy", "sell"}:
            continue
        published = parse_iso_date(event.get("published_at"))
        if not published or published < cutoff:
            continue
        ticker = (event.get("ticker") or "").upper()
        if not ticker:
            continue
        buckets[(ticker, direction)].append(event)

    for (ticker, direction), events in buckets.items():
        actor_keys = {actor_match_key(event) for event in events if actor_match_key(event)}
        if len(actor_keys) < min_members:
            continue
        cluster_candidates.append(build_cluster_event(ticker, direction, events, window_days=window_days))

    return cluster_candidates


def compile_notification_events(
    events: list[dict], *, congress_cluster_window_days: int = 7, congress_cluster_min_members: int = 2
) -> list[dict]:
    grouped = compile_grouped_events(events)
    filing_summaries = compile_filing_summary_events(grouped)
    clusters = compile_congress_cluster_events(
        grouped,
        window_days=congress_cluster_window_days,
        min_members=congress_cluster_min_members,
    )
    compiled = grouped + filing_summaries + clusters
    deduped: dict[str, dict] = {}
    for event in compiled:
        deduped[str(event["id"])] = event
    return list(deduped.values())


def is_compiled_notification_event(event: dict) -> bool:
    return bool((event.get("payload") or {}).get("compiled_notification_event"))
