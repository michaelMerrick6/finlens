import re
from collections import defaultdict
from datetime import date, timedelta
from uuid import NAMESPACE_URL, uuid5

from shared_utils import extract_sec_accession, stable_id

from notification_targets import normalize_actor_key
from alert_rules import classify_event_behavior, parse_amount_lower_bound


GROUPABLE_SIGNAL_TYPES = {"politician_trade", "insider_trade"}
FILING_SUMMARY_SIGNAL_TYPES = {"politician_filing_summary", "insider_filing_summary"}
UNPUBLISHABLE_CLUSTER_TICKERS = {"", "N/A", "NA", "UNKNOWN", "US-TREAS", "MULTI"}
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{0,4}(?:[.-][A-Z])?$")
DEFAULT_INSIDER_CLUSTER_MIN_MEMBERS = 5

def stable_uuid(parts: list[str]) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(parts)))


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def publishable_cluster_ticker(value: object) -> str:
    ticker = str(value or "").strip().upper()
    if ticker in UNPUBLISHABLE_CLUSTER_TICKERS:
        return ""
    if not TICKER_PATTERN.fullmatch(ticker):
        return ""
    return ticker

def actor_match_key(event: dict) -> str:
    payload = event.get("payload") or {}
    member_id = str(payload.get("member_id") or "").strip().lower()
    if member_id:
        return member_id
    return normalize_actor_key(event.get("actor_name") or "")


def latest_signal_date(events: list[dict]) -> str | None:
    return max(
        (
            str(event.get("published_at") or event.get("occurred_at") or "").strip()[:10]
            for event in events
            if str(event.get("published_at") or event.get("occurred_at") or "").strip()
        ),
        default=None,
    )


def latest_published_date(events: list[dict]) -> date | None:
    return max((parse_iso_date(event.get("published_at")) for event in events if event.get("published_at")), default=None)


def events_in_published_window(events: list[dict], anchor: date, window_days: int) -> list[dict]:
    cutoff = anchor - timedelta(days=max(1, window_days) - 1)
    windowed = []
    for event in events:
        published = parse_iso_date(event.get("published_at"))
        if published and cutoff <= published <= anchor:
            windowed.append(event)
    return windowed


def published_anchor_dates(events: list[dict]) -> list[date]:
    return sorted(
        {
            published
            for event in events
            if (published := parse_iso_date(event.get("published_at")))
        }
    )


def non_overlapping_published_windows(events: list[dict], window_days: int) -> list[tuple[date, list[dict]]]:
    """Partition events into deterministic fixed windows with no overlapping subsets."""
    dated_events = sorted(
        (
            (published, event)
            for event in events
            if (published := parse_iso_date(event.get("published_at")))
        ),
        key=lambda item: (item[0], str(item[1].get("id") or "")),
    )
    windows: list[tuple[date, list[dict]]] = []
    index = 0
    duration = max(1, int(window_days))
    while index < len(dated_events):
        window_start = dated_events[index][0]
        window_end = window_start + timedelta(days=duration - 1)
        window_events: list[dict] = []
        while index < len(dated_events) and dated_events[index][0] <= window_end:
            window_events.append(dated_events[index][1])
            index += 1
        windows.append((window_start, window_events))
    return windows


def distinct_actor_keys(events: list[dict]) -> set[str]:
    return {actor_match_key(event) for event in events if actor_match_key(event)}


def strong_fund_alignment(fund_events: list[dict]) -> bool:
    fund_actors = distinct_actor_keys(fund_events)
    if len(fund_actors) >= 2:
        return True
    return any(float(event.get("importance_score") or 0) >= 0.70 for event in fund_events)


def fund_change_type(event: dict) -> str:
    return str((event.get("payload") or {}).get("change_type") or "").strip().lower()


def is_fund_accumulation_event(event: dict) -> bool:
    direction = str(event.get("direction") or "").strip().lower()
    return direction == "increase" or fund_change_type(event) in {"new", "increase"}


def is_fund_distribution_event(event: dict) -> bool:
    direction = str(event.get("direction") or "").strip().lower()
    return direction == "decrease" or fund_change_type(event) in {"exit", "decrease"}


def unique_actor_events(events: list[dict]) -> list[dict]:
    by_actor: dict[str, dict] = {}
    for event in sorted(events, key=lambda row: (row.get("published_at") or "", row.get("created_at") or "")):
        key = actor_match_key(event)
        if key and key not in by_actor:
            by_actor[key] = event
    return list(by_actor.values())


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


def event_amount_floor(event: dict) -> float:
    payload = event.get("payload") or {}
    return max(
        parse_amount_lower_bound(payload.get("amount_range")),
        float(payload.get("insider_total_buy_value") or 0),
        float(payload.get("insider_total_sell_value") or 0),
        float(payload.get("insider_change_value") or 0),
        float(payload.get("value") or 0),
        float(payload.get("amount") or 0),
    )


def normalized_numeric_component(value) -> str:
    try:
        numeric = float(value or 0)
    except (TypeError, ValueError):
        return ""
    if not numeric:
        return ""
    return f"{numeric:.4f}".rstrip("0").rstrip(".")


def insider_economic_signature(event: dict) -> tuple[str, ...]:
    payload = event.get("payload") or {}
    ticker = publishable_cluster_ticker(event.get("ticker") or payload.get("ticker"))
    direction = str(event.get("direction") or payload.get("direction") or "").strip().lower()
    group_value = normalized_numeric_component(payload.get("group_combined_lower_bound"))
    group_count = str(payload.get("group_row_count") or "").strip()
    group_start = str(payload.get("group_trade_date_start") or event.get("occurred_at") or "").strip()[:10]
    group_end = str(payload.get("group_trade_date_end") or event.get("occurred_at") or "").strip()[:10]

    if group_value and group_count:
        return ("group", ticker, direction, group_start, group_end, group_count, group_value)

    value = normalized_numeric_component(payload.get("value") or event_amount_floor(event))
    return (
        "single",
        ticker,
        direction,
        str(payload.get("transaction_date") or event.get("occurred_at") or "").strip()[:10],
        normalized_numeric_component(payload.get("amount")),
        normalized_numeric_component(payload.get("price")),
        value,
        str(payload.get("amount_range") or "").strip(),
    )


def unique_insider_economic_events(events: list[dict]) -> list[dict]:
    by_signature: dict[tuple[str, ...], dict] = {}
    for event in events:
        if str(event.get("source") or "").strip().lower() != "insider":
            continue
        signature = insider_economic_signature(event)
        existing = by_signature.get(signature)
        if existing is None or float(event.get("importance_score") or 0) > float(existing.get("importance_score") or 0):
            by_signature[signature] = event
    return list(by_signature.values())


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
    combined_floor = sum(event_amount_floor(event) for event in events)
    occurred_dates = sorted({str(event.get("occurred_at") or "").strip()[:10] for event in events if str(event.get("occurred_at") or "").strip()})
    amount_ranges = [
        str((event.get("payload") or {}).get("amount_range") or "").strip()
        for event in events
        if str((event.get("payload") or {}).get("amount_range") or "").strip()
    ]
    unique_amount_ranges = list(dict.fromkeys(amount_ranges))

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
            "group_combined_lower_bound": combined_floor,
            "group_amount_ranges": unique_amount_ranges,
            "group_trade_date_start": occurred_dates[0] if occurred_dates else None,
            "group_trade_date_end": occurred_dates[-1] if occurred_dates else None,
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
    combined_lower_bound = 0.0
    for event in sorted(events, key=lambda row: (row.get("published_at") or "", row.get("actor_name") or "")):
        key = actor_match_key(event)
        if not key or key in seen_actor_keys:
            continue
        seen_actor_keys.add(key)
        payload = event.get("payload") or {}
        combined_lower_bound += parse_amount_lower_bound(payload.get("amount_range"))
        actor_rows.append(
            {
                "name": event.get("actor_name"),
                "member_id": payload.get("member_id"),
                "amount_range": payload.get("amount_range"),
                "published_at": event.get("published_at"),
                "source_document_id": event.get("source_document_id"),
            }
        )

    latest_published = latest_signal_date(events)
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
        "cluster_clocked_at": latest_published,
        "cluster_actors": actor_rows,
        "cluster_combined_lower_bound": combined_lower_bound,
        "cluster_event_ids": [event["id"] for event in events],
    }
    actor_hash = stable_id(sorted(seen_actor_keys))
    cluster_anchor = latest_published or "unknown"
    source_document_id = f"cluster::{ticker}::{direction}::{window_days}d::{cluster_anchor}::{actor_hash}"

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
    grouped_events: list[dict], *, window_days: int = 10, min_members: int = 2
) -> list[dict]:
    cluster_candidates = []
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for event in grouped_events:
        if str(event.get("source") or "").lower() != "congress":
            continue
        direction = str(event.get("direction") or "").lower()
        if direction not in {"buy", "sell"}:
            continue
        if not parse_iso_date(event.get("published_at")):
            continue
        ticker = publishable_cluster_ticker(event.get("ticker"))
        if not ticker:
            continue
        buckets[(ticker, direction)].append(event)

    for (ticker, direction), events in buckets.items():
        seen_cluster_keys: set[tuple[str, str, str, tuple[str, ...]]] = set()
        for anchor in published_anchor_dates(events):
            window_events = events_in_published_window(events, anchor, window_days)
            actor_keys = distinct_actor_keys(window_events)
            if len(actor_keys) < min_members:
                continue
            cluster_key = (ticker, direction, anchor.isoformat(), tuple(sorted(actor_keys)))
            if cluster_key in seen_cluster_keys:
                continue
            seen_cluster_keys.add(cluster_key)
            cluster_candidates.append(build_cluster_event(ticker, direction, window_events, window_days=window_days))

    return cluster_candidates


def cluster_actor_row(event: dict) -> dict:
    payload = event.get("payload") or {}
    actor_type = str(event.get("actor_type") or "").strip().lower()
    row = {
        "name": event.get("actor_name"),
        "actor_type": actor_type or str(event.get("source") or "").strip().lower(),
        "source": str(event.get("source") or "").strip().lower(),
        "published_at": event.get("published_at"),
        "source_document_id": event.get("source_document_id"),
    }
    if actor_type == "politician":
        row["member_id"] = payload.get("member_id")
        row["amount_range"] = payload.get("amount_range")
    if actor_type == "insider":
        row["filer_relation"] = payload.get("filer_relation")
    return row


def build_cross_source_accumulation_event(
    ticker: str,
    congress_events: list[dict],
    insider_events: list[dict],
    fund_events: list[dict],
    *,
    window_days: int,
    fund_window_days: int,
    window_start: date,
) -> dict:
    congress_actor_rows = [cluster_actor_row(event) for event in congress_events]
    insider_actor_rows = [cluster_actor_row(event) for event in insider_events]
    fund_actor_rows = [cluster_actor_row(event) for event in fund_events]
    all_actor_rows = congress_actor_rows + insider_actor_rows + fund_actor_rows
    actor_names = [str(row.get("name") or "").strip() for row in all_actor_rows if str(row.get("name") or "").strip()]

    includes_fund = bool(fund_actor_rows)
    source_labels = []
    if congress_actor_rows:
        source_labels.append("Congress")
    if insider_actor_rows:
        source_labels.append("insiders")
    if fund_actor_rows:
        source_labels.append("funds")
    title_prefix = "Full-stack accumulation" if len(source_labels) >= 3 else "Cross-source accumulation"
    title = f"{title_prefix}: {' + '.join(source_labels)} on {ticker}"
    summary = (
        f"{ticker} saw aligned accumulation across {' + '.join(source_labels)} "
        f"within {window_days} days"
    )
    if includes_fund:
        summary += f" (fund window {fund_window_days} days)"
    summary += "."

    if actor_names:
        summary += f" Actors: {', '.join(actor_names[:6])}."

    importance = 0.9
    if includes_fund:
        importance += 0.04
    total_actor_count = len(congress_actor_rows) + len(insider_actor_rows) + len(fund_actor_rows)
    combined_lower_bound = sum(event_amount_floor(event) for event in congress_events + insider_events)
    importance += min(0.05, 0.01 * max(total_actor_count - 2, 0))
    importance = round(min(0.99, importance), 2)

    latest_published = latest_signal_date(congress_events + insider_events + fund_events)
    created_at = max(
        (
            event.get("created_at")
            for event in congress_events + insider_events + fund_events
            if event.get("created_at")
        ),
        default=None,
    )
    source_document_id = f"cross-source::{ticker}::buy::{window_days}d::{fund_window_days}fd::{window_start.isoformat()}"

    payload = {
        "compiled_notification_event": True,
        "base_signal_type": "cross_source_accumulation",
        "base_source": "cross_source",
        "cluster_type": "cross_source_accumulation",
        "cluster_sources": [
            source
            for source, rows in (
                ("congress", congress_actor_rows),
                ("insider", insider_actor_rows),
                ("hedge_fund", fund_actor_rows),
            )
            if rows
        ],
        "cluster_actor_count": total_actor_count,
        "cluster_window_days": window_days,
        "cluster_window_start": window_start.isoformat(),
        "cluster_window_end": (window_start + timedelta(days=max(1, window_days) - 1)).isoformat(),
        "fund_window_days": fund_window_days,
        "cluster_clocked_at": latest_published,
        "cluster_combined_lower_bound": combined_lower_bound,
        "congress_actor_count": len(congress_actor_rows),
        "insider_actor_count": len(insider_actor_rows),
        "fund_actor_count": len(fund_actor_rows),
        "cluster_actors": all_actor_rows,
        "cluster_event_ids": [event["id"] for event in congress_events + insider_events + fund_events],
        "includes_fund_source": includes_fund,
    }

    return {
        "id": stable_uuid(["notification", "cross_source_accumulation", source_document_id]),
        "source": "cross_source",
        "signal_type": "cross_source_accumulation",
        "source_document_id": source_document_id,
        "ticker": ticker,
        "actor_name": ", ".join(actor_names[:6]),
        "actor_type": "cluster",
        "direction": "buy",
        "occurred_at": latest_published,
        "published_at": latest_published,
        "importance_score": importance,
        "title": title,
        "summary": summary,
        "source_url": None,
        "payload": payload,
        "created_at": created_at,
    }


def compile_cross_source_accumulation_events(
    grouped_events: list[dict], *, window_days: int = 45, fund_window_days: int = 120
) -> list[dict]:
    buckets: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"congress": [], "insider": [], "hedge_fund": []})

    for event in grouped_events:
        source = str(event.get("source") or "").strip().lower()
        direction = str(event.get("direction") or "").strip().lower()
        published = parse_iso_date(event.get("published_at"))
        ticker = publishable_cluster_ticker(event.get("ticker"))
        if not ticker or not published:
            continue

        if source == "congress" and direction == "buy":
            buckets[ticker]["congress"].append(event)
        elif source == "insider" and direction == "buy":
            buckets[ticker]["insider"].append(event)
        elif source == "hedge_fund" and is_fund_accumulation_event(event):
            buckets[ticker]["hedge_fund"].append(event)

    compiled: list[dict] = []
    for ticker, grouped in buckets.items():
        primary_events = grouped["congress"] + grouped["insider"]
        for window_start, window_events in non_overlapping_published_windows(primary_events, window_days):
            window_end = window_start + timedelta(days=max(1, window_days) - 1)
            congress_events = [event for event in window_events if str(event.get("source") or "").strip().lower() == "congress"]
            insider_events = unique_insider_economic_events(
                [event for event in window_events if str(event.get("source") or "").strip().lower() == "insider"]
            )
            fund_events = events_in_published_window(grouped["hedge_fund"], window_end, fund_window_days)
            congress_actors = distinct_actor_keys(congress_events)
            insider_actors = {":".join(insider_economic_signature(event)) for event in insider_events}
            fund_actors = distinct_actor_keys(fund_events)
            source_family_count = sum(1 for actors in (congress_actors, insider_actors, fund_actors) if actors)

            if source_family_count < 2:
                continue
            if not (congress_actors or insider_actors):
                continue
            if not fund_actors and (not congress_actors or not insider_actors):
                continue
            if fund_actors and not (congress_actors and insider_actors) and not strong_fund_alignment(fund_events):
                continue

            compiled.append(
                build_cross_source_accumulation_event(
                    ticker,
                    [event for event in congress_events if actor_match_key(event) in congress_actors],
                    insider_events,
                    [event for event in fund_events if actor_match_key(event) in fund_actors],
                    window_days=window_days,
                    fund_window_days=fund_window_days,
                    window_start=window_start,
                )
            )
    return compiled


def build_cross_source_sell_event(
    ticker: str,
    congress_events: list[dict],
    insider_events: list[dict],
    fund_events: list[dict],
    *,
    window_days: int,
    fund_window_days: int,
    window_start: date,
) -> dict:
    congress_actor_rows = [cluster_actor_row(event) for event in congress_events]
    insider_actor_rows = [cluster_actor_row(event) for event in insider_events]
    fund_actor_rows = [cluster_actor_row(event) for event in fund_events]
    all_actor_rows = congress_actor_rows + insider_actor_rows + fund_actor_rows
    actor_names = [str(row.get("name") or "").strip() for row in all_actor_rows if str(row.get("name") or "").strip()]

    source_labels = []
    if congress_actor_rows:
        source_labels.append("Congress")
    if insider_actor_rows:
        source_labels.append("insiders")
    if fund_actor_rows:
        source_labels.append("funds")
    source_label = " + ".join(source_labels)
    title = f"Cross-source distribution: {source_label} on {ticker}"
    summary = (
        f"{ticker} saw aligned selling or position reductions across {source_label} "
        f"within {window_days} days"
    )
    if fund_actor_rows:
        summary += f" (fund window {fund_window_days} days)"
    summary += "."
    if actor_names:
        summary += f" Actors: {', '.join(actor_names[:6])}."

    importance = 0.88
    if fund_actor_rows:
        importance += 0.04
    total_actor_count = len(congress_actor_rows) + len(insider_actor_rows) + len(fund_actor_rows)
    importance += min(0.05, 0.01 * max(total_actor_count - 2, 0))
    importance = round(min(0.99, importance), 2)

    latest_published = latest_signal_date(congress_events + insider_events + fund_events)
    created_at = max(
        (
            event.get("created_at")
            for event in congress_events + insider_events + fund_events
            if event.get("created_at")
        ),
        default=None,
    )
    source_document_id = f"cross-source::{ticker}::sell::{window_days}d::{fund_window_days}fd::{window_start.isoformat()}"

    payload = {
        "compiled_notification_event": True,
        "base_signal_type": "cross_source_accumulation",
        "base_source": "cross_source",
        "cluster_type": "cross_source_sell",
        "cluster_sources": [
            source
            for source, rows in (
                ("congress", congress_actor_rows),
                ("insider", insider_actor_rows),
                ("hedge_fund", fund_actor_rows),
            )
            if rows
        ],
        "cluster_actor_count": total_actor_count,
        "cluster_window_days": window_days,
        "cluster_window_start": window_start.isoformat(),
        "cluster_window_end": (window_start + timedelta(days=max(1, window_days) - 1)).isoformat(),
        "fund_window_days": fund_window_days,
        "cluster_clocked_at": latest_published,
        "congress_actor_count": len(congress_actor_rows),
        "insider_actor_count": len(insider_actor_rows),
        "fund_actor_count": len(fund_actor_rows),
        "cluster_actors": all_actor_rows,
        "cluster_event_ids": [event["id"] for event in congress_events + insider_events + fund_events],
        "includes_fund_source": bool(fund_actor_rows),
    }

    return {
        "id": stable_uuid(["notification", "cross_source_accumulation", source_document_id]),
        "source": "cross_source",
        "signal_type": "cross_source_accumulation",
        "source_document_id": source_document_id,
        "ticker": ticker,
        "actor_name": ", ".join(actor_names[:6]),
        "actor_type": "cluster",
        "direction": "sell",
        "occurred_at": latest_published,
        "published_at": latest_published,
        "importance_score": importance,
        "title": title,
        "summary": summary,
        "source_url": None,
        "payload": payload,
        "created_at": created_at,
    }


def compile_cross_source_sell_events(
    grouped_events: list[dict], *, window_days: int = 45, fund_window_days: int = 120
) -> list[dict]:
    buckets: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"congress": [], "insider": [], "hedge_fund": []})

    for event in grouped_events:
        source = str(event.get("source") or "").strip().lower()
        direction = str(event.get("direction") or "").strip().lower()
        published = parse_iso_date(event.get("published_at"))
        ticker = publishable_cluster_ticker(event.get("ticker"))
        if not ticker or not published:
            continue

        if source == "congress" and direction == "sell":
            buckets[ticker]["congress"].append(event)
        elif source == "insider" and direction == "sell":
            buckets[ticker]["insider"].append(event)
        elif source == "hedge_fund" and is_fund_distribution_event(event):
            buckets[ticker]["hedge_fund"].append(event)

    compiled: list[dict] = []
    for ticker, grouped in buckets.items():
        primary_events = grouped["congress"] + grouped["insider"]
        for window_start, window_events in non_overlapping_published_windows(primary_events, window_days):
            window_end = window_start + timedelta(days=max(1, window_days) - 1)
            congress_events = [event for event in window_events if str(event.get("source") or "").strip().lower() == "congress"]
            insider_events = unique_insider_economic_events(
                [event for event in window_events if str(event.get("source") or "").strip().lower() == "insider"]
            )
            fund_events = events_in_published_window(grouped["hedge_fund"], window_end, fund_window_days)
            congress_actors = distinct_actor_keys(congress_events)
            insider_actors = {":".join(insider_economic_signature(event)) for event in insider_events}
            fund_actors = distinct_actor_keys(fund_events)
            source_family_count = sum(1 for actors in (congress_actors, insider_actors, fund_actors) if actors)

            if source_family_count < 2:
                continue
            if not (congress_actors or insider_actors):
                continue
            if not fund_actors and (not congress_actors or not insider_actors):
                continue
            if fund_actors and not (congress_actors and insider_actors) and not strong_fund_alignment(fund_events):
                continue

            compiled.append(
                build_cross_source_sell_event(
                    ticker,
                    [event for event in congress_events if actor_match_key(event) in congress_actors],
                    insider_events,
                    [event for event in fund_events if actor_match_key(event) in fund_actors],
                    window_days=window_days,
                    fund_window_days=fund_window_days,
                    window_start=window_start,
                )
            )
    return compiled


def build_insider_cluster_event(
    ticker: str,
    direction: str,
    events: list[dict],
    *,
    window_days: int,
    window_start: date,
) -> dict:
    economic_events = unique_insider_economic_events(events)
    actor_rows = []
    economic_signatures: set[str] = set()
    for event in sorted(economic_events, key=lambda row: (row.get("published_at") or "", row.get("actor_name") or "")):
        signature = ":".join(insider_economic_signature(event))
        if not signature or signature in economic_signatures:
            continue
        economic_signatures.add(signature)
        payload = event.get("payload") or {}
        actor_rows.append(
            {
                "name": event.get("actor_name"),
                "relation": str(payload.get("filer_relation") or "").strip(),
                "value": event_amount_floor(event),
                "published_at": event.get("published_at"),
                "source_document_id": event.get("source_document_id"),
                "economic_signature": signature,
            }
        )

    latest_published = latest_signal_date(events)
    created_at = max((event.get("created_at") for event in events if event.get("created_at")), default=None)
    actor_names = [row["name"] for row in actor_rows if row.get("name")]
    reporting_actor_count = len(distinct_actor_keys(events))
    actor_count = len(economic_signatures)
    total_value = sum(row.get("value") or 0 for row in actor_rows)

    title = f"Insider cluster: {actor_count} economic {direction} group{'s' if actor_count != 1 else ''} in {ticker}"
    summary = f"{actor_count} unique insider economic {direction} group{'s' if actor_count != 1 else ''} appeared in {ticker} within {window_days} days: {', '.join(actor_names[:5])}."
    importance = round(min(0.99, 0.82 + min(0.12, 0.04 * max(actor_count - 2, 0))), 2)

    payload = {
        "compiled_notification_event": True,
        "base_signal_type": "insider_trade",
        "base_source": "insider",
        "cluster_type": "insider_same_ticker_same_direction",
        "cluster_actor_count": actor_count,
        "cluster_window_days": window_days,
        "cluster_window_start": window_start.isoformat(),
        "cluster_window_end": (window_start + timedelta(days=max(1, window_days) - 1)).isoformat(),
        "cluster_clocked_at": latest_published,
        "cluster_actors": actor_rows,
        "cluster_total_value": total_value,
        "cluster_event_ids": [event["id"] for event in economic_events],
        "cluster_raw_event_ids": [event["id"] for event in events],
        "cluster_economic_transaction_count": actor_count,
        "cluster_reporting_actor_count": reporting_actor_count,
        "cluster_deduped_related_form4s": reporting_actor_count > actor_count,
    }
    source_document_id = f"insider-cluster::{ticker}::{direction}::{window_days}d::{window_start.isoformat()}"

    return {
        "id": stable_uuid(["notification", "insider_cluster", source_document_id]),
        "source": "insider",
        "signal_type": "insider_cluster",
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


def compile_insider_cluster_events(
    grouped_events: list[dict],
    *,
    window_days: int = 10,
    min_members: int = DEFAULT_INSIDER_CLUSTER_MIN_MEMBERS,
) -> list[dict]:
    cluster_candidates = []
    required_members = max(DEFAULT_INSIDER_CLUSTER_MIN_MEMBERS, int(min_members))
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for event in grouped_events:
        if str(event.get("source") or "").lower() != "insider":
            continue
        direction = str(event.get("direction") or "").lower()
        if direction not in {"buy", "sell"}:
            continue
        if not parse_iso_date(event.get("published_at")):
            continue
        ticker = publishable_cluster_ticker(event.get("ticker"))
        if not ticker:
            continue
        buckets[(ticker, direction)].append(event)

    for (ticker, direction), events in buckets.items():
        for window_start, window_events in non_overlapping_published_windows(events, window_days):
            economic_events = unique_insider_economic_events(window_events)
            economic_keys = {":".join(insider_economic_signature(event)) for event in economic_events}
            if len(economic_keys) < required_members:
                continue
            cluster_candidates.append(
                build_insider_cluster_event(
                    ticker,
                    direction,
                    window_events,
                    window_days=window_days,
                    window_start=window_start,
                )
            )

    return cluster_candidates


def compile_notification_events(
    events: list[dict],
    *,
    congress_cluster_window_days: int = 10,
    congress_cluster_min_members: int = 2,
    insider_cluster_window_days: int = 10,
    insider_cluster_min_members: int = DEFAULT_INSIDER_CLUSTER_MIN_MEMBERS,
    cross_source_window_days: int = 45,
    fund_window_days: int = 120,
) -> list[dict]:
    grouped = compile_grouped_events(events)
    filing_summaries = compile_filing_summary_events(grouped)
    clusters = compile_congress_cluster_events(
        grouped,
        window_days=congress_cluster_window_days,
        min_members=congress_cluster_min_members,
    )
    insider_clusters = compile_insider_cluster_events(
        grouped,
        window_days=insider_cluster_window_days,
        min_members=insider_cluster_min_members,
    )
    cross_source_clusters = compile_cross_source_accumulation_events(
        grouped,
        window_days=cross_source_window_days,
        fund_window_days=fund_window_days,
    )
    cross_source_sell_clusters = compile_cross_source_sell_events(
        grouped,
        window_days=cross_source_window_days,
        fund_window_days=fund_window_days,
    )
    compiled = grouped + filing_summaries + clusters + insider_clusters + cross_source_clusters + cross_source_sell_clusters
    deduped: dict[str, dict] = {}
    for event in compiled:
        deduped[str(event["id"])] = event
    return list(deduped.values())


def is_compiled_notification_event(event: dict) -> bool:
    return bool((event.get("payload") or {}).get("compiled_notification_event"))
