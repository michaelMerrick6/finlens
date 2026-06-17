from __future__ import annotations

import re
from datetime import date

from market_price_support import fetch_market_price_series
from pipeline_support import utc_now


def parse_iso_date(value: str | None) -> date | None:
    raw = str(value or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def resolve_baseline_date(primary_value: str | None, fallback_value: str | None, latest_allowed: str) -> tuple[str | None, str | None]:
    primary = str(primary_value or "").strip()[:10]
    fallback = str(fallback_value or "").strip()[:10]
    if primary and primary <= latest_allowed:
        return primary, "primary"
    if fallback and fallback <= latest_allowed:
        return fallback, "fallback"
    if primary:
        return primary, "primary"
    if fallback:
        return fallback, "fallback"
    return None, None


def supported_market_ticker(value: str | None) -> bool:
    ticker = str(value or "").strip().upper()
    if not ticker or ticker in {"MULTI", "UNKNOWN", "N/A"}:
        return False
    return bool(re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", ticker))


def supported_baseline_asset_type(event: dict) -> bool:
    payload = event.get("payload") or {}
    asset_type = str(payload.get("asset_type") or "").strip().upper()
    signal_type = str(event.get("signal_type") or "").strip().lower()
    if signal_type == "politician_trade":
        if asset_type in {"OP", "OPTION", "OPTIONS", "PUT", "CALL"}:
            return False
        if asset_type and asset_type not in {"ST", "STOCK", "EQ", "EQUITY"}:
            return False
    return True


def baseline_reference_for_event(event: dict) -> tuple[str | None, str | None]:
    today_iso = utc_now().date().isoformat()
    signal_type = str(event.get("signal_type") or "").strip().lower()
    payload = event.get("payload") or {}

    if signal_type in {"politician_trade", "insider_trade"}:
        baseline_date, source = resolve_baseline_date(event.get("occurred_at"), event.get("published_at"), today_iso)
        if source == "primary":
            return baseline_date, "trade_date"
        if source == "fallback":
            return baseline_date, "filing_date"
        return None, None

    if signal_type == "fund_position_change":
        baseline_date, _ = resolve_baseline_date(event.get("published_at"), event.get("occurred_at"), today_iso)
        if baseline_date:
            return baseline_date, "filing_date"
        return None, None

    if signal_type in {"politician_cluster", "cross_source_accumulation"}:
        baseline_date, _ = resolve_baseline_date(payload.get("cluster_clocked_at") or event.get("published_at"), event.get("published_at"), today_iso)
        if baseline_date:
            return baseline_date, "cluster_clocked_at"
        return None, None

    if signal_type in {
        "politician_trade_grouped",
        "insider_trade_grouped",
        "politician_filing_summary",
        "insider_filing_summary",
    }:
        baseline_date, _ = resolve_baseline_date(event.get("published_at"), event.get("occurred_at"), today_iso)
        if baseline_date:
            return baseline_date, "signal_date"
        return None, None

    return None, None


def get_price_point_on_or_before(series: dict | None, target_date: str | None) -> dict | None:
    if not series or not target_date:
        return None
    points = series.get("points") or []
    if not points:
        return None

    target = str(target_date)[:10]
    left = 0
    right = len(points) - 1
    best_index = -1

    while left <= right:
        middle = (left + right) // 2
        point_date = str(points[middle].get("date") or "")
        if point_date <= target:
            best_index = middle
            left = middle + 1
        else:
            right = middle - 1

    if best_index < 0:
        return None
    return points[best_index]


def stored_baseline_snapshot(event: dict) -> tuple[float | None, str | None, str | None, str | None]:
    payload = event.get("payload") or {}
    try:
        baseline_price = float(payload.get("baseline_price") or 0)
    except (TypeError, ValueError):
        baseline_price = 0

    baseline_reference_date = str(payload.get("baseline_reference_date") or "").strip()[:10] or None
    baseline_price_date = str(payload.get("baseline_price_date") or "").strip()[:10] or None
    baseline_reference_type = str(payload.get("baseline_reference_type") or "").strip() or None
    if baseline_price <= 0 or not baseline_reference_date:
        return None, None, None, None
    return baseline_price, baseline_reference_date, baseline_price_date, baseline_reference_type


def enrich_events_with_baseline_snapshots(events: list[dict]) -> list[dict]:
    candidates: list[tuple[dict, dict, str, str, str]] = []
    earliest_by_ticker: dict[str, str] = {}

    for event in events:
        ticker = str(event.get("ticker") or "").strip().upper()
        if not supported_market_ticker(ticker):
            continue
        if not supported_baseline_asset_type(event):
            continue

        payload = dict(event.get("payload") or {})
        existing_baseline, existing_reference_date, _, _ = stored_baseline_snapshot({"payload": payload})
        if existing_baseline and existing_reference_date:
            event["payload"] = payload
            continue

        reference_date, reference_type = baseline_reference_for_event(event)
        if not reference_date or not reference_type:
            continue

        candidates.append((event, payload, ticker, reference_date, reference_type))
        current_earliest = earliest_by_ticker.get(ticker)
        if not current_earliest or reference_date < current_earliest:
            earliest_by_ticker[ticker] = reference_date

    if not candidates:
        return events

    series_by_ticker = {
        ticker: fetch_market_price_series(ticker, earliest_date)
        for ticker, earliest_date in earliest_by_ticker.items()
    }

    for event, payload, ticker, reference_date, reference_type in candidates:
        point = get_price_point_on_or_before(series_by_ticker.get(ticker), reference_date)
        if not point:
            continue
        baseline_price = point.get("price")
        baseline_price_date = str(point.get("date") or "").strip()[:10]
        if not isinstance(baseline_price, (int, float)) or baseline_price <= 0 or not baseline_price_date:
            continue
        series = series_by_ticker.get(ticker) or {}
        payload.update(
            {
                "baseline_price": round(float(baseline_price), 4),
                "baseline_price_date": baseline_price_date,
                "baseline_reference_date": reference_date,
                "baseline_reference_type": reference_type,
                "baseline_price_provider": series.get("provider"),
            }
        )
        event["payload"] = payload

    return events
