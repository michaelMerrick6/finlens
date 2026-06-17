from __future__ import annotations

import re
from datetime import date, timedelta
from uuid import NAMESPACE_URL, uuid5

from alert_rules import is_clean_market_asset, parse_amount_lower_bound
from market_price_support import fetch_market_price_series
from pipeline_support import utc_now, utc_now_iso
from signal_baseline_support import baseline_reference_for_event, get_price_point_on_or_before, stored_baseline_snapshot
from signal_policy import load_signal_policy


POLICY = load_signal_policy()
POLICY_THRESHOLDS = POLICY.get("thresholds") or {}
DEFAULT_GAIN_THRESHOLDS = POLICY_THRESHOLDS.get("politician_gain_milestone_thresholds_pct") or [100, 200]
POLITICIAN_GAIN_MILESTONE_THRESHOLDS = sorted(
    {
        float(value)
        for value in DEFAULT_GAIN_THRESHOLDS
        if isinstance(value, (int, float)) or str(value).strip()
    }
)
POLITICIAN_GAIN_MIN_LOWER_BOUND = float(POLICY_THRESHOLDS.get("politician_gain_milestone_min_lower_bound") or 15001)
POLITICIAN_GAIN_LOOKBACK_DAYS = int(POLICY_THRESHOLDS.get("politician_gain_milestone_lookback_days") or 365)
DEFAULT_CLUSTER_GAIN_THRESHOLDS = POLICY_THRESHOLDS.get("cluster_gain_milestone_thresholds_pct") or [50, 100, 200]
CLUSTER_GAIN_MILESTONE_THRESHOLDS = sorted(
    {
        float(value)
        for value in DEFAULT_CLUSTER_GAIN_THRESHOLDS
        if isinstance(value, (int, float)) or str(value).strip()
    }
)
CLUSTER_GAIN_MIN_LOWER_BOUND = float(POLICY_THRESHOLDS.get("cluster_gain_milestone_min_lower_bound") or 50001)
CLUSTER_GAIN_LOOKBACK_DAYS = int(POLICY_THRESHOLDS.get("cluster_gain_milestone_lookback_days") or 365)


def stable_uuid(parts: list[str]) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(parts)))


def money_label(value: float | None) -> str | None:
    if value is None or value <= 0:
        return None
    return f"${value:,.2f}"


def pct_label(value: float | None) -> str | None:
    if value is None:
        return None
    rounded = round(float(value), 1)
    if abs(rounded - round(rounded)) < 0.05:
        return f"{int(round(rounded))}%"
    return f"{rounded:.1f}%"


def parse_iso_date(value: str | None) -> date | None:
    raw = str(value or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def days_between(start_value: str | None, end_value: str | None) -> int | None:
    start_date = parse_iso_date(start_value)
    end_date = parse_iso_date(end_value)
    if not start_date or not end_date:
        return None
    return max((end_date - start_date).days, 0)


def resolve_baseline_date(primary_value: str | None, fallback_value: str | None, latest_allowed: str) -> str | None:
    primary = str(primary_value or "").strip()[:10]
    fallback = str(fallback_value or "").strip()[:10]
    if primary and primary <= latest_allowed:
        return primary
    if fallback and fallback <= latest_allowed:
        return fallback
    if primary:
        return primary
    if fallback:
        return fallback
    return None


def performance_score(*, gain_pct: float, milestone_pct: float, window_days: int | None, lower_bound: float) -> float:
    score = 0.82
    if gain_pct >= 200 or milestone_pct >= 200:
        score += 0.12
    elif gain_pct >= 100 or milestone_pct >= 100:
        score += 0.08
    elif gain_pct >= 50 or milestone_pct >= 50:
        score += 0.05
    elif gain_pct >= 40 or milestone_pct >= 40:
        score += 0.03

    if window_days is not None:
        if window_days <= 90:
            score += 0.04
        elif window_days <= 180:
            score += 0.03
        elif window_days <= 270:
            score += 0.02
        elif window_days <= 365:
            score += 0.01

    if lower_bound >= 500000:
        score += 0.02
    elif lower_bound >= 250000:
        score += 0.01

    if window_days is not None and window_days <= 120 and gain_pct >= 40:
        score += 0.02

    return round(min(score, 0.99), 2)


def milestone_for_gain_pct(gain_pct: float) -> float | None:
    achieved = [threshold for threshold in POLITICIAN_GAIN_MILESTONE_THRESHOLDS if gain_pct >= threshold]
    if not achieved:
        return None
    return max(achieved)


def has_supported_performance_ticker(event: dict) -> bool:
    ticker = str(event.get("ticker") or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z])?", ticker):
        return False
    payload = event.get("payload") or {}
    asset_type = str(payload.get("asset_type") or "").strip().upper()
    if asset_type in {"OP", "OPTION", "OPTIONS", "PUT", "CALL"}:
        return False
    if asset_type and asset_type not in {"ST", "STOCK", "EQ", "EQUITY"}:
        return False
    return True


def cluster_milestone_for_gain_pct(gain_pct: float) -> float | None:
    achieved = [threshold for threshold in CLUSTER_GAIN_MILESTONE_THRESHOLDS if gain_pct >= threshold]
    if not achieved:
        return None
    return max(achieved)


def build_politician_gain_milestone_events(events: list[dict]) -> list[dict]:
    eligible_events: list[dict] = []
    earliest_by_ticker: dict[str, str] = {}
    today_iso = utc_now().date().isoformat()
    cutoff_date = (utc_now() - timedelta(days=POLITICIAN_GAIN_LOOKBACK_DAYS)).date().isoformat()

    for event in events:
        if str(event.get("signal_type") or "").strip().lower() != "politician_trade":
            continue
        if str(event.get("direction") or "").strip().lower() != "buy":
            continue
        if not is_clean_market_asset(event):
            continue
        if not has_supported_performance_ticker(event):
            continue
        payload = event.get("payload") or {}
        lower_bound = parse_amount_lower_bound(payload.get("amount_range"))
        if lower_bound < POLITICIAN_GAIN_MIN_LOWER_BOUND:
            continue
        stored_entry_price, stored_trade_date, _, _ = stored_baseline_snapshot(event)
        fallback_trade_date, _ = baseline_reference_for_event(event)
        trade_date = stored_trade_date or fallback_trade_date or resolve_baseline_date(
            event.get("occurred_at"), event.get("published_at"), today_iso
        )
        if not trade_date or trade_date < cutoff_date:
            continue
        ticker = str(event.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        eligible_events.append(event)
        current_earliest = earliest_by_ticker.get(ticker)
        if not current_earliest or trade_date < current_earliest:
            earliest_by_ticker[ticker] = trade_date

    if not eligible_events:
        return []

    series_by_ticker = {
        ticker: fetch_market_price_series(ticker, earliest_date)
        for ticker, earliest_date in earliest_by_ticker.items()
    }
    now_iso = utc_now_iso()
    compiled: list[dict] = []

    for event in eligible_events:
        payload = event.get("payload") or {}
        ticker = str(event.get("ticker") or "").strip().upper()
        stored_entry_price, stored_trade_date, stored_price_date, stored_reference_type = stored_baseline_snapshot(event)
        fallback_trade_date, fallback_reference_type = baseline_reference_for_event(event)
        trade_date = stored_trade_date or fallback_trade_date or resolve_baseline_date(
            event.get("occurred_at"), event.get("published_at"), today_iso
        )
        baseline_reference_type = stored_reference_type or fallback_reference_type or "trade_date"
        series = series_by_ticker.get(ticker)
        if not series or not trade_date:
            continue
        current_price = series.get("current_price")
        price_as_of = str(series.get("price_as_of") or "").strip()[:10]
        if not isinstance(current_price, (int, float)) or current_price <= 0 or not price_as_of:
            continue

        if stored_entry_price and stored_entry_price > 0:
            entry_price = float(stored_entry_price)
            baseline_price_date = stored_price_date or trade_date
            baseline_provider = str(payload.get("baseline_price_provider") or "").strip() or None
        else:
            entry_point = get_price_point_on_or_before(series, trade_date)
            if not entry_point:
                continue
            point_price = entry_point.get("price")
            baseline_price_date = str(entry_point.get("date") or "").strip()[:10]
            if not isinstance(point_price, (int, float)) or point_price <= 0 or not baseline_price_date:
                continue
            entry_price = float(point_price)
            baseline_provider = str(series.get("provider") or "").strip() or None

        if entry_price <= 0:
            continue

        gain_pct = ((float(current_price) / float(entry_price)) - 1) * 100
        milestone = milestone_for_gain_pct(gain_pct)
        if milestone is None:
            continue

        actor_name = str(event.get("actor_name") or "Unknown politician").strip()
        amount_range = str(payload.get("amount_range") or "").strip()
        lower_bound = parse_amount_lower_bound(amount_range)
        holding_days = days_between(trade_date, price_as_of)
        estimated_gain_value = round(lower_bound * (gain_pct / 100), 2) if lower_bound > 0 else None
        estimated_current_value = round(lower_bound * (1 + gain_pct / 100), 2) if lower_bound > 0 else None
        score = performance_score(
            gain_pct=gain_pct,
            milestone_pct=milestone,
            window_days=holding_days,
            lower_bound=lower_bound,
        )

        source_document_id = f"politician-gain::{str(event.get('source_document_id') or event.get('id') or '').strip()}::{int(milestone)}"
        title = f"Politician gain milestone: {actor_name} on {ticker}"
        summary_parts = [
            f"{actor_name}'s {ticker} buy from {trade_date} is now up {pct_label(gain_pct) or 'a significant amount'}",
        ]
        if holding_days is not None:
            summary_parts.append(f"over {holding_days} days")
        if amount_range:
            summary_parts.append(f"from a disclosed range of {amount_range}")
        if money_label(entry_price) and money_label(current_price):
            summary_parts.append(f"(entry {money_label(entry_price)} to {money_label(float(current_price))})")
        summary = " ".join(summary_parts).strip() + "."

        compiled.append(
            {
                "id": stable_uuid(["notification", "politician_gain_milestone", source_document_id]),
                "source": "congress",
                "signal_type": "politician_gain_milestone",
                "source_document_id": source_document_id,
                "ticker": ticker,
                "actor_name": actor_name,
                "actor_type": "politician",
                "direction": "buy",
                "occurred_at": price_as_of,
                "published_at": price_as_of,
                "importance_score": score,
                "title": title,
                "summary": summary,
                "source_url": event.get("source_url"),
                "payload": {
                    "compiled_notification_event": True,
                    "base_signal_type": "politician_trade",
                    "member_id": payload.get("member_id"),
                    "amount_range": amount_range,
                    "asset_type": payload.get("asset_type"),
                    "asset_name": payload.get("asset_name"),
                    "source_trade_event_id": event.get("id"),
                    "source_trade_document_id": event.get("source_document_id"),
                    "trade_date": trade_date,
                    "original_filed_at": event.get("published_at"),
                    "baseline_price": round(float(entry_price), 4),
                    "baseline_price_date": baseline_price_date,
                    "baseline_reference_date": trade_date,
                    "baseline_reference_type": baseline_reference_type,
                    "baseline_price_provider": baseline_provider,
                    "entry_price": round(float(entry_price), 4),
                    "current_price": round(float(current_price), 4),
                    "price_as_of": price_as_of,
                    "holding_days": holding_days,
                    "gain_return_pct": round(gain_pct, 2),
                    "gain_milestone_pct": int(milestone),
                    "estimated_gain_lower_bound": estimated_gain_value,
                    "estimated_current_lower_bound": estimated_current_value,
                },
                "created_at": now_iso,
            }
        )

    return compiled


def build_cluster_gain_milestone_events(events: list[dict]) -> list[dict]:
    eligible_events: list[dict] = []
    earliest_by_ticker: dict[str, str] = {}
    cutoff_date = (utc_now() - timedelta(days=CLUSTER_GAIN_LOOKBACK_DAYS)).date().isoformat()

    for event in events:
        signal_type = str(event.get("signal_type") or "").strip().lower()
        if signal_type not in {"politician_cluster", "cross_source_accumulation"}:
            continue
        payload = event.get("payload") or {}
        cluster_floor = float(payload.get("cluster_combined_lower_bound") or 0)
        if cluster_floor < CLUSTER_GAIN_MIN_LOWER_BOUND:
            continue
        _, stored_cluster_date, _, _ = stored_baseline_snapshot(event)
        fallback_cluster_date, _ = baseline_reference_for_event(event)
        cluster_date = stored_cluster_date or fallback_cluster_date or str(event.get("published_at") or event.get("occurred_at") or "").strip()[:10]
        if not cluster_date or cluster_date < cutoff_date:
            continue
        ticker = str(event.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        eligible_events.append(event)
        current_earliest = earliest_by_ticker.get(ticker)
        if not current_earliest or cluster_date < current_earliest:
            earliest_by_ticker[ticker] = cluster_date

    if not eligible_events:
        return []

    series_by_ticker = {
        ticker: fetch_market_price_series(ticker, earliest_date)
        for ticker, earliest_date in earliest_by_ticker.items()
    }
    now_iso = utc_now_iso()
    compiled: list[dict] = []

    for event in eligible_events:
        payload = event.get("payload") or {}
        signal_type = str(event.get("signal_type") or "").strip().lower()
        ticker = str(event.get("ticker") or "").strip().upper()
        stored_entry_price, stored_cluster_date, stored_price_date, stored_reference_type = stored_baseline_snapshot(event)
        fallback_cluster_date, fallback_reference_type = baseline_reference_for_event(event)
        cluster_date = (
            stored_cluster_date
            or fallback_cluster_date
            or str(event.get("published_at") or event.get("occurred_at") or "").strip()[:10]
        )
        baseline_reference_type = stored_reference_type or fallback_reference_type or "cluster_clocked_at"
        series = series_by_ticker.get(ticker)
        if not series or not cluster_date:
            continue
        current_price = series.get("current_price")
        price_as_of = str(series.get("price_as_of") or "").strip()[:10]
        if not isinstance(current_price, (int, float)) or current_price <= 0 or not price_as_of:
            continue

        if stored_entry_price and stored_entry_price > 0:
            entry_price = float(stored_entry_price)
            baseline_price_date = stored_price_date or cluster_date
            baseline_provider = str(payload.get("baseline_price_provider") or "").strip() or None
        else:
            entry_point = get_price_point_on_or_before(series, cluster_date)
            if not entry_point:
                continue
            point_price = entry_point.get("price")
            baseline_price_date = str(entry_point.get("date") or "").strip()[:10]
            if not isinstance(point_price, (int, float)) or point_price <= 0 or not baseline_price_date:
                continue
            entry_price = float(point_price)
            baseline_provider = str(series.get("provider") or "").strip() or None

        if entry_price <= 0:
            continue

        gain_pct = ((float(current_price) / float(entry_price)) - 1) * 100
        milestone = cluster_milestone_for_gain_pct(gain_pct)
        if milestone is None:
            continue

        cluster_actor_count = int(payload.get("cluster_actor_count") or 0)
        cluster_floor = float(payload.get("cluster_combined_lower_bound") or 0)
        days_since_cluster = days_between(cluster_date, price_as_of)
        source_label = "cross-source cluster" if signal_type == "cross_source_accumulation" else "Congress cluster"
        estimated_gain_floor = round(cluster_floor * (gain_pct / 100), 2) if cluster_floor > 0 else None
        score = performance_score(
            gain_pct=gain_pct,
            milestone_pct=milestone,
            window_days=days_since_cluster,
            lower_bound=cluster_floor,
        )

        source_document_id = f"cluster-gain::{str(event.get('source_document_id') or event.get('id') or '').strip()}::{int(milestone)}"
        title = f"Cluster gain milestone on {ticker}"
        summary_parts = [
            f"{source_label.title()} on {ticker} from {cluster_date} is now up {pct_label(gain_pct) or 'a significant amount'}",
        ]
        if days_since_cluster is not None:
            summary_parts.append(f"over {days_since_cluster} days")
        if cluster_actor_count:
            summary_parts.append(f"across {cluster_actor_count} actors")
        if cluster_floor > 0:
            summary_parts.append(f"from a tracked cluster floor of {money_label(cluster_floor)}")
        if money_label(entry_price) and money_label(current_price):
            summary_parts.append(f"(entry {money_label(entry_price)} to {money_label(float(current_price))})")
        summary = " ".join(summary_parts).strip() + "."

        compiled.append(
            {
                "id": stable_uuid(["notification", "cluster_gain_milestone", source_document_id]),
                "source": str(event.get("source") or "congress"),
                "signal_type": "cluster_gain_milestone",
                "source_document_id": source_document_id,
                "ticker": ticker,
                "actor_name": str(event.get("actor_name") or source_label).strip(),
                "actor_type": "cluster",
                "direction": "buy",
                "occurred_at": price_as_of,
                "published_at": price_as_of,
                "importance_score": score,
                "title": title,
                "summary": summary,
                "source_url": event.get("source_url"),
                "payload": {
                    "compiled_notification_event": True,
                    "base_signal_type": signal_type,
                    "cluster_type": signal_type,
                    "cluster_actor_count": cluster_actor_count,
                    "cluster_actors": payload.get("cluster_actors") or [],
                    "cluster_combined_lower_bound": cluster_floor,
                    "congress_actor_count": payload.get("congress_actor_count"),
                    "insider_actor_count": payload.get("insider_actor_count"),
                    "fund_actor_count": payload.get("fund_actor_count"),
                    "cluster_source_event_id": event.get("id"),
                    "cluster_source_document_id": event.get("source_document_id"),
                    "cluster_clocked_at": cluster_date,
                    "baseline_price": round(float(entry_price), 4),
                    "baseline_price_date": baseline_price_date,
                    "baseline_reference_date": cluster_date,
                    "baseline_reference_type": baseline_reference_type,
                    "baseline_price_provider": baseline_provider,
                    "entry_price": round(float(entry_price), 4),
                    "current_price": round(float(current_price), 4),
                    "price_as_of": price_as_of,
                    "days_since_cluster": days_since_cluster,
                    "gain_return_pct": round(gain_pct, 2),
                    "gain_milestone_pct": int(milestone),
                    "estimated_gain_lower_bound": estimated_gain_floor,
                },
                "created_at": now_iso,
            }
        )

    return compiled
