import os
import re

from notification_targets import normalize_actor_key
from signal_policy import load_signal_policy
from signal_profiles import event_signal_profile, theme_labels

POLICY = load_signal_policy()
POLICY_THRESHOLDS = POLICY.get("thresholds") or {}


POLITICIAN_UNUSUAL_MIN_LOWER_BOUND = float(
    POLICY_THRESHOLDS.get("politician_unusual_min_lower_bound")
    or os.environ.get("POLITICIAN_UNUSUAL_MIN_LOWER_BOUND", "100000")
)
POLITICIAN_THEME_UNUSUAL_MIN_LOWER_BOUND = float(
    POLICY_THRESHOLDS.get("politician_theme_unusual_min_lower_bound")
    or os.environ.get("POLITICIAN_THEME_UNUSUAL_MIN_LOWER_BOUND", "50000")
)
POLITICIAN_COMMITTEE_RELEVANCE_MIN_LOWER_BOUND = float(
    POLICY_THRESHOLDS.get("politician_committee_relevance_min_lower_bound")
    or os.environ.get("POLITICIAN_COMMITTEE_RELEVANCE_MIN_LOWER_BOUND", "15001")
)
INSIDER_BUY_UNUSUAL_MIN_VALUE = float(
    POLICY_THRESHOLDS.get("insider_buy_unusual_min_value")
    or os.environ.get("INSIDER_BUY_UNUSUAL_MIN_VALUE", "250000")
)
INSIDER_THEME_BUY_UNUSUAL_MIN_VALUE = float(
    POLICY_THRESHOLDS.get("insider_theme_buy_unusual_min_value")
    or os.environ.get("INSIDER_THEME_BUY_UNUSUAL_MIN_VALUE", "100000")
)
INSIDER_SELL_UNUSUAL_MIN_VALUE = float(
    POLICY_THRESHOLDS.get("insider_sell_unusual_min_value")
    or os.environ.get("INSIDER_SELL_UNUSUAL_MIN_VALUE", "1000000")
)
INSIDER_POSITION_REDUCTION_UNUSUAL_MIN_PCT = float(
    POLICY_THRESHOLDS.get("insider_position_reduction_unusual_min_pct")
    or os.environ.get("INSIDER_POSITION_REDUCTION_UNUSUAL_MIN_PCT", "0.5")
)
INSIDER_POSITION_INCREASE_UNUSUAL_MIN_PCT = float(
    POLICY_THRESHOLDS.get("insider_position_increase_unusual_min_pct")
    or os.environ.get("INSIDER_POSITION_INCREASE_UNUSUAL_MIN_PCT", "0.5")
)
POLITICIAN_GAIN_MILESTONE_MIN_PCT = float(
    POLICY_THRESHOLDS.get("politician_gain_milestone_min_pct")
    or os.environ.get("POLITICIAN_GAIN_MILESTONE_MIN_PCT", "100")
)
CLUSTER_GAIN_MILESTONE_MIN_PCT = float(
    POLICY_THRESHOLDS.get("cluster_gain_milestone_min_pct")
    or os.environ.get("CLUSTER_GAIN_MILESTONE_MIN_PCT", "50")
)

PRIORITY_SIGNAL_TICKERS = {
    ticker.strip().upper()
    for ticker in (
        POLICY.get("priority_signal_tickers")
        or os.environ.get("PRIORITY_SIGNAL_TICKERS", "IONQ,SKYT,QBTS,RGTI,VLD,NVDA").split(",")
    )
    if ticker.strip()
}
NOTABLE_POLITICIAN_KEYS = {
    normalize_actor_key(value)
    for value in (
        POLICY.get("notable_politician_keys")
        or os.environ.get("NOTABLE_POLITICIAN_KEYS", "Nancy Pelosi,P000197").split(",")
    )
    if value.strip()
}
NON_CLEAN_TICKER_PREFIXES = ("US-TREAS", "USTREAS")
CONGRESS_CLEAN_EQUITY_ASSET_TYPES = {
    "st",
    "stock",
    "stocks",
    "stock/etf",
    "etf",
    "cs",
    "common stock",
    "common stocks",
}


def parse_amount_lower_bound(amount_range: str | None) -> float:
    raw = str(amount_range or "").strip().lower()
    if not raw:
        return 0.0
    if "over $" in raw:
        digits = re.sub(r"[^\d]", "", raw)
        return float(digits or 0)
    matches = re.findall(r"\$?([\d,]+)", raw)
    if not matches:
        return 0.0
    return float(matches[0].replace(",", ""))


def payload_signal_type(event: dict) -> str:
    payload = event.get("payload") or {}
    return str(payload.get("base_signal_type") or event.get("signal_type") or "").strip().lower()


def event_ticker(event: dict) -> str:
    return (event.get("ticker") or "").strip().upper()


def is_priority_ticker(ticker: str) -> bool:
    return ticker.upper() in PRIORITY_SIGNAL_TICKERS


def is_notable_politician(event: dict) -> bool:
    payload = event.get("payload") or {}
    member_id = str(payload.get("member_id") or "").strip()
    actor_name = str(event.get("actor_name") or "").strip()
    return normalize_actor_key(member_id) in NOTABLE_POLITICIAN_KEYS or normalize_actor_key(actor_name) in NOTABLE_POLITICIAN_KEYS


def is_clean_market_asset(event: dict) -> bool:
    ticker = event_ticker(event)
    if not ticker or ticker in {"N/A", "NA", "UNKNOWN"}:
        return False
    if ticker.startswith(NON_CLEAN_TICKER_PREFIXES):
        return False

    source = str(event.get("source") or "").strip().lower()
    signal_type = payload_signal_type(event)
    payload = event.get("payload") or {}

    if source == "congress" or signal_type == "politician_trade":
        asset_type = str(payload.get("asset_type") or "").strip().lower()
        if asset_type and asset_type not in CONGRESS_CLEAN_EQUITY_ASSET_TYPES and "stock" not in asset_type and "etf" not in asset_type:
            return False
        asset_name = str(payload.get("asset_name") or "").strip().lower()
        if "treasury" in asset_name or "municipal" in asset_name or "bond" in asset_name:
            return False

    return True


def classify_event_behavior(event: dict) -> dict:
    signal_type = str(event.get("signal_type") or "").strip().lower()
    base_signal_type = payload_signal_type(event)
    payload = event.get("payload") or {}
    direction = str(event.get("direction") or "").strip().lower()
    ticker = event_ticker(event)
    profile = event_signal_profile(event)
    themes = profile.get("themes") or []
    is_priority_signal = bool(profile.get("is_priority_theme")) or is_priority_ticker(ticker)
    member_committee_themes = [
        str(theme).strip().lower()
        for theme in (payload.get("member_committee_themes") or [])
        if str(theme).strip()
    ]
    committee_match_themes = [theme for theme in themes if theme in member_committee_themes]

    result = {
        "activity": False,
        "unusual": False,
        "suppressed": False,
        "reasons": [],
        "themes": themes,
        "theme_labels": profile.get("theme_labels") or [],
        "committee_match_themes": committee_match_themes,
    }

    if signal_type == "politician_cluster":
        result["unusual"] = True
        result["reasons"].append("congress_cluster")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type == "cross_source_accumulation":
        result["activity"] = True
        result["unusual"] = True
        result["reasons"].append("cross_source_accumulation")
        if payload.get("includes_fund_source"):
            result["reasons"].append("cross_source_full_stack")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type == "politician_gain_milestone":
        result["activity"] = True
        gain_pct = float(payload.get("gain_return_pct") or 0)
        if gain_pct >= POLITICIAN_GAIN_MILESTONE_MIN_PCT:
            result["unusual"] = True
            result["reasons"].append("politician_gain_milestone")
        milestone_pct = float(payload.get("gain_milestone_pct") or 0)
        if milestone_pct >= 100:
            result["reasons"].append("triple_digit_gain")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type == "cluster_gain_milestone":
        result["activity"] = True
        gain_pct = float(payload.get("gain_return_pct") or 0)
        if gain_pct >= CLUSTER_GAIN_MILESTONE_MIN_PCT:
            result["unusual"] = True
            result["reasons"].append("cluster_gain_milestone")
        milestone_pct = float(payload.get("gain_milestone_pct") or 0)
        if milestone_pct >= 100:
            result["reasons"].append("triple_digit_gain")
        cluster_type = str(payload.get("cluster_type") or "").strip().lower()
        if cluster_type == "cross_source_accumulation":
            result["reasons"].append("cross_source_accumulation")
        else:
            result["reasons"].append("congress_cluster")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type in {"politician_filing_summary", "insider_filing_summary"}:
        result["activity"] = bool(payload.get("summary_contains_activity"))
        result["unusual"] = bool(payload.get("summary_contains_unusual"))
        result["reasons"].append("actor_filing_summary")
        if committee_match_themes:
            result["reasons"].append("committee_relevance")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type in {"fund_filing_deadline_reminder", "fund_filing_received"}:
        result["activity"] = True
        result["reasons"].append(signal_type)
        return result

    if not is_clean_market_asset(event):
        result["suppressed"] = True
        result["reasons"].append("non_clean_asset")
        return result

    if base_signal_type == "politician_trade":
        result["activity"] = True
        lower_bound = parse_amount_lower_bound(payload.get("amount_range"))
        is_first_congress_ticker_buy = bool(payload.get("is_first_congress_ticker_buy"))
        is_first_congress_actor_ticker_buy = bool(payload.get("is_first_congress_actor_ticker_buy"))
        threshold = (
            POLITICIAN_THEME_UNUSUAL_MIN_LOWER_BOUND
            if is_priority_signal or is_notable_politician(event)
            else POLITICIAN_UNUSUAL_MIN_LOWER_BOUND
        )
        if lower_bound >= threshold and direction in {"buy", "sell"}:
            result["unusual"] = True
            result["reasons"].append("large_politician_trade")
        if committee_match_themes and lower_bound >= POLITICIAN_COMMITTEE_RELEVANCE_MIN_LOWER_BOUND:
            result["unusual"] = True
            result["reasons"].append("committee_relevance")
        if direction == "buy" and "quantum" in themes and lower_bound > 0:
            if is_first_congress_ticker_buy:
                result["unusual"] = True
                result["reasons"].append("first_quantum_congress_buy")
            elif is_first_congress_actor_ticker_buy:
                result["unusual"] = True
                result["reasons"].append("new_quantum_position")
        if is_notable_politician(event):
            result["unusual"] = True
            result["reasons"].append("notable_politician")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if base_signal_type == "insider_trade":
        result["activity"] = True
        value = float(payload.get("value") or 0)
        if direction == "buy":
            threshold = INSIDER_THEME_BUY_UNUSUAL_MIN_VALUE if is_priority_signal else INSIDER_BUY_UNUSUAL_MIN_VALUE
            if value >= threshold:
                result["unusual"] = True
                result["reasons"].append("large_insider_buy")
            increase_pct = float(payload.get("insider_holding_increase_pct") or 0)
            if increase_pct >= INSIDER_POSITION_INCREASE_UNUSUAL_MIN_PCT or payload.get("insider_new_position_after_buy"):
                result["unusual"] = True
                result["reasons"].append("substantial_insider_position_increase")
        elif direction == "sell":
            if value >= INSIDER_SELL_UNUSUAL_MIN_VALUE:
                result["unusual"] = True
                result["reasons"].append("large_insider_sell")
            reduction_pct = float(payload.get("insider_holding_reduction_pct") or 0)
            if reduction_pct >= INSIDER_POSITION_REDUCTION_UNUSUAL_MIN_PCT:
                result["unusual"] = True
                result["reasons"].append("substantial_insider_position_reduction")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    if signal_type == "fund_position_change":
        result["activity"] = True
        qoq_pct = abs(float(payload.get("qoq_change_percent") or 0))
        change_type = str(payload.get("change_type") or "").lower()
        if not change_type:
            # Infer change_type from direction
            if direction == "increase":
                change_type = "increase"
            elif direction == "decrease":
                change_type = "decrease"
        if change_type in {"new", "exit"} or qoq_pct >= 20:
            result["unusual"] = True
            if change_type == "new":
                result["reasons"].append("new_fund_position")
            elif change_type == "exit":
                result["reasons"].append("fund_position_exit")
            else:
                result["reasons"].append("large_fund_position_change")
        if is_priority_signal:
            result["reasons"].append("priority_theme_ticker")
        for theme in themes:
            result["reasons"].append(f"theme_{theme}")
        return result

    result["activity"] = True
    return result


def follow_mode_matches(alert_mode: str | None, behavior: dict) -> bool:
    mode = str(alert_mode or "both").strip().lower()
    if mode == "both":
        return bool(behavior.get("activity") or behavior.get("unusual"))
    if mode == "activity":
        return bool(behavior.get("activity"))
    if mode == "unusual":
        return bool(behavior.get("unusual"))
    return bool(behavior.get("activity") or behavior.get("unusual"))


def describe_behavior_reasons(behavior: dict) -> list[str]:
    labels = []
    seen = set()
    for reason in behavior.get("reasons") or []:
        if reason in seen:
            continue
        seen.add(reason)
        if reason == "large_politician_trade":
            labels.append("Large politician trade")
        elif reason == "large_insider_buy":
            labels.append("Large insider buy")
        elif reason == "large_insider_sell":
            labels.append("Large insider sell")
        elif reason == "notable_politician":
            labels.append("Notable politician")
        elif reason == "committee_relevance":
            labels.append("Committee relevance")
        elif reason == "new_quantum_position":
            labels.append("New quantum position")
        elif reason == "first_quantum_congress_buy":
            labels.append("First Congress quantum buy")
        elif reason == "congress_cluster":
            labels.append("Congress cluster")
        elif reason == "cross_source_accumulation":
            labels.append("Cross-source accumulation")
        elif reason == "politician_gain_milestone":
            labels.append("Politician gain milestone")
        elif reason == "cluster_gain_milestone":
            labels.append("Cluster gain milestone")
        elif reason == "triple_digit_gain":
            labels.append("Triple-digit gain")
        elif reason == "cross_source_full_stack":
            labels.append("Congress + insiders + funds")
        elif reason == "actor_filing_summary":
            labels.append("Grouped filing summary")
        elif reason == "substantial_insider_position_increase":
            labels.append("Insider position increase")
        elif reason == "substantial_insider_position_reduction":
            labels.append("Insider position reduction")
        elif reason == "priority_theme_ticker":
            labels.append("Priority theme ticker")
        elif reason.startswith("theme_"):
            labels.extend(theme_labels([reason.removeprefix("theme_")]))
        elif reason == "non_clean_asset":
            labels.append("Non-market asset suppressed")
        elif reason == "new_fund_position":
            labels.append("New fund position")
        elif reason == "fund_position_exit":
            labels.append("Fund exited position")
        elif reason == "large_fund_position_change":
            labels.append("Large fund position change")
        elif reason == "fund_filing_deadline_reminder":
            labels.append("13F filing reminder")
        elif reason == "fund_filing_received":
            labels.append("13F filing received")
        else:
            labels.append(reason.replace("_", " ").title())
    return labels
