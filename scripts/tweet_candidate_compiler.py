import re
from datetime import datetime, timedelta

from alert_rules import classify_event_behavior, parse_amount_lower_bound
from shared_utils import extract_sec_accession
from notification_targets import normalize_actor_key
from signal_policy import load_signal_policy


MAX_TWEET_LENGTH = 280
POLICY = load_signal_policy()
TWEET_POLICY = POLICY.get("tweet_candidates") or {}
TWEET_ALWAYS_ACTOR_KEYS = {
    normalize_actor_key(value)
    for value in (TWEET_POLICY.get("always_actor_keys") or [])
    if str(value).strip()
}
TWEET_THEME_ALLOWLIST = {
    str(value).strip().lower()
    for value in (TWEET_POLICY.get("theme_allowlist") or [])
    if str(value).strip()
}
LARGE_POLITICIAN_BUY_MIN = float(TWEET_POLICY.get("large_politician_buy_min_lower_bound") or 100000)
COMMITTEE_RELEVANCE_BUY_MIN = float(TWEET_POLICY.get("committee_relevance_buy_min_lower_bound") or 15001)
ENABLE_CRYPTO_POLITICIAN_SELLS = bool(TWEET_POLICY.get("enable_crypto_politician_sells", True))
SUBSTANTIAL_INSIDER_SELL_MIN_REDUCTION_PCT = float(TWEET_POLICY.get("substantial_insider_sell_min_reduction_pct") or 0.10)
SUBSTANTIAL_INSIDER_SELL_MIN_VALUE = float(TWEET_POLICY.get("substantial_insider_sell_min_value") or 250000)
SUBSTANTIAL_INSIDER_BUY_MIN_INCREASE_PCT = float(TWEET_POLICY.get("substantial_insider_buy_min_increase_pct") or 0.50)
SUBSTANTIAL_INSIDER_BUY_MIN_VALUE = float(TWEET_POLICY.get("substantial_insider_buy_min_value") or 250000)
MEANINGFUL_INSIDER_CHANGE_MIN_PCT = float(TWEET_POLICY.get("meaningful_insider_change_min_pct") or 0.25)
MEANINGFUL_INSIDER_CHANGE_MIN_VALUE = float(TWEET_POLICY.get("meaningful_insider_change_min_value") or 250000)
MEANINGFUL_INSIDER_CHANGE_SCORE_MULTIPLIER = float(TWEET_POLICY.get("meaningful_insider_change_score_multiplier") or 0.8)
MATERIAL_INSIDER_CLUSTER_MIN_VALUE = float(TWEET_POLICY.get("material_insider_cluster_min_value") or 1000000)
INSIDER_CLUSTER_WINDOW_DAYS = int(TWEET_POLICY.get("insider_cluster_window_days") or 10)
SCORE_GATED_RULE_KEYS = {
    "grouped_congress_buy",
    "grouped_insider_buy",
}
DISCORD_PREMIUM_CHANNEL = "discord_premium"
BROADCAST_CATEGORY_BY_RULE_KEY = {
    "committee_relevance_buy": "politicians",
    "congress_cluster": "clusters",
    "cross_source_accumulation": "clusters",
    "crypto_politician_sell": "politicians",
    "first_quantum_politician_buy": "politicians",
    "grouped_congress_buy": "politicians",
    "grouped_insider_buy": "insiders",
    "insider_cluster": "clusters",
    "large_politician_buy": "politicians",
    "meaningful_insider_change": "insiders",
    "notable_politician_filing": "politicians",
    "notable_politician_trade": "politicians",
    "politician_gain_milestone": "updates",
    "cluster_gain_milestone": "updates",
    "substantial_insider_buy": "insiders",
    "substantial_insider_sell": "insiders",
    "theme_politician_buy": "politicians",
}
BROADCAST_CHANNELS_BY_RULE_KEY = {
    "committee_relevance_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "congress_cluster": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "cross_source_accumulation": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "crypto_politician_sell": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "first_quantum_politician_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "grouped_congress_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "grouped_insider_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "insider_cluster": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "large_politician_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "meaningful_insider_change": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "notable_politician_filing": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "notable_politician_trade": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "politician_gain_milestone": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "cluster_gain_milestone": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "substantial_insider_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "substantial_insider_sell": ["twitter", DISCORD_PREMIUM_CHANNEL],
    "theme_politician_buy": ["twitter", DISCORD_PREMIUM_CHANNEL],
}


def truncate_tweet(text: str, limit: int = MAX_TWEET_LENGTH) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."


def short_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).strftime("%b %-d, %Y")
    except ValueError:
        return str(value)[:10]


def normalize_signal_type(value: str | None) -> str:
    return str(value or "").strip().lower()


def comma_names(names: list[str], limit: int = 4) -> str:
    clean = [name.strip() for name in names if name and name.strip()]
    if not clean:
        return ""
    if len(clean) <= limit:
        return ", ".join(clean)
    remaining = len(clean) - limit
    return f"{', '.join(clean[:limit])}, +{remaining} more"


def is_entity_style_actor_name(value: str | None) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered)
    entity_tokens = {
        "inc",
        "corp",
        "corporation",
        "holdings",
        "partners",
        "trust",
        "services",
        "management",
        "capital",
        "group",
    }
    parts = set(normalized.split())
    if parts & entity_tokens:
        return True
    abbreviation_patterns = (
        r"\bl\s*\.?\s*l\s*\.?\s*c\.?\b",
        r"\bl\s*\.?\s*l\s*\.?\s*p\.?\b",
        r"\bl\s*\.?\s*p\.?\b",
    )
    return any(re.search(pattern, lowered) for pattern in abbreviation_patterns)


def event_actor_key(event: dict) -> str:
    payload = event.get("payload") or {}
    member_id = str(payload.get("member_id") or "").strip()
    if member_id:
        return normalize_actor_key(member_id)
    return normalize_actor_key(event.get("actor_name") or "")


def event_ticker(event: dict) -> str:
    return (event.get("ticker") or "").strip().upper()


def has_publishable_ticker(event: dict) -> bool:
    ticker = event_ticker(event)
    return bool(ticker and ticker not in {"N/A", "MULTI", "UNKNOWN"})


def event_direction(event: dict) -> str:
    return str(event.get("direction") or "").strip().lower()


def insider_relation(event: dict) -> str | None:
    payload = event.get("payload") or {}
    relation = str(payload.get("filer_relation") or "").strip()
    if not relation or relation.lower() == "insider":
        return None
    return relation


def insider_actor_label(actor_name: str | None, relation: str | None) -> str:
    name = str(actor_name or "").strip()
    clean_relation = str(relation or "").strip()
    if not name:
        return ""
    if clean_relation:
        return f"{name} ({clean_relation})"
    return name





def normalized_story_source_id(event: dict) -> str:
    payload = event.get("payload") or {}
    source = str(event.get("source") or "").strip().lower()
    source_document_id = str(event.get("source_document_id") or "").strip()

    if source == "congress" and source_document_id:
        return re.sub(r"-\d+$", "", source_document_id)

    if source == "insider":
        for value in (
            source_document_id,
            payload.get("group_source_document_id"),
            payload.get("source_url"),
            event.get("source_url"),
        ):
            accession = extract_sec_accession(value)
            if accession:
                return accession

    return source_document_id or str(event.get("id") or "")


def semantic_candidate_key(rule_key: str, event: dict, *parts: str) -> str:
    base_parts = [
        "broadcast",
        rule_key,
        normalized_story_source_id(event),
        event_actor_key(event) or "unknown",
        str(event.get("published_at") or "").strip(),
    ]
    for value in parts:
        clean = str(value or "").strip().lower()
        if clean:
            base_parts.append(clean)
    return "::".join(base_parts)


def base_signal_type(event: dict) -> str:
    payload = event.get("payload") or {}
    return normalize_signal_type(payload.get("base_signal_type") or event.get("signal_type"))


def candidate_category(rule_key: str) -> str:
    return BROADCAST_CATEGORY_BY_RULE_KEY.get(str(rule_key or "").strip(), "all")


def congress_actor_amount_label(row: dict) -> str:
    name = str(row.get("name") or "").strip()
    amount_range = str(row.get("amount_range") or "").strip()
    if not name:
        return ""
    if amount_range:
        return f"{name} ({amount_range})"
    return name


def congress_cluster_lower_bound(actor_rows: list[dict]) -> float:
    total = 0.0
    for row in actor_rows:
        total += parse_amount_lower_bound(row.get("amount_range"))
    return total


def money_floor_label(value: float) -> str | None:
    if value <= 0:
        return None
    return f"${int(round(value)):,}+"


def exact_money_label(value: float) -> str | None:
    if value <= 0:
        return None
    return f"${int(round(value)):,}"


def pct_display_label(value: float | int | None, *, digits: int = 1) -> str | None:
    if value is None:
        return None
    numeric = float(value)
    if abs(numeric - round(numeric)) < 0.05:
        return f"{int(round(numeric))}%"
    return f"{numeric:.{digits}f}%"


def unique_nonempty_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))


def amount_range_summary(ranges: list[str], *, limit: int = 3) -> str | None:
    unique_ranges = unique_nonempty_strings(ranges)
    if not unique_ranges:
        return None
    if len(unique_ranges) <= limit:
        return ", ".join(unique_ranges)
    remaining = len(unique_ranges) - limit
    return f"{', '.join(unique_ranges[:limit])}, +{remaining} more"


def date_span_label(start: str | None, end: str | None) -> str | None:
    start_label = short_date(start)
    end_label = short_date(end)
    if start_label and end_label:
        if start_label == end_label:
            return start_label
        return f"{start_label} to {end_label}"
    return start_label or end_label


def congress_amount_lower_bound(event: dict) -> float:
    payload = event.get("payload") or {}
    return parse_amount_lower_bound(payload.get("amount_range"))


def congress_trade_date(event: dict) -> str | None:
    payload = event.get("payload") or {}
    return (
        str(
            payload.get("trade_date")
            or payload.get("transaction_date")
            or payload.get("group_trade_date_start")
            or event.get("occurred_at")
            or ""
        ).strip()[:10]
        or None
    )


def is_congress_event(event: dict) -> bool:
    source = str(event.get("source") or "").strip().lower()
    return source == "congress" or base_signal_type(event) == "politician_trade"


def is_raw_congress_trade_event(event: dict) -> bool:
    return normalize_signal_type(event.get("signal_type")) == "politician_trade"


def is_notable_actor(event: dict) -> bool:
    return event_actor_key(event) in TWEET_ALWAYS_ACTOR_KEYS


def allowed_themes(event: dict, behavior: dict) -> list[str]:
    themes = [str(theme).strip().lower() for theme in (behavior.get("themes") or []) if str(theme).strip()]
    if not TWEET_THEME_ALLOWLIST:
        return themes
    return [theme for theme in themes if theme in TWEET_THEME_ALLOWLIST]


def build_cluster_candidate(event: dict) -> dict | None:
    payload = event.get("payload") or {}
    actor_count = int(payload.get("cluster_actor_count") or 0)
    if actor_count < 2:
        return None
    if not has_publishable_ticker(event):
        return None

    ticker = event_ticker(event)
    direction = event_direction(event)
    window_days = int(payload.get("cluster_window_days") or 7)
    actor_rows = payload.get("cluster_actors") or []
    actor_names = [str(row.get("name") or "").strip() for row in actor_rows if str(row.get("name") or "").strip()]
    actor_labels = [congress_actor_amount_label(row) for row in actor_rows if congress_actor_amount_label(row)]
    amount_ranges = [str(row.get("amount_range") or "").strip() for row in actor_rows if str(row.get("amount_range") or "").strip()]
    amount_range_label = amount_range_summary(amount_ranges)
    combined_floor = money_floor_label(congress_cluster_lower_bound(actor_rows))
    filed_label = short_date(event.get("published_at"))
    direction_word = "buy" if direction == "buy" else "sell"
    title = f"Congress cluster on {ticker}"
    rationale = f"{actor_count} Congress members reported {direction_word}s in {ticker} within {window_days} days."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Congress cluster: {actor_count} members reported {ticker} {direction_word}s within {window_days} days.",
                f"Members: {comma_names(actor_labels)}" if actor_labels else (f"Members: {comma_names(actor_names)}" if actor_names else ""),
                f"Disclosed ranges: {amount_range_label}" if amount_range_label else "",
                f"Combined floor: {combined_floor}" if combined_floor else "",
                f"Latest filing: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )

    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("congress_cluster", event, ticker, direction, str(window_days)),
        "rule_key": "congress_cluster",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("congress_cluster"),
            "direction": direction,
            "ticker": ticker,
            "cluster_actor_count": actor_count,
            "cluster_window_days": window_days,
            "cluster_actors": actor_rows,
            "cluster_combined_lower_bound": congress_cluster_lower_bound(actor_rows),
            "cluster_amount_ranges": unique_nonempty_strings(amount_ranges),
            "published_at": event.get("published_at"),
        },
    }


def build_cross_source_accumulation_candidate(event: dict) -> dict | None:
    payload = event.get("payload") or {}
    ticker = event_ticker(event)
    if normalize_signal_type(event.get("signal_type")) != "cross_source_accumulation" or not has_publishable_ticker(event):
        return None

    direction = event_direction(event)
    congress_count = int(payload.get("congress_actor_count") or 0)
    insider_count = int(payload.get("insider_actor_count") or 0)
    fund_count = int(payload.get("fund_actor_count") or 0)
    actor_rows = payload.get("cluster_actors") or []
    actor_names = [str(row.get("name") or "").strip() for row in actor_rows if str(row.get("name") or "").strip()]
    congress_actor_rows = [row for row in actor_rows if str(row.get("source") or "").strip().lower() == "congress"]
    congress_actor_labels = [congress_actor_amount_label(row) for row in congress_actor_rows if congress_actor_amount_label(row)]
    congress_amount_ranges = [str(row.get("amount_range") or "").strip() for row in congress_actor_rows if str(row.get("amount_range") or "").strip()]
    congress_range_label = amount_range_summary(congress_amount_ranges)
    congress_floor = money_floor_label(congress_cluster_lower_bound(congress_actor_rows))
    combined_floor = money_floor_label(float(payload.get("cluster_combined_lower_bound") or 0))
    filed_label = short_date(event.get("published_at"))
    is_sell_cluster = direction == "sell"
    source_parts = []
    count_parts = []
    if congress_count:
        source_parts.append("Congress")
        count_parts.append(f"Congress {congress_count}")
    if insider_count:
        source_parts.append("insiders")
        count_parts.append(f"insiders {insider_count}")
    if fund_count:
        source_parts.append("funds")
        count_parts.append(f"funds {fund_count}")
    source_mix = " + ".join(source_parts) or "multiple sources"
    title = f"Cross-source {'selling' if is_sell_cluster else 'accumulation'} on {ticker}"
    rationale = (
        f"{ticker} has aligned {source_mix} {'selling' if is_sell_cluster else 'accumulation'}."
    )
    if is_sell_cluster:
        lead = f"Cross-source selling: {ticker} has {source_mix} selling."
    else:
        lead = f"Cross-source accumulation: {ticker} has {source_mix} buying/increasing."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                lead,
                f"Counts: {', '.join(count_parts)}" if count_parts else "",
                f"Congress: {comma_names(congress_actor_labels)}" if congress_actor_labels else "",
                f"Congress ranges: {congress_range_label}" if congress_range_label else "",
                f"Congress floor: {congress_floor}" if congress_floor else "",
                f"Tracked floor: {combined_floor}" if combined_floor else "",
                f"Actors: {comma_names(actor_names)}" if actor_names else "",
                f"Latest filing: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("cross_source_accumulation", event, ticker),
        "rule_key": "cross_source_accumulation",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("cross_source_accumulation"),
            "ticker": ticker,
            "direction": direction,
            "congress_actor_count": congress_count,
            "insider_actor_count": insider_count,
            "fund_actor_count": fund_count,
            "cluster_actor_count": int(payload.get("cluster_actor_count") or 0),
            "cluster_window_days": int(payload.get("cluster_window_days") or 0),
            "cluster_actors": actor_rows,
            "cluster_combined_lower_bound": float(payload.get("cluster_combined_lower_bound") or 0),
            "cluster_amount_ranges": unique_nonempty_strings(congress_amount_ranges),
            "published_at": event.get("published_at"),
        },
    }


def build_insider_cluster_candidate(event: dict) -> dict | None:
    payload = event.get("payload") or {}
    if normalize_signal_type(event.get("signal_type")) != "insider_cluster" or not has_publishable_ticker(event):
        return None

    ticker = event_ticker(event)
    direction = event_direction(event)
    actor_rows = payload.get("cluster_actors") or []
    actor_count = int(payload.get("cluster_actor_count") or len(actor_rows) or 0)
    if actor_count < 2:
        return None

    window_days = int(payload.get("cluster_window_days") or 7)
    actor_labels = [
        insider_actor_label(row.get("name"), row.get("relation"))
        for row in actor_rows
        if insider_actor_label(row.get("name"), row.get("relation"))
    ]
    total_value = float(payload.get("cluster_total_value") or 0)
    total_value_label = exact_money_label(total_value)
    filed_label = short_date(event.get("published_at"))
    direction_word = "buying" if direction == "buy" else "selling"
    title = f"Insider cluster on {ticker}"
    rationale = f"{actor_count} insiders reported {direction_word} in {ticker} within {window_days} days."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Insider cluster: {actor_count} insiders reported {ticker} {direction_word} within {window_days} days.",
                f"Actors: {comma_names(actor_labels)}" if actor_labels else "",
                f"Estimated total value: {total_value_label}" if total_value_label else "",
                f"Latest filing: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )

    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("insider_cluster", event, ticker, direction, str(window_days)),
        "rule_key": "insider_cluster",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("insider_cluster"),
            "direction": direction,
            "ticker": ticker,
            "cluster_actor_count": actor_count,
            "cluster_window_days": window_days,
            "cluster_actors": actor_rows,
            "cluster_total_value": total_value,
            "cluster_event_ids": payload.get("cluster_event_ids") or [],
            "published_at": event.get("published_at"),
        },
    }


def build_notable_politician_candidate(event: dict) -> dict | None:
    if not is_congress_event(event) or not is_notable_actor(event):
        return None

    payload = event.get("payload") or {}
    signal_type = normalize_signal_type(event.get("signal_type"))
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    direction = event_direction(event)
    filed_label = short_date(event.get("published_at"))

    if signal_type == "politician_filing_summary":
        trade_count = int(payload.get("summary_trade_count") or 0)
        tickers = payload.get("summary_tickers") or []
        title = f"Notable politician filing: {actor_name}"
        rationale = f"{actor_name} is configured as always reviewable for social posting."
        draft = truncate_tweet(
            "\n".join(
                line
                for line in [
                    f"{actor_name} filed {trade_count} Congress trades.",
                    f"Tickers: {', '.join(str(value) for value in tickers[:6])}" if tickers else "",
                    f"Filed: {filed_label}" if filed_label else "",
                ]
                if line
            )
        )
        return {
            "channel": "twitter",
            "candidate_key": semantic_candidate_key("notable_politician_filing", event, actor_name),
            "rule_key": "notable_politician_filing",
            "signal_event_id": event["id"],
            "status": "pending_review",
            "score": float(event.get("importance_score") or 0),
            "title": title,
            "draft_text": draft,
            "rationale": rationale,
            "payload": {
                "signal_type": signal_type,
                "broadcast_category": candidate_category("notable_politician_filing"),
                "actor_name": actor_name,
                "summary_trade_count": trade_count,
                "summary_tickers": tickers,
            },
        }

    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event) or direction not in {"buy", "sell"}:
        return None

    title = f"Notable politician trade: {actor_name} {direction} {ticker}"
    rationale = f"{actor_name} is configured as always reviewable for social posting."
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"{actor_name} reported a {ticker} {direction}.",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("notable_politician_trade", event, ticker, direction),
        "rule_key": "notable_politician_trade",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": signal_type,
            "broadcast_category": candidate_category("notable_politician_trade"),
            "actor_name": actor_name,
            "ticker": ticker,
            "direction": direction,
            "amount_range": amount_range,
            "trade_date": congress_trade_date(event),
        },
    }


def build_committee_relevance_candidate(event: dict, behavior: dict) -> dict | None:
    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "buy":
        return None
    if "committee_relevance" not in (behavior.get("reasons") or []):
        return None
    if congress_amount_lower_bound(event) < COMMITTEE_RELEVANCE_BUY_MIN:
        return None

    payload = event.get("payload") or {}
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    committee_roles = payload.get("member_committee_roles") or []
    role_names = [str(role.get("name") or "").strip() for role in committee_roles if str(role.get("name") or "").strip()]
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))

    title = f"Committee-relevant Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} sits on a committee relevant to {ticker}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Committee-relevant Congress buy: {actor_name} reported a {ticker} buy.",
                f"Committees: {comma_names(role_names, limit=2)}" if role_names else "",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("committee_relevance_buy", event, ticker),
        "rule_key": "committee_relevance_buy",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("committee_relevance_buy"),
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "committee_roles": committee_roles,
            "themes": behavior.get("themes") or [],
            "trade_date": congress_trade_date(event),
        },
    }


def build_first_quantum_politician_buy_candidate(event: dict, behavior: dict) -> dict | None:
    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "buy":
        return None

    themes = allowed_themes(event, behavior)
    if "quantum" not in themes:
        return None

    payload = event.get("payload") or {}
    is_first_ticker_buy = bool(payload.get("is_first_congress_ticker_buy"))
    is_first_actor_ticker_buy = bool(payload.get("is_first_congress_actor_ticker_buy"))
    if not (is_first_ticker_buy or is_first_actor_ticker_buy):
        return None

    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))
    prior_ticker_buy_count = int(payload.get("prior_congress_ticker_buy_count") or 0)

    title = f"New Congress quantum position: {actor_name} -> {ticker}"
    if is_first_ticker_buy:
        rationale = f"{actor_name} appears to be the first tracked Congress buyer in {ticker}."
    else:
        rationale = f"{actor_name} appears to be opening a new tracked Congress position in {ticker}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"New Congress quantum position: {actor_name} reported a {ticker} buy.",
                "Theme: Quantum",
                f"Tracked prior Congress buys in {ticker}: {prior_ticker_buy_count}",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key(
            "first_quantum_politician_buy",
            event,
            ticker,
            "ticker-first" if is_first_ticker_buy else "actor-first",
        ),
        "rule_key": "first_quantum_politician_buy",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("first_quantum_politician_buy"),
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "themes": themes,
            "trade_date": congress_trade_date(event),
            "is_first_congress_ticker_buy": is_first_ticker_buy,
            "is_first_congress_actor_ticker_buy": is_first_actor_ticker_buy,
            "prior_congress_ticker_buy_count": prior_ticker_buy_count,
        },
    }


def build_large_politician_buy_candidate(event: dict) -> dict | None:
    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "buy":
        return None
    if congress_amount_lower_bound(event) < LARGE_POLITICIAN_BUY_MIN:
        return None

    payload = event.get("payload") or {}
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))
    title = f"Large Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} reported a Congress buy with a lower bound of at least ${int(LARGE_POLITICIAN_BUY_MIN):,}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Large Congress buy: {actor_name} reported a {ticker} buy.",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("large_politician_buy", event, ticker),
        "rule_key": "large_politician_buy",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("large_politician_buy"),
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "trade_date": congress_trade_date(event),
        },
    }


def build_theme_politician_buy_candidate(event: dict, behavior: dict) -> dict | None:
    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "buy":
        return None
    themes = allowed_themes(event, behavior)
    if not themes:
        return None
    if congress_amount_lower_bound(event) < 50000:
        return None

    payload = event.get("payload") or {}
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))
    title = f"Thematic Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} bought a theme-priority name in {', '.join(theme.title() for theme in themes[:3])}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Thematic Congress buy: {actor_name} reported a {ticker} buy.",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("theme_politician_buy", event, ticker, ",".join(themes[:3])),
        "rule_key": "theme_politician_buy",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("theme_politician_buy"),
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "themes": themes,
            "trade_date": congress_trade_date(event),
        },
    }


def build_crypto_politician_sell_candidate(event: dict, behavior: dict) -> dict | None:
    if not ENABLE_CRYPTO_POLITICIAN_SELLS:
        return None
    if not is_raw_congress_trade_event(event):
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "sell":
        return None
    if "crypto" not in allowed_themes(event, behavior):
        return None

    payload = event.get("payload") or {}
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    amount_range = payload.get("amount_range")
    trade_label = short_date(congress_trade_date(event))
    title = f"Congress crypto-related sell: {actor_name} -> {ticker}"
    rationale = f"{actor_name} sold a crypto-related equity."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Congress crypto-related sell: {actor_name} reported a {ticker} sale.",
                f"Size: {amount_range}" if amount_range else "",
                f"Trade date: {trade_label}" if trade_label else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("crypto_politician_sell", event, ticker),
        "rule_key": "crypto_politician_sell",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("crypto_politician_sell"),
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "trade_date": congress_trade_date(event),
        },
    }


def build_substantial_insider_sell_candidate(event: dict, behavior: dict) -> dict | None:
    if str(event.get("source") or "").strip().lower() != "insider":
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "sell":
        return None

    payload = event.get("payload") or {}
    reduction_pct = float(payload.get("insider_holding_reduction_pct") or 0)
    total_value = float(payload.get("insider_total_sell_value") or payload.get("value") or 0)
    if reduction_pct < SUBSTANTIAL_INSIDER_SELL_MIN_REDUCTION_PCT:
        return None
    if total_value < SUBSTANTIAL_INSIDER_SELL_MIN_VALUE:
        return None

    actor_name = str(event.get("actor_name") or "Unknown").strip()
    if is_entity_style_actor_name(actor_name):
        return None
    relation = insider_relation(event)
    actor_label = insider_actor_label(actor_name, relation)
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    reduction_label = f"{reduction_pct * 100:.1f}%"
    value_label = f"${total_value:,.0f}" if total_value else None
    themes = allowed_themes(event, behavior)
    title = f"Substantial insider sell: {actor_name} -> {ticker}"
    rationale = f"{actor_name} sold roughly {reduction_label} of their holding in {ticker}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Substantial insider sell: {actor_label} reduced their {ticker} holding by about {reduction_label}.",
                f"Estimated sale value: {value_label}" if value_label else "",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}" if themes else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("substantial_insider_sell", event, ticker),
        "rule_key": "substantial_insider_sell",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("substantial_insider_sell"),
            "actor_name": actor_name,
            "filer_relation": relation,
            "ticker": ticker,
            "insider_holding_reduction_pct": reduction_pct,
            "insider_total_sell_value": total_value,
            "themes": themes,
        },
    }


def build_substantial_insider_buy_candidate(event: dict, behavior: dict) -> dict | None:
    if str(event.get("source") or "").strip().lower() != "insider":
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) != "buy":
        return None

    payload = event.get("payload") or {}
    increase_pct = payload.get("insider_holding_increase_pct")
    total_value = float(payload.get("insider_total_buy_value") or payload.get("value") or 0)
    new_position = bool(payload.get("insider_new_position_after_buy"))
    if (increase_pct is None or float(increase_pct) < SUBSTANTIAL_INSIDER_BUY_MIN_INCREASE_PCT) and not new_position:
        return None
    if total_value < SUBSTANTIAL_INSIDER_BUY_MIN_VALUE:
        return None

    actor_name = str(event.get("actor_name") or "Unknown").strip()
    if is_entity_style_actor_name(actor_name):
        return None
    relation = insider_relation(event)
    actor_label = insider_actor_label(actor_name, relation)
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    increase_label = "new position" if new_position else f"{float(increase_pct) * 100:.1f}%"
    value_label = f"${total_value:,.0f}" if total_value else None
    themes = allowed_themes(event, behavior)
    title = f"Substantial insider buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} materially increased their {ticker} exposure."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Substantial insider buy: {actor_label} increased their {ticker} position by {increase_label}.",
                f"Estimated buy value: {value_label}" if value_label else "",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}" if themes else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("substantial_insider_buy", event, ticker),
        "rule_key": "substantial_insider_buy",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("substantial_insider_buy"),
            "actor_name": actor_name,
            "filer_relation": relation,
            "ticker": ticker,
            "insider_holding_increase_pct": increase_pct,
            "insider_total_buy_value": total_value,
            "insider_new_position_after_buy": new_position,
            "themes": themes,
        },
    }


def build_meaningful_insider_change_candidate(event: dict, behavior: dict) -> dict | None:
    if str(event.get("source") or "").strip().lower() != "insider":
        return None
    if not has_publishable_ticker(event):
        return None
    if event_direction(event) not in {"buy", "sell"}:
        return None

    payload = event.get("payload") or {}
    direction = event_direction(event)
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    if is_entity_style_actor_name(actor_name):
        return None
    relation = insider_relation(event)
    actor_label = insider_actor_label(actor_name, relation)

    if direction == "buy":
        change_pct = float(payload.get("insider_holding_increase_pct") or 0)
        total_value = float(payload.get("insider_total_buy_value") or payload.get("value") or 0)
    else:
        change_pct = float(payload.get("insider_holding_reduction_pct") or 0)
        total_value = float(payload.get("insider_total_sell_value") or payload.get("value") or 0)

    if change_pct < MEANINGFUL_INSIDER_CHANGE_MIN_PCT:
        return None
    if total_value < MEANINGFUL_INSIDER_CHANGE_MIN_VALUE:
        return None

    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    change_label = f"{change_pct * 100:.1f}%"
    value_label = f"${total_value:,.0f}" if total_value else None
    themes = allowed_themes(event, behavior)
    action_word = "increased" if direction == "buy" else "reduced"
    noun = "buy" if direction == "buy" else "sell"
    title = f"Meaningful insider {noun}: {actor_name} -> {ticker}"
    rationale = f"{actor_name} {action_word} their {ticker} exposure by about {change_label}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Meaningful insider {noun}: {actor_label} {action_word} their {ticker} position by about {change_label}.",
                f"Estimated transaction value: {value_label}" if value_label else "",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}" if themes else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )
    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("meaningful_insider_change", event, ticker, direction),
        "rule_key": "meaningful_insider_change",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": round(float(event.get("importance_score") or 0) * MEANINGFUL_INSIDER_CHANGE_SCORE_MULTIPLIER, 4),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("meaningful_insider_change"),
            "actor_name": actor_name,
            "filer_relation": relation,
            "ticker": ticker,
            "direction": direction,
            "insider_change_pct": change_pct,
            "insider_change_value": total_value,
            "themes": themes,
        },
    }


def build_politician_gain_milestone_candidate(event: dict) -> dict | None:
    if normalize_signal_type(event.get("signal_type")) != "politician_gain_milestone":
        return None
    if not has_publishable_ticker(event):
        return None

    payload = event.get("payload") or {}
    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    gain_pct = float(payload.get("gain_return_pct") or 0)
    milestone_pct = float(payload.get("gain_milestone_pct") or 0)
    trade_date = short_date(payload.get("trade_date"))
    price_as_of_label = short_date(payload.get("price_as_of") or event.get("published_at"))
    amount_range = str(payload.get("amount_range") or "").strip()
    entry_price = payload.get("entry_price")
    current_price = payload.get("current_price")
    estimated_gain_floor = payload.get("estimated_gain_lower_bound")
    holding_days = int(payload.get("holding_days") or 0)
    gain_label = pct_display_label(gain_pct)
    milestone_label = f"{int(milestone_pct)}%" if milestone_pct else None
    entry_label = exact_money_label(float(entry_price or 0))
    current_label = exact_money_label(float(current_price or 0))
    estimated_gain_label = money_floor_label(float(estimated_gain_floor or 0))

    title = f"Politician gain milestone on {ticker}"
    rationale = (
        f"{actor_name}'s {ticker} buy from {trade_date or 'a prior filing'} is now up "
        f"{gain_label or 'meaningfully'}."
    )
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Politician gain milestone: {actor_name}'s {ticker} buy is now up {gain_label or 'meaningfully'}.",
                f"Milestone: {milestone_label}" if milestone_label else "",
                f"Disclosed range: {amount_range}" if amount_range else "",
                f"Entry / current: {entry_label} -> {current_label}" if entry_label and current_label else "",
                f"Estimated gain floor: {estimated_gain_label}" if estimated_gain_label else "",
                f"Window: {holding_days} days" if holding_days else "",
                f"Trade date: {trade_date}" if trade_date else "",
                f"Price as of: {price_as_of_label}" if price_as_of_label else "",
            ]
            if line
        )
    )

    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("politician_gain_milestone", event, ticker, str(int(milestone_pct or 0))),
        "rule_key": "politician_gain_milestone",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("politician_gain_milestone"),
            "direction": event_direction(event),
            "ticker": ticker,
            "actor_name": actor_name,
            "amount_range": amount_range,
            "gain_return_pct": gain_pct,
            "gain_milestone_pct": milestone_pct,
            "entry_price": entry_price,
            "current_price": current_price,
            "holding_days": holding_days,
            "trade_date": payload.get("trade_date"),
            "original_filed_at": payload.get("original_filed_at"),
            "price_as_of": payload.get("price_as_of"),
            "estimated_gain_lower_bound": estimated_gain_floor,
        },
    }


def build_cluster_gain_milestone_candidate(event: dict) -> dict | None:
    if normalize_signal_type(event.get("signal_type")) != "cluster_gain_milestone":
        return None
    if not has_publishable_ticker(event):
        return None

    payload = event.get("payload") or {}
    ticker = event_ticker(event)
    gain_pct = float(payload.get("gain_return_pct") or 0)
    milestone_pct = float(payload.get("gain_milestone_pct") or 0)
    cluster_date = short_date(payload.get("cluster_clocked_at"))
    price_as_of_label = short_date(payload.get("price_as_of") or event.get("published_at"))
    entry_price = payload.get("entry_price")
    current_price = payload.get("current_price")
    estimated_gain_floor = payload.get("estimated_gain_lower_bound")
    cluster_floor = payload.get("cluster_combined_lower_bound")
    days_since_cluster = int(payload.get("days_since_cluster") or 0)
    gain_label = pct_display_label(gain_pct)
    milestone_label = f"{int(milestone_pct)}%" if milestone_pct else None
    entry_label = exact_money_label(float(entry_price or 0))
    current_label = exact_money_label(float(current_price or 0))
    estimated_gain_label = money_floor_label(float(estimated_gain_floor or 0))
    cluster_floor_label = money_floor_label(float(cluster_floor or 0))
    cluster_type = str(payload.get("cluster_type") or "").strip().lower()
    cluster_actor_count = int(payload.get("cluster_actor_count") or 0)
    source_mix_parts: list[str] = []
    congress_count = int(payload.get("congress_actor_count") or 0)
    insider_count = int(payload.get("insider_actor_count") or 0)
    fund_count = int(payload.get("fund_actor_count") or 0)
    if congress_count:
        source_mix_parts.append(f"Congress {congress_count}")
    if insider_count:
        source_mix_parts.append(f"Insiders {insider_count}")
    if fund_count:
        source_mix_parts.append(f"Funds {fund_count}")
    if not source_mix_parts:
        if cluster_type == "cross_source_accumulation":
            source_mix_parts.append("Cross-source")
        else:
            source_mix_parts.append("Congress")
    source_mix_label = ", ".join(source_mix_parts)

    title = f"Cluster performance update on {ticker}"
    rationale = (
        f"{source_mix_label} accumulation in {ticker}"
        f"{f' across {cluster_actor_count} actors' if cluster_actor_count else ''} "
        f"is now up {gain_label or 'meaningfully'} since the cluster was clocked."
    )
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Cluster update: {ticker} is up {gain_label or 'meaningfully'} since the cluster was clocked.",
                f"Milestone: {milestone_label}" if milestone_label else "",
                f"Source mix: {source_mix_label}" if source_mix_label else "",
                f"Actors: {cluster_actor_count}" if cluster_actor_count else "",
                f"Tracked cluster floor: {cluster_floor_label}" if cluster_floor_label else "",
                f"Entry / current: {entry_label} -> {current_label}" if entry_label and current_label else "",
                f"Estimated gain floor: {estimated_gain_label}" if estimated_gain_label else "",
                f"Window: {days_since_cluster} days" if days_since_cluster else "",
                f"Cluster date: {cluster_date}" if cluster_date else "",
                f"Price as of: {price_as_of_label}" if price_as_of_label else "",
            ]
            if line
        )
    )

    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key("cluster_gain_milestone", event, ticker, str(int(milestone_pct or 0))),
        "rule_key": "cluster_gain_milestone",
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": event.get("signal_type"),
            "broadcast_category": candidate_category("cluster_gain_milestone"),
            "direction": event_direction(event),
            "ticker": ticker,
            "cluster_type": cluster_type,
            "cluster_actor_count": cluster_actor_count,
            "cluster_combined_lower_bound": cluster_floor,
            "congress_actor_count": congress_count,
            "insider_actor_count": insider_count,
            "fund_actor_count": fund_count,
            "cluster_actors": payload.get("cluster_actors") or [],
            "gain_return_pct": gain_pct,
            "gain_milestone_pct": milestone_pct,
            "entry_price": entry_price,
            "current_price": current_price,
            "days_since_cluster": days_since_cluster,
            "cluster_clocked_at": payload.get("cluster_clocked_at"),
            "price_as_of": payload.get("price_as_of"),
            "estimated_gain_lower_bound": estimated_gain_floor,
        },
    }


def build_grouped_candidate(event: dict, behavior: dict) -> dict | None:
    payload = event.get("payload") or {}
    row_count = int(payload.get("group_row_count") or 0)
    if row_count < 2:
        return None

    signal_type = normalize_signal_type(event.get("signal_type"))
    direction = event_direction(event)
    if direction != "buy":
        return None
    if not has_publishable_ticker(event):
        return None

    actor_name = str(event.get("actor_name") or "Unknown").strip()
    relation = insider_relation(event)
    actor_label = insider_actor_label(actor_name, relation)
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))
    themes: list[str] = []
    amount_label: str | None = None
    trade_span: str | None = None
    range_label: str | None = None

    if signal_type == "politician_trade_grouped":
        rule_key = "grouped_congress_buy"
        title = f"Congress filing: {actor_name} bought {ticker}"
        rationale = f"{actor_name} reported {row_count} {ticker} buys in one congressional filing."
        headline = f"Congress filing: {actor_name} reported {row_count} {ticker} buys in one filing."
        range_label = amount_range_summary(list(payload.get("group_amount_ranges") or []))
        amount_label = money_floor_label(float(payload.get("group_combined_lower_bound") or 0))
        trade_span = date_span_label(payload.get("group_trade_date_start"), payload.get("group_trade_date_end"))
    elif signal_type == "insider_trade_grouped":
        if is_entity_style_actor_name(actor_name):
            return None
        themes = allowed_themes(event, behavior)
        if not themes:
            return None
        rule_key = "grouped_insider_buy"
        title = f"Insider filing: {actor_name} bought {ticker}"
        rationale = f"{actor_name} reported {row_count} {ticker} insider buys in one filing in a tracked theme."
        headline = f"Insider filing: {actor_label} reported {row_count} {ticker} buys in one filing."
        amount_label = exact_money_label(float(payload.get("group_combined_lower_bound") or 0))
        trade_span = date_span_label(payload.get("group_trade_date_start"), payload.get("group_trade_date_end"))
    else:
        return None

    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                headline,
                f"Disclosed ranges: {range_label}" if range_label else "",
                f"Combined floor: {amount_label}" if signal_type == "politician_trade_grouped" and amount_label else "",
                f"Combined value: {amount_label}" if signal_type == "insider_trade_grouped" and amount_label else "",
                f"Trade dates: {trade_span}" if trade_span else "",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}" if signal_type == "insider_trade_grouped" and themes else "",
                f"Filed: {filed_label}" if filed_label else "",
            ]
            if line
        )
    )

    return {
        "channel": "twitter",
        "candidate_key": semantic_candidate_key(rule_key, event, ticker, direction),
        "rule_key": rule_key,
        "signal_event_id": event["id"],
        "status": "pending_review",
        "score": float(event.get("importance_score") or 0),
        "title": title,
        "draft_text": draft,
        "rationale": rationale,
        "payload": {
            "signal_type": signal_type,
            "broadcast_category": candidate_category(rule_key),
            "direction": direction,
            "ticker": ticker,
            "group_row_count": row_count,
            "group_event_ids": payload.get("group_event_ids") or [],
            "group_combined_lower_bound": float(payload.get("group_combined_lower_bound") or 0),
            "group_amount_ranges": payload.get("group_amount_ranges") or [],
            "group_trade_date_start": payload.get("group_trade_date_start"),
            "group_trade_date_end": payload.get("group_trade_date_end"),
            "amount_range": payload.get("amount_range"),
            "actor_name": actor_name,
            "filer_relation": relation,
            "themes": themes,
        },
    }


def candidate_for_event(event: dict, *, minimum_group_count: int) -> dict | None:
    signal_type = normalize_signal_type(event.get("signal_type"))
    behavior = classify_event_behavior(event)

    if signal_type == "politician_cluster":
        return build_cluster_candidate(event)
    if signal_type == "insider_cluster":
        return build_insider_cluster_candidate(event)
    if signal_type == "cross_source_accumulation":
        return build_cross_source_accumulation_candidate(event)
    if signal_type == "politician_gain_milestone":
        return build_politician_gain_milestone_candidate(event)
    if signal_type == "cluster_gain_milestone":
        return build_cluster_gain_milestone_candidate(event)

    if behavior.get("suppressed"):
        return None

    for builder in (
        build_notable_politician_candidate,
        lambda current: build_first_quantum_politician_buy_candidate(current, behavior),
        lambda current: build_committee_relevance_candidate(current, behavior),
        build_large_politician_buy_candidate,
        lambda current: build_crypto_politician_sell_candidate(current, behavior),
        lambda current: build_theme_politician_buy_candidate(current, behavior),
        lambda current: build_substantial_insider_buy_candidate(current, behavior),
        lambda current: build_substantial_insider_sell_candidate(current, behavior),
        lambda current: build_meaningful_insider_change_candidate(current, behavior),
    ):
        candidate = builder(event)
        if candidate:
            return candidate

    payload = event.get("payload") or {}
    if int(payload.get("group_row_count") or 0) >= minimum_group_count:
        return build_grouped_candidate(event, behavior)

    return None


def build_tweet_candidates(
    events: list[dict],
    *,
    minimum_importance: float = 0.88,
    minimum_group_count: int = 2,
) -> list[dict]:
    candidates: list[dict] = []
    seen_keys: set[str] = set()

    for event in events:
        candidate = candidate_for_event(event, minimum_group_count=minimum_group_count)
        if not candidate:
            continue

        score = float(event.get("importance_score") or 0)
        if candidate["rule_key"] in SCORE_GATED_RULE_KEYS and score < minimum_importance:
            continue

        dedupe_key = f"{candidate['channel']}::{candidate['candidate_key']}"
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        candidates.append(candidate)

    return candidates


def clone_candidate_for_channel(candidate: dict, channel: str) -> dict:
    cloned = {
        **candidate,
        "channel": channel,
        "payload": {
            **(candidate.get("payload") or {}),
            "broadcast_channel": channel,
        },
    }
    return cloned


def candidate_story_date(candidate: dict) -> str:
    return str(candidate.get("published_at") or candidate.get("payload", {}).get("published_at") or "")


def insider_cluster_score(max_score: float, total_value: float, actor_count: int) -> float:
    score = max_score + 0.08
    if actor_count >= 3:
        score += min(0.04, 0.02 * (actor_count - 2))
    if total_value >= MATERIAL_INSIDER_CLUSTER_MIN_VALUE:
        score += 0.04
    if total_value >= 5_000_000:
        score += 0.02
    if total_value >= 20_000_000:
        score += 0.02
    return min(0.99, round(score, 4))


def insider_cluster_group_key(event: dict) -> tuple[str, str]:
    return (event_ticker(event), event_direction(event))


def can_participate_in_insider_cluster(event: dict) -> bool:
    if str(event.get("source") or "").strip().lower() != "insider":
        return False
    if not has_publishable_ticker(event):
        return False
    if event_direction(event) not in {"buy", "sell"}:
        return False
    actor_name = str(event.get("actor_name") or "").strip()
    if not actor_name or is_entity_style_actor_name(actor_name):
        return False

    payload = event.get("payload") or {}
    if event_direction(event) == "buy":
        change_pct = float(payload.get("insider_holding_increase_pct") or 0)
        value = float(payload.get("insider_total_buy_value") or payload.get("value") or 0)
        new_position = bool(payload.get("insider_new_position_after_buy"))
        return ((change_pct >= MEANINGFUL_INSIDER_CHANGE_MIN_PCT) or new_position) and value >= MEANINGFUL_INSIDER_CHANGE_MIN_VALUE

    change_pct = float(payload.get("insider_holding_reduction_pct") or 0)
    value = float(payload.get("insider_total_sell_value") or payload.get("value") or 0)
    return change_pct >= MEANINGFUL_INSIDER_CHANGE_MIN_PCT and value >= MEANINGFUL_INSIDER_CHANGE_MIN_VALUE


def insider_cluster_event_value(event: dict) -> float:
    payload = event.get("payload") or {}
    direction = event_direction(event)
    return float(
        payload.get("group_combined_lower_bound")
        or payload.get("insider_total_buy_value" if direction == "buy" else "insider_total_sell_value")
        or payload.get("insider_change_value")
        or payload.get("value")
        or 0
    )


def normalized_numeric_component(value) -> str:
    try:
        numeric = float(value or 0)
    except (TypeError, ValueError):
        return ""
    if not numeric:
        return ""
    return f"{numeric:.4f}".rstrip("0").rstrip(".")


def insider_cluster_economic_signature(event: dict) -> tuple[str, ...]:
    payload = event.get("payload") or {}
    ticker = event_ticker(event)
    direction = event_direction(event)
    group_value = normalized_numeric_component(payload.get("group_combined_lower_bound"))
    group_count = str(payload.get("group_row_count") or "").strip()
    group_start = str(payload.get("group_trade_date_start") or event.get("occurred_at") or "").strip()[:10]
    group_end = str(payload.get("group_trade_date_end") or event.get("occurred_at") or "").strip()[:10]

    if group_value and group_count:
        return ("group", ticker, direction, group_start, group_end, group_count, group_value)

    return (
        "single",
        ticker,
        direction,
        str(event.get("occurred_at") or "").strip()[:10],
        normalized_numeric_component(payload.get("amount")),
        normalized_numeric_component(payload.get("price")),
        normalized_numeric_component(payload.get("value") or insider_cluster_event_value(event)),
    )


def build_insider_cluster_candidates(
    events: list[dict], *, window_days: int = INSIDER_CLUSTER_WINDOW_DAYS
) -> tuple[list[dict], set[str]]:
    window_days = max(1, int(window_days))
    grouped: dict[tuple[str, str], list[dict]] = {}
    for event in events:
        if not can_participate_in_insider_cluster(event):
            continue
        grouped.setdefault(insider_cluster_group_key(event), []).append(event)

    candidates: list[dict] = []
    suppressed_signal_event_ids: set[str] = set()
    for (ticker, direction), grouped_events in grouped.items():
        dated_events: list[tuple[datetime, dict]] = []
        for event in grouped_events:
            published_at = str(event.get("published_at") or "").strip()[:10]
            if not published_at:
                continue
            try:
                dated_events.append((datetime.fromisoformat(published_at), event))
            except ValueError:
                continue

        if len(dated_events) < 2:
            continue

        dated_events.sort(key=lambda item: (item[0], str(item[1].get("actor_name") or "").strip().lower()))
        seen_cluster_keys: set[tuple[str, str, str, tuple[str, ...]]] = set()

        for window_end, _anchor_event in dated_events:
            window_start = window_end - timedelta(days=window_days - 1)
            distinct_economic_groups: dict[tuple[str, ...], dict] = {}
            max_score = 0.0

            for event_date, event in dated_events:
                if event_date < window_start or event_date > window_end:
                    continue
                actor_name = str(event.get("actor_name") or "").strip()
                if not actor_name:
                    continue

                signature = insider_cluster_economic_signature(event)
                existing = distinct_economic_groups.get(signature)
                if existing is None or float(event.get("importance_score") or 0) > float(existing.get("importance_score") or 0):
                    distinct_economic_groups[signature] = event
                max_score = max(max_score, float(event.get("importance_score") or 0))

            if len(distinct_economic_groups) < 2:
                continue

            distinct_events = list(distinct_economic_groups.values())
            total_value = sum(insider_cluster_event_value(event) for event in distinct_events)
            actor_names = sorted(
                str(event.get("actor_name") or "").strip()
                for event in distinct_events
                if str(event.get("actor_name") or "").strip()
            )
            actor_rows = [
                {
                    "name": name,
                    "relation": insider_relation(next(event for event in distinct_events if str(event.get("actor_name") or "").strip() == name)),
                }
                for name in actor_names
            ]
            actor_labels = [insider_actor_label(row.get("name"), row.get("relation")) for row in actor_rows]
            latest_date = window_end.date().isoformat()
            cluster_key = (ticker, direction, latest_date, tuple(actor_names))
            if cluster_key in seen_cluster_keys:
                continue
            seen_cluster_keys.add(cluster_key)

            direction_word = "buying" if direction == "buy" else "selling"
            value_label = f"${total_value:,.0f}" if total_value else None
            title = f"Insider cluster on {ticker}"
            rationale = f"{len(actor_names)} insiders reported {direction_word} in {ticker} within {window_days} days."
            draft = truncate_tweet(
                "\n".join(
                    line
                    for line in [
                        f"Insider cluster: {len(actor_names)} insiders reported {ticker} {direction_word} within {window_days} days.",
                        f"Actors: {comma_names(actor_labels)}" if actor_labels else "",
                        f"Estimated total value: {value_label}" if value_label else "",
                        f"Latest filing: {short_date(latest_date)}" if latest_date else "",
                    ]
                    if line
                )
            )
            grouped_source_ids = sorted(
                str(event.get("source_document_id") or event.get("id") or "")
                for event in distinct_events
                if str(event.get("source_document_id") or event.get("id") or "").strip()
            )
            representative = max(distinct_events, key=lambda item: float(item.get("importance_score") or 0))
            actor_key = ",".join(name.lower() for name in actor_names)
            candidate = {
                "channel": "twitter",
                "candidate_key": "::".join(
                    [
                        "broadcast",
                        "insider_cluster",
                        ticker.lower(),
                        direction,
                        latest_date,
                        actor_key,
                    ]
                ),
                "rule_key": "insider_cluster",
                "signal_event_id": representative["id"],
                "status": "pending_review",
                "score": insider_cluster_score(max_score, total_value, len(actor_names)),
                "title": title,
                "draft_text": draft,
                "rationale": rationale,
                "payload": {
                    "signal_type": "insider_cluster",
                    "broadcast_category": candidate_category("insider_cluster"),
                    "ticker": ticker,
                    "direction": direction,
                    "cluster_actor_count": len(actor_names),
                    "cluster_window_days": window_days,
                    "cluster_actors": actor_rows,
                    "cluster_event_ids": [str(event["id"]) for event in distinct_events],
                    "cluster_total_value": total_value,
                    "cluster_source_document_ids": grouped_source_ids,
                    "published_at": latest_date,
                    "cluster_window_start": window_start.date().isoformat(),
                },
            }
            candidates.append(candidate)
            suppressed_signal_event_ids.update(str(event["id"]) for event in distinct_events)

    return candidates, suppressed_signal_event_ids


def build_broadcast_candidates(
    events: list[dict],
    *,
    minimum_importance: float = 0.88,
    minimum_group_count: int = 2,
) -> list[dict]:
    twitter_candidates = build_tweet_candidates(
        events,
        minimum_importance=minimum_importance,
        minimum_group_count=minimum_group_count,
    )
    insider_cluster_candidates, suppressed_signal_event_ids = build_insider_cluster_candidates(events)
    twitter_candidates = [
        candidate
        for candidate in twitter_candidates
        if not (
            str(candidate.get("rule_key") or "") in {"substantial_insider_buy", "substantial_insider_sell", "meaningful_insider_change", "grouped_insider_buy"}
            and str(candidate.get("signal_event_id") or "") in suppressed_signal_event_ids
        )
    ]
    twitter_candidates = insider_cluster_candidates + twitter_candidates
    broadcast_candidates: list[dict] = []
    seen_keys: set[str] = set()

    for candidate in twitter_candidates:
        channels = BROADCAST_CHANNELS_BY_RULE_KEY.get(str(candidate.get("rule_key") or "").strip(), ["twitter"])
        for channel in channels:
            cloned = clone_candidate_for_channel(candidate, channel)
            dedupe_key = f"{cloned['channel']}::{cloned['candidate_key']}"
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            broadcast_candidates.append(cloned)

    return broadcast_candidates
