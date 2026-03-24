import re
from datetime import datetime

from alert_rules import classify_event_behavior, parse_amount_lower_bound
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
SCORE_GATED_RULE_KEYS = {
    "grouped_congress_buy",
    "grouped_insider_buy",
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


def event_direction(event: dict) -> str:
    return str(event.get("direction") or "").strip().lower()


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
        "twitter",
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


def congress_amount_lower_bound(event: dict) -> float:
    payload = event.get("payload") or {}
    return parse_amount_lower_bound(payload.get("amount_range"))


def is_congress_event(event: dict) -> bool:
    source = str(event.get("source") or "").strip().lower()
    return source == "congress" or base_signal_type(event) == "politician_trade"


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

    ticker = event_ticker(event)
    direction = event_direction(event)
    window_days = int(payload.get("cluster_window_days") or 7)
    actor_rows = payload.get("cluster_actors") or []
    actor_names = [str(row.get("name") or "").strip() for row in actor_rows if str(row.get("name") or "").strip()]
    filed_label = short_date(event.get("published_at"))
    direction_word = "buy" if direction == "buy" else "sell"
    title = f"Congress cluster on {ticker}"
    rationale = f"{actor_count} Congress members reported {direction_word}s in {ticker} within {window_days} days."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Congress cluster: {actor_count} members reported {ticker} {direction_word}s within {window_days} days.",
                f"Members: {comma_names(actor_names)}" if actor_names else "",
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
            "direction": direction,
            "ticker": ticker,
            "cluster_actor_count": actor_count,
            "cluster_window_days": window_days,
            "cluster_actors": actor_rows,
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
                "actor_name": actor_name,
                "summary_trade_count": trade_count,
                "summary_tickers": tickers,
            },
        }

    if not ticker or direction not in {"buy", "sell"}:
        return None

    title = f"Notable politician trade: {actor_name} {direction} {ticker}"
    rationale = f"{actor_name} is configured as always reviewable for social posting."
    amount_range = payload.get("amount_range")
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"{actor_name} reported a {ticker} {direction}.",
                f"Size: {amount_range}" if amount_range else "",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "direction": direction,
            "amount_range": amount_range,
        },
    }


def build_committee_relevance_candidate(event: dict, behavior: dict) -> dict | None:
    if not is_congress_event(event):
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

    title = f"Committee-relevant Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} sits on a committee relevant to {ticker}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Committee-relevant Congress buy: {actor_name} reported a {ticker} buy.",
                f"Committees: {comma_names(role_names, limit=2)}" if role_names else "",
                f"Size: {amount_range}" if amount_range else "",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "committee_roles": committee_roles,
            "themes": behavior.get("themes") or [],
        },
    }


def build_large_politician_buy_candidate(event: dict) -> dict | None:
    if not is_congress_event(event):
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
    title = f"Large Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} reported a Congress buy with a lower bound of at least ${int(LARGE_POLITICIAN_BUY_MIN):,}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Large Congress buy: {actor_name} reported a {ticker} buy.",
                f"Size: {amount_range}" if amount_range else "",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
        },
    }


def build_theme_politician_buy_candidate(event: dict, behavior: dict) -> dict | None:
    if not is_congress_event(event):
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
    title = f"Thematic Congress buy: {actor_name} -> {ticker}"
    rationale = f"{actor_name} bought a theme-priority name in {', '.join(theme.title() for theme in themes[:3])}."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Thematic Congress buy: {actor_name} reported a {ticker} buy.",
                f"Themes: {', '.join(theme.title() for theme in themes[:3])}",
                f"Size: {amount_range}" if amount_range else "",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
            "themes": themes,
        },
    }


def build_crypto_politician_sell_candidate(event: dict, behavior: dict) -> dict | None:
    if not ENABLE_CRYPTO_POLITICIAN_SELLS:
        return None
    if not is_congress_event(event):
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
    title = f"Congress crypto-related sell: {actor_name} -> {ticker}"
    rationale = f"{actor_name} sold a crypto-related equity."
    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                f"Congress crypto-related sell: {actor_name} reported a {ticker} sale.",
                f"Size: {amount_range}" if amount_range else "",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "amount_range": amount_range,
        },
    }


def build_substantial_insider_sell_candidate(event: dict, behavior: dict) -> dict | None:
    if str(event.get("source") or "").strip().lower() != "insider":
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
                f"Substantial insider sell: {actor_name} reduced their {ticker} holding by about {reduction_label}.",
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
            "actor_name": actor_name,
            "ticker": ticker,
            "insider_holding_reduction_pct": reduction_pct,
            "insider_total_sell_value": total_value,
            "themes": themes,
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

    actor_name = str(event.get("actor_name") or "Unknown").strip()
    ticker = event_ticker(event)
    filed_label = short_date(event.get("published_at"))

    if signal_type == "politician_trade_grouped":
        rule_key = "grouped_congress_buy"
        title = f"Congress filing: {actor_name} bought {ticker}"
        rationale = f"{actor_name} reported {row_count} {ticker} buys in one congressional filing."
        headline = f"Congress filing: {actor_name} reported {row_count} {ticker} buys in one filing."
    elif signal_type == "insider_trade_grouped":
        if is_entity_style_actor_name(actor_name):
            return None
        themes = allowed_themes(event, behavior)
        if not themes:
            return None
        rule_key = "grouped_insider_buy"
        title = f"Insider filing: {actor_name} bought {ticker}"
        rationale = f"{actor_name} reported {row_count} {ticker} insider buys in one filing in a tracked theme."
        headline = f"Insider filing: {actor_name} reported {row_count} {ticker} buys in one filing."
    else:
        return None

    draft = truncate_tweet(
        "\n".join(
            line
            for line in [
                headline,
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
            "direction": direction,
            "ticker": ticker,
            "group_row_count": row_count,
            "actor_name": actor_name,
        },
    }


def candidate_for_event(event: dict, *, minimum_group_count: int) -> dict | None:
    signal_type = normalize_signal_type(event.get("signal_type"))
    behavior = classify_event_behavior(event)

    if signal_type == "politician_cluster":
        return build_cluster_candidate(event)

    if behavior.get("suppressed"):
        return None

    for builder in (
        build_notable_politician_candidate,
        lambda current: build_committee_relevance_candidate(current, behavior),
        build_large_politician_buy_candidate,
        lambda current: build_crypto_politician_sell_candidate(current, behavior),
        lambda current: build_theme_politician_buy_candidate(current, behavior),
        lambda current: build_substantial_insider_sell_candidate(current, behavior),
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
