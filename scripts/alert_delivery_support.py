import html
import os
from datetime import datetime, timezone

from alert_rules import describe_behavior_reasons

DEFAULT_PRODUCT_NAME = os.environ.get("ALERT_PRODUCT_NAME", "Vail Signals").strip() or "Vail Signals"
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:3000").strip().rstrip("/")
DEFAULT_DISCORD_USERNAME = os.environ.get("DISCORD_GLOBAL_WEBHOOK_USERNAME", "Vail Signals").strip() or "Vail Signals"
DEFAULT_DISCORD_AVATAR_URL = os.environ.get("DISCORD_GLOBAL_WEBHOOK_AVATAR_URL", "").strip()


def event_color(direction: str | None) -> int:
    if direction in {"buy", "increase"}:
        return 0x10B981
    if direction in {"sell", "decrease"}:
        return 0xEF4444
    return 0x3B82F6


def build_discord_embed(event: dict) -> dict:
    return {
        "title": event.get("title") or "Vail signal event",
        "description": event.get("summary") or "A new signal event was detected.",
        "url": event.get("source_url"),
        "color": event_color(event.get("direction")),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "Ticker", "value": event.get("ticker") or "Unknown", "inline": True},
            {"name": "Source", "value": event.get("source") or "Unknown", "inline": True},
            {"name": "Signal", "value": event.get("signal_type") or "Unknown", "inline": True},
            {"name": "Actor", "value": event.get("actor_name") or "Unknown", "inline": False},
            {"name": "Importance", "value": str(event.get("importance_score") or 0), "inline": True},
        ],
        "footer": {"text": DEFAULT_PRODUCT_NAME},
    }


def build_discord_webhook_payload(event: dict) -> dict:
    payload: dict = {
        "username": DEFAULT_DISCORD_USERNAME,
        "embeds": [build_discord_embed(event)],
        "allowed_mentions": {"parse": []},
    }
    if DEFAULT_DISCORD_AVATAR_URL:
        payload["avatar_url"] = DEFAULT_DISCORD_AVATAR_URL
    return payload


def build_curated_discord_candidate_payload(candidate: dict, signal_event: dict | None = None) -> dict:
    source_event = signal_event or {}
    payload = candidate.get("payload") or {}
    actor_name = source_event.get("actor_name") or payload.get("actor_name") or "Unknown"
    filer_relation = str(payload.get("filer_relation") or "").strip()
    if filer_relation and filer_relation.lower() != "insider":
        actor_name = f"{actor_name} ({filer_relation})"
    event = {
        "title": candidate.get("title") or "Vail broadcast candidate",
        "summary": candidate.get("draft_text") or candidate.get("rationale") or "A curated Vail broadcast candidate is ready.",
        "ticker": source_event.get("ticker") or payload.get("ticker") or "Unknown",
        "source": source_event.get("source") or "broadcast_queue",
        "signal_type": source_event.get("signal_type") or payload.get("signal_type") or candidate.get("rule_key") or "broadcast_candidate",
        "actor_name": actor_name,
        "importance_score": candidate.get("score") or 0,
        "direction": source_event.get("direction") or payload.get("direction"),
        "source_url": source_event.get("source_url"),
    }
    return build_discord_webhook_payload(event)


def fetch_pending_deliveries(supabase, *, channel: str, batch_size: int) -> list[dict]:
    response = (
        supabase.table("alert_deliveries")
        .select("*, signal_events(*)")
        .eq("status", "pending")
        .eq("channel", channel)
        .order("queued_at", desc=False)
        .limit(batch_size)
        .execute()
    )
    return response.data or []


def mark_delivery(supabase, delivery_id: str, *, status: str, attempts: int, last_error: str | None = None) -> None:
    payload = {
        "status": status,
        "attempts": attempts,
        "last_error": last_error,
    }
    if status == "sent":
        payload["sent_at"] = datetime.now(timezone.utc).isoformat()
    supabase.table("alert_deliveries").update(payload).eq("id", delivery_id).execute()


def event_subject(event: dict) -> str:
    title = (event.get("title") or "New signal event").strip()
    return f"{DEFAULT_PRODUCT_NAME}: {title}"


def event_fields(event: dict) -> list[tuple[str, str]]:
    payload = event.get("payload") or {}
    delivery_payload = event.get("_delivery_payload") or {}
    behavior = delivery_payload.get("behavior") or {}
    signal_type = str(event.get("signal_type") or "").lower()
    is_filing_summary = signal_type in {"politician_filing_summary", "insider_filing_summary"}

    fields = [("Source", event.get("source") or "Unknown"), ("Signal", event.get("signal_type") or "Unknown"), ("Actor", event.get("actor_name") or "Unknown")]
    if not is_filing_summary:
        fields.append(("Ticker", event.get("ticker") or "Unknown"))
        fields.append(("Direction", event.get("direction") or "Unknown"))
    fields.append(("Score", str(event.get("importance_score") or 0)))
    occurred_at = event.get("occurred_at")
    published_at = event.get("published_at")
    if occurred_at:
        fields.append(("Occurred", str(occurred_at)))
    if published_at:
        fields.append(("Filed", str(published_at)))
    if behavior.get("unusual"):
        fields.append(("Alert Class", "Unusual"))
    elif behavior.get("activity"):
        fields.append(("Alert Class", "Activity"))
    theme_labels = behavior.get("theme_labels") or []
    if theme_labels:
        fields.append(("Themes", ", ".join(str(label) for label in theme_labels[:6])))
    reason_labels = describe_behavior_reasons(behavior)
    if reason_labels:
        fields.append(("Why Flagged", "; ".join(reason_labels[:6])))
    committee_match_themes = behavior.get("committee_match_themes") or []
    if committee_match_themes:
        fields.append(("Committee Match", ", ".join(str(theme).title() for theme in committee_match_themes[:6])))
    committee_roles = payload.get("member_committee_roles") or []
    if committee_match_themes and committee_roles:
        role_names = [str(role.get("name") or "").strip() for role in committee_roles if str(role.get("name") or "").strip()]
        if role_names:
            fields.append(("Relevant Committees", "; ".join(role_names[:4])))

    if is_filing_summary:
        summary_trade_count = payload.get("summary_trade_count")
        if summary_trade_count:
            fields.append(("Trades", str(summary_trade_count)))
        summary_tickers = payload.get("summary_tickers") or []
        if summary_tickers:
            fields.append(("Tickers", ", ".join(str(ticker) for ticker in summary_tickers[:8])))
        summary_unusual_event_ids = payload.get("summary_unusual_event_ids") or []
        if summary_unusual_event_ids:
            fields.append(("Unusual Matches", str(len(summary_unusual_event_ids))))
    else:
        amount_range = payload.get("amount_range")
        if amount_range:
            fields.append(("Amount", str(amount_range)))
    group_row_count = payload.get("group_row_count")
    if group_row_count:
        fields.append(("Grouped Trades", str(group_row_count)))
    group_combined_lower_bound = payload.get("group_combined_lower_bound")
    if group_combined_lower_bound:
        try:
            fields.append(("Grouped Floor", f"${float(group_combined_lower_bound):,.0f}+"))
        except (TypeError, ValueError):
            fields.append(("Grouped Floor", str(group_combined_lower_bound)))
    cluster_actor_count = payload.get("cluster_actor_count")
    if cluster_actor_count:
        fields.append(("Cluster Members", str(cluster_actor_count)))
    cluster_combined_lower_bound = payload.get("cluster_combined_lower_bound")
    if cluster_combined_lower_bound:
        try:
            fields.append(("Cluster Floor", f"${float(cluster_combined_lower_bound):,.0f}+"))
        except (TypeError, ValueError):
            fields.append(("Cluster Floor", str(cluster_combined_lower_bound)))
    congress_actor_count = payload.get("congress_actor_count")
    if congress_actor_count:
        fields.append(("Congress Members", str(congress_actor_count)))
    insider_actor_count = payload.get("insider_actor_count")
    if insider_actor_count:
        fields.append(("Insiders", str(insider_actor_count)))
    fund_actor_count = payload.get("fund_actor_count")
    if fund_actor_count:
        fields.append(("Funds", str(fund_actor_count)))
    cluster_actors = payload.get("cluster_actors") or []
    if cluster_actors:
        actor_names = [str(actor.get("name") or "").strip() for actor in cluster_actors if str(actor.get("name") or "").strip()]
        if actor_names:
            fields.append(("Actors", ", ".join(actor_names[:6])))
    cluster_window_days = payload.get("cluster_window_days")
    if cluster_window_days:
        fields.append(("Cluster Window", f"{cluster_window_days} days"))
    value = payload.get("value")
    if value:
        try:
            fields.append(("Reported Value", f"${float(value):,.0f}"))
        except (TypeError, ValueError):
            fields.append(("Reported Value", str(value)))
    gain_pct = payload.get("gain_return_pct")
    if gain_pct:
        try:
            fields.append(("Return", f"{float(gain_pct):,.1f}%"))
        except (TypeError, ValueError):
            fields.append(("Return", str(gain_pct)))
    gain_milestone_pct = payload.get("gain_milestone_pct")
    if gain_milestone_pct:
        fields.append(("Milestone", f"{gain_milestone_pct}%"))
    trade_date = payload.get("trade_date")
    if trade_date:
        fields.append(("Trade Date", str(trade_date)))
    holding_days = payload.get("holding_days")
    if holding_days:
        fields.append(("Holding Period", f"{holding_days} days"))
    cluster_clocked_at = payload.get("cluster_clocked_at")
    if cluster_clocked_at:
        fields.append(("Cluster Date", str(cluster_clocked_at)))
    days_since_cluster = payload.get("days_since_cluster")
    if days_since_cluster:
        fields.append(("Cluster Window", f"{days_since_cluster} days"))
    entry_price = payload.get("entry_price")
    if entry_price:
        try:
            fields.append(("Entry Price", f"${float(entry_price):,.2f}"))
        except (TypeError, ValueError):
            fields.append(("Entry Price", str(entry_price)))
    current_price = payload.get("current_price")
    if current_price:
        try:
            fields.append(("Current Price", f"${float(current_price):,.2f}"))
        except (TypeError, ValueError):
            fields.append(("Current Price", str(current_price)))
    estimated_gain_lower_bound = payload.get("estimated_gain_lower_bound")
    if estimated_gain_lower_bound:
        try:
            fields.append(("Estimated Gain Floor", f"${float(estimated_gain_lower_bound):,.0f}+"))
        except (TypeError, ValueError):
            fields.append(("Estimated Gain Floor", str(estimated_gain_lower_bound)))
    estimated_current_lower_bound = payload.get("estimated_current_lower_bound")
    if estimated_current_lower_bound:
        try:
            fields.append(("Estimated Current Floor", f"${float(estimated_current_lower_bound):,.0f}+"))
        except (TypeError, ValueError):
            fields.append(("Estimated Current Floor", str(estimated_current_lower_bound)))

    source_url = event.get("source_url")
    if source_url:
        fields.append(("Source Filing", source_url))
    if APP_BASE_URL:
        event_url = source_event_url(event)
        if event_url:
            fields.append(("Vail", event_url))
    return fields


def source_event_url(event: dict) -> str | None:
    ticker = (event.get("ticker") or "").strip().upper()
    signal_type = str(event.get("signal_type") or "").lower()
    if signal_type in {"politician_filing_summary", "insider_filing_summary"}:
        return None
    if ticker and ticker != "MULTI" and APP_BASE_URL:
        return f"{APP_BASE_URL}/ticker/{ticker}"
    return None


def event_text_body(event: dict) -> str:
    lines = [
        event.get("title") or "Vail signal event",
        "",
        event.get("summary") or "A new signal event was detected.",
    ]
    for label, value in event_fields(event):
        lines.append(f"{label}: {value}")
    return "\n".join(lines).strip()


def event_sms_text(event: dict) -> str:
    lines = [
        event.get("title") or "Vail signal event",
        event.get("summary") or "A new signal event was detected.",
    ]
    ticker = (event.get("ticker") or "").strip().upper()
    if ticker and ticker != "MULTI":
        lines.append(f"Ticker: {ticker}")
    direction = (event.get("direction") or "").strip()
    if direction:
        lines.append(f"Direction: {direction}")
    event_url = source_event_url(event)
    if event_url:
        lines.append(event_url)
    return "\n".join(line for line in lines if line).strip()


def event_email_html(event: dict) -> str:
    title = html.escape(event.get("title") or "Vail signal event")
    summary = html.escape(event.get("summary") or "A new signal event was detected.")
    rows = []
    for label, value in event_fields(event):
        safe_label = html.escape(label)
        safe_value = html.escape(str(value))
        if value.startswith("http://") or value.startswith("https://"):
            safe_value = f'<a href="{html.escape(value)}">{safe_value}</a>'
        rows.append(
            f"<tr><td style=\"padding:6px 12px 6px 0;font-weight:600;vertical-align:top;\">{safe_label}</td>"
            f"<td style=\"padding:6px 0;\">{safe_value}</td></tr>"
        )
    return (
        "<div style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.5;color:#111827;\">"
        f"<h2 style=\"margin:0 0 12px;\">{title}</h2>"
        f"<p style=\"margin:0 0 16px;\">{summary}</p>"
        "<table style=\"border-collapse:collapse;\">"
        f"{''.join(rows)}"
        "</table>"
        "</div>"
    )
