import html
import os
from datetime import datetime, timezone

from alert_rules import describe_behavior_reasons

DEFAULT_PRODUCT_NAME = os.environ.get("ALERT_PRODUCT_NAME", "Vail Signals").strip() or "Vail Signals"
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:3000").strip().rstrip("/")


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
    fields.append(("Importance", str(event.get("importance_score") or 0)))
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
    cluster_actor_count = payload.get("cluster_actor_count")
    if cluster_actor_count:
        fields.append(("Cluster Members", str(cluster_actor_count)))
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


def event_telegram_text(event: dict) -> str:
    title = html.escape(event.get("title") or "Vail signal event")
    summary = html.escape(event.get("summary") or "A new signal event was detected.")
    lines = [f"<b>{title}</b>", "", summary]
    for label, value in event_fields(event):
        lines.append(f"<b>{html.escape(label)}:</b> {html.escape(str(value))}")
    return "\n".join(lines)


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
