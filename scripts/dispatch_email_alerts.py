import os
import sys

import requests

from alert_delivery_support import event_email_html, event_subject, event_text_body, fetch_pending_deliveries, mark_delivery
from pipeline_support import emit_summary, get_supabase_client


BATCH_SIZE = int(os.environ.get("EMAIL_ALERT_BATCH_SIZE", "20"))
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "").strip()
RESEND_FROM_NAME = os.environ.get("RESEND_FROM_NAME", "Vail Signals").strip() or "Vail Signals"
RESEND_REPLY_TO = os.environ.get("RESEND_REPLY_TO", "").strip()


def send_email(destination: str, event: dict) -> None:
    if not RESEND_API_KEY or not RESEND_FROM_EMAIL:
        raise RuntimeError("Missing RESEND_API_KEY or RESEND_FROM_EMAIL.")

    from_value = RESEND_FROM_EMAIL if not RESEND_FROM_NAME else f"{RESEND_FROM_NAME} <{RESEND_FROM_EMAIL}>"
    payload = {
        "from": from_value,
        "to": [destination],
        "subject": event_subject(event),
        "html": event_email_html(event),
        "text": event_text_body(event),
    }
    if RESEND_REPLY_TO:
        payload["reply_to"] = RESEND_REPLY_TO

    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Resend error {response.status_code}: {response.text}")


def main():
    print("Dispatching email alerts...")
    supabase = get_supabase_client()
    deliveries = fetch_pending_deliveries(supabase, channel="email", batch_size=BATCH_SIZE)
    if deliveries and (not RESEND_API_KEY or not RESEND_FROM_EMAIL):
        emit_summary(
            {
                "deliveries_seen": len(deliveries),
                "deliveries_sent": 0,
                "deliveries_failed": 0,
                "deliveries_blocked_config": len(deliveries),
                "batch_size": BATCH_SIZE,
            }
        )
        print("Email dispatch blocked: missing RESEND_API_KEY or RESEND_FROM_EMAIL.")
        sys.exit(1)

    sent = 0
    failed = 0
    for delivery in deliveries:
        attempts = int(delivery.get("attempts") or 0) + 1
        destination = (delivery.get("destination") or "").strip()
        event = delivery.get("signal_events")
        if not destination or not event:
            mark_delivery(
                supabase,
                delivery["id"],
                status="failed",
                attempts=attempts,
                last_error="Missing destination or event payload",
            )
            failed += 1
            continue

        try:
            rendered_event = dict(event)
            rendered_event["_delivery_payload"] = delivery.get("payload") or {}
            send_email(destination, rendered_event)
            mark_delivery(supabase, delivery["id"], status="sent", attempts=attempts)
            sent += 1
        except Exception as exc:
            mark_delivery(supabase, delivery["id"], status="failed", attempts=attempts, last_error=str(exc))
            failed += 1

    emit_summary(
        {
            "deliveries_seen": len(deliveries),
            "deliveries_sent": sent,
            "deliveries_failed": failed,
            "batch_size": BATCH_SIZE,
        }
    )
    print(f"Email dispatch complete: {sent} sent, {failed} failed.")


if __name__ == "__main__":
    main()
