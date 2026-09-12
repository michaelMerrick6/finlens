import os

import requests

from alert_delivery_support import DeliveryRateLimited, event_email_html, event_subject, event_text_body, dispatch_channel
from pipeline_support import emit_summary, get_supabase_client


BATCH_SIZE = int(os.environ.get("EMAIL_ALERT_BATCH_SIZE", "20"))
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "").strip()
RESEND_FROM_NAME = os.environ.get("RESEND_FROM_NAME", "Vail Signals").strip() or "Vail Signals"
RESEND_REPLY_TO = os.environ.get("RESEND_REPLY_TO", "").strip()


def send_email(destination: str, event: dict, delivery_id: str) -> None:
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
            "Idempotency-Key": f"vail-delivery-{delivery_id}",
        },
        json=payload,
        timeout=20,
    )
    if response.status_code == 429:
        raise DeliveryRateLimited("Email provider rate limit")
    if response.status_code >= 400:
        raise RuntimeError(f"Resend error {response.status_code}: {response.text}")


def main():
    summary = dispatch_channel(get_supabase_client(), channel="email", batch_size=BATCH_SIZE, configured=bool(RESEND_API_KEY and RESEND_FROM_EMAIL), min_send_interval=0.6,
                               send=send_email)
    emit_summary(summary)
    if summary['deliveries_uncertain'] or summary['deliveries_failed']:
        raise RuntimeError("Delivery batch requires review; inspect delivery statuses.")


if __name__ == "__main__":
    main()
