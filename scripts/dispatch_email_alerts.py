import os

import requests

from alert_delivery_support import DeliveryRateLimited
from daily_email_digest import dispatch_daily_digests
from pipeline_support import emit_summary, get_supabase_client


BATCH_SIZE = int(os.environ.get("EMAIL_ALERT_BATCH_SIZE", "20"))
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
RESEND_FROM_EMAIL = os.environ.get("RESEND_FROM_EMAIL", "").strip()
RESEND_FROM_NAME = os.environ.get("RESEND_FROM_NAME", "Vail Signals").strip() or "Vail Signals"
RESEND_REPLY_TO = os.environ.get("RESEND_REPLY_TO", "").strip()


def send_digest(destination: str, content: dict, digest_id: str) -> str:
    if not RESEND_API_KEY or not RESEND_FROM_EMAIL:
        raise RuntimeError("Missing email provider configuration")
    payload = {"from": f"{RESEND_FROM_NAME} <{RESEND_FROM_EMAIL}>", "to": [destination],
               "subject": content['subject'], "html": content['html'], "text": content['text']}
    if RESEND_REPLY_TO:
        payload['reply_to'] = RESEND_REPLY_TO
    response = requests.post('https://api.resend.com/emails',
        headers={'Authorization': f'Bearer {RESEND_API_KEY}', 'Content-Type': 'application/json',
                 'Idempotency-Key': f'vail-daily-digest-{digest_id}'}, json=payload, timeout=20)
    if response.status_code == 429:
        raise DeliveryRateLimited('Email provider rate limit')
    response.raise_for_status()
    message_id = response.json().get('id')
    if not message_id:
        raise RuntimeError('Email provider returned no message identifier')
    return str(message_id)


def main():
    summary = dispatch_daily_digests(get_supabase_client(), send=send_digest, batch_size=BATCH_SIZE,
        configured=bool(RESEND_API_KEY and RESEND_FROM_EMAIL))
    emit_summary(summary)
    if summary['digests_uncertain']:
        raise RuntimeError("Daily email outcome requires provider verification; automatic replay blocked.")


if __name__ == "__main__":
    main()
