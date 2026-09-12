import os

import requests

from alert_delivery_support import DeliveryRateLimited, build_discord_webhook_payload, dispatch_channel
from pipeline_support import emit_summary, get_supabase_client


BATCH_SIZE = int(os.environ.get("DISCORD_ALERT_BATCH_SIZE", "20"))


def send_webhook(destination: str, event: dict):
    payload = build_discord_webhook_payload(event)
    response = requests.post(destination, json=payload, timeout=15)
    if response.status_code == 429:
        raise DeliveryRateLimited("Discord provider rate limit")
    response.raise_for_status()


def main():
    summary = dispatch_channel(get_supabase_client(), channel="discord", batch_size=BATCH_SIZE,
                               send=lambda destination, event, delivery_id: send_webhook(destination, event))
    emit_summary(summary)
    if summary['deliveries_uncertain'] or summary['deliveries_failed']:
        raise RuntimeError("Delivery batch requires review; inspect delivery statuses.")


if __name__ == "__main__":
    main()
