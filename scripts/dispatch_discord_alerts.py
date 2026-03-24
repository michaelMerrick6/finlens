import os

import requests

from alert_delivery_support import build_discord_embed, fetch_pending_deliveries, mark_delivery
from pipeline_support import emit_summary, get_supabase_client


BATCH_SIZE = int(os.environ.get("DISCORD_ALERT_BATCH_SIZE", "20"))


def send_webhook(destination: str, event: dict):
    payload = {
        "username": "Vail Signals",
        "embeds": [build_discord_embed(event)],
    }
    response = requests.post(destination, json=payload, timeout=15)
    response.raise_for_status()


def main():
    print("Dispatching Discord alerts...")
    supabase = get_supabase_client()
    deliveries = fetch_pending_deliveries(supabase, channel="discord", batch_size=BATCH_SIZE)
    sent = 0
    failed = 0

    for delivery in deliveries:
        attempts = int(delivery.get("attempts") or 0) + 1
        destination = (delivery.get("destination") or "").strip()
        event = delivery.get("signal_events")
        if not destination or not event:
            mark_delivery(supabase, delivery["id"], status="failed", attempts=attempts, last_error="Missing destination or event payload")
            failed += 1
            continue

        try:
            send_webhook(destination, event)
            mark_delivery(supabase, delivery["id"], status="sent", attempts=attempts)
            sent += 1
        except Exception as exc:
            mark_delivery(supabase, delivery["id"], status="failed", attempts=attempts, last_error=str(exc))
            failed += 1

    summary = {
        "deliveries_seen": len(deliveries),
        "deliveries_sent": sent,
        "deliveries_failed": failed,
        "batch_size": BATCH_SIZE,
    }
    emit_summary(summary)
    print(f"Discord dispatch complete: {sent} sent, {failed} failed.")


if __name__ == "__main__":
    main()
