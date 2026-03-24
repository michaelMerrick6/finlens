import os
import sys

from alert_delivery_support import event_telegram_text, fetch_pending_deliveries, mark_delivery
from pipeline_support import emit_summary, get_supabase_client
from telegram_support import send_message, telegram_bot_token


BATCH_SIZE = int(os.environ.get("TELEGRAM_ALERT_BATCH_SIZE", "20"))


def main():
    print("Dispatching Telegram alerts...")
    supabase = get_supabase_client()
    deliveries = fetch_pending_deliveries(supabase, channel="telegram", batch_size=BATCH_SIZE)
    if deliveries and not telegram_bot_token():
        emit_summary(
            {
                "deliveries_seen": len(deliveries),
                "deliveries_sent": 0,
                "deliveries_failed": 0,
                "deliveries_blocked_config": len(deliveries),
                "batch_size": BATCH_SIZE,
            }
        )
        print("Telegram dispatch blocked: missing TELEGRAM_BOT_TOKEN.")
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
            send_message(destination, event_telegram_text(rendered_event))
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
    print(f"Telegram dispatch complete: {sent} sent, {failed} failed.")


if __name__ == "__main__":
    main()
