import os

from alert_delivery_support import event_sms_text, fetch_pending_deliveries, mark_delivery
from pipeline_support import emit_summary, get_supabase_client
from sms_support import send_sms, sms_configured


BATCH_SIZE = int(os.environ.get("SMS_ALERT_BATCH_SIZE", "20"))


def main():
    print("Dispatching text alerts...")
    supabase = get_supabase_client()
    deliveries = fetch_pending_deliveries(supabase, channel="sms", batch_size=BATCH_SIZE)
    if not deliveries:
        emit_summary(
            {
                "deliveries_seen": 0,
                "deliveries_sent": 0,
                "deliveries_failed": 0,
                "batch_size": BATCH_SIZE,
            }
        )
        print("No pending text deliveries.")
        return

    if not sms_configured():
        for delivery in deliveries:
            attempts = int(delivery.get("attempts") or 0) + 1
            mark_delivery(
                supabase,
                delivery["id"],
                status="failed",
                attempts=attempts,
                last_error="Text dispatch blocked: missing TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_FROM_PHONE.",
            )
        emit_summary(
            {
                "deliveries_seen": len(deliveries),
                "deliveries_sent": 0,
                "deliveries_failed": len(deliveries),
                "deliveries_blocked_config": len(deliveries),
                "batch_size": BATCH_SIZE,
            }
        )
        print("Text dispatch blocked: missing Twilio config.")
        return

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
            send_sms(destination, event_sms_text(rendered_event))
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
    print(f"Text dispatch complete: {sent} sent, {failed} failed.")


if __name__ == "__main__":
    main()
