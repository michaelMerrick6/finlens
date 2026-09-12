import os

from alert_delivery_support import event_sms_text, dispatch_channel
from pipeline_support import emit_summary, get_supabase_client
from sms_support import send_sms, sms_configured


BATCH_SIZE = int(os.environ.get("SMS_ALERT_BATCH_SIZE", "20"))


def main():
    summary = dispatch_channel(get_supabase_client(), channel="sms", batch_size=BATCH_SIZE, configured=sms_configured(),
                               send=lambda destination, event, delivery_id: send_sms(destination, event_sms_text(event)))
    emit_summary(summary)
    if summary['deliveries_uncertain'] or summary['deliveries_failed']:
        raise RuntimeError("Delivery batch requires review; inspect delivery statuses.")


if __name__ == "__main__":
    main()
