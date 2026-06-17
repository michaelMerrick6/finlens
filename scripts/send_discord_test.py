import os

import requests
from dotenv import load_dotenv

from alert_delivery_support import build_discord_webhook_payload


load_dotenv(".env.local")

GLOBAL_WEBHOOK_URL = os.environ.get("DISCORD_GLOBAL_WEBHOOK_URL", os.environ.get("DISCORD_WEBHOOK_URL", "")).strip()


def main():
    if not GLOBAL_WEBHOOK_URL:
        raise RuntimeError("Missing DISCORD_GLOBAL_WEBHOOK_URL (or DISCORD_WEBHOOK_URL) for the global Discord server.")

    payload = build_discord_webhook_payload(
        {
            "title": "Discord integration test",
            "summary": "Vail can post into the premium Discord server channel.",
            "ticker": "VAILTEST",
            "source": "notifications",
            "signal_type": "integration_test",
            "actor_name": "Vail Ops",
            "importance_score": 1,
            "direction": "buy",
            "source_url": None,
        }
    )
    response = requests.post(GLOBAL_WEBHOOK_URL, json=payload, timeout=15)
    response.raise_for_status()
    print({"ok": True, "destination": "global_discord", "status_code": response.status_code})


if __name__ == "__main__":
    main()
