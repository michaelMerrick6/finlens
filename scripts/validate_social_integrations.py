import argparse
import json
import os

from dotenv import load_dotenv

load_dotenv(".env.local")

from send_discord_test import GLOBAL_WEBHOOK_URL
from twitter_api_support import get_authenticated_user, twitter_posting_config


def summarize_config() -> dict:
    x_config = twitter_posting_config()
    return {
        "discord_global_webhook_configured": bool(GLOBAL_WEBHOOK_URL),
        "x_posting_enabled": bool(x_config["enabled"]),
        "x_configured": bool(x_config["configured"]),
        "x_auth_mode": x_config["auth_mode"],
    }


def main():
    parser = argparse.ArgumentParser(description="Validate Discord and X integration configuration.")
    parser.add_argument("--test-discord", action="store_true", help="Send a Discord test message to the global webhook.")
    parser.add_argument("--test-x", action="store_true", help="Call X /2/users/me with the configured credentials.")
    args = parser.parse_args()

    summary = summarize_config()

    if args.test_discord:
        if not GLOBAL_WEBHOOK_URL:
            raise RuntimeError("Discord global webhook is not configured.")
        from send_discord_test import main as send_discord_main

        send_discord_main()
        summary["discord_test_sent"] = True

    if args.test_x:
        if not summary["x_configured"]:
            raise RuntimeError("X credentials are not configured.")
        if not summary["x_posting_enabled"]:
            raise RuntimeError("TWITTER_POSTING_ENABLED is disabled.")
        user = get_authenticated_user()
        summary["x_authenticated_user"] = {
            "id": user.get("id"),
            "name": user.get("name"),
            "username": user.get("username"),
        }

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
