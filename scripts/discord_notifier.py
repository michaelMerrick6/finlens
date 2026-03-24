import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

def send_discord_payload(payload, webhook_url=None):
    destination = webhook_url or DISCORD_WEBHOOK_URL
    if not destination:
        print("Webhook URL not configured.")
        return False

    try:
        response = requests.post(destination, json=payload, timeout=15)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error sending Discord payload: {e}")
        return False


def send_discord_alert(category, title, description, url, color=0x3b82f6, webhook_url=None):
    if not (webhook_url or DISCORD_WEBHOOK_URL):
        print("Webhook URL not configured.")
        return False

    payload = {
        "username": "FinLens Engine",
        "avatar_url": "https://your-domain.com/logo.png",
        "embeds": [
            {
                "title": f"🚨 {category}: {title}",
                "description": description,
                "url": url,
                "color": color,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        ]
    }

    if send_discord_payload(payload, webhook_url=webhook_url):
        print(f"Discord Alert Sent: {title}")
        return True
    return False

# Example usage (can be imported by the other scrapers)
if __name__ == "__main__":
    send_discord_alert(
        category="Massive Politician Buy",
        title="Nancy Pelosi purchased $1M-$5M NVDA",
        description="**Ticker**: $NVDA\n**Chamber**: House\n**Amount**: $1,000,001 - $5,000,000",
        url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20023456.pdf",
        color=0x10b981 # Green
    )
