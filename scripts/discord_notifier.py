import os
import requests
from dotenv import load_dotenv

load_dotenv()

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

def send_discord_alert(category, title, description, url, color=0x3b82f6):
    """
    Sends a rich embed message to a Discord Webhook.
    Colors: Blue (0x3b82f6) for Info, Green (0x10b981) for Buys, Red (0xef4444) for Sells.
    """
    if not DISCORD_WEBHOOK_URL:
        print("Webhook URL not configured.")
        return

    payload = {
        "username": "FinLens Engine",
        "avatar_url": "https://your-domain.com/logo.png",
        "embeds": [
            {
                "title": f"🚨 {category}: {title}",
                "description": description,
                "url": url,
                "color": color,
                "timestamp": __import__('datetime').datetime.now(tz=__import__('datetime').timezone.utc).isoformat()
            }
        ]
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print(f"Discord Alert Sent: {title}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending Discord alert: {e}")

# Example usage (can be imported by the other scrapers)
if __name__ == "__main__":
    send_discord_alert(
        category="Massive Politician Buy",
        title="Nancy Pelosi purchased $1M-$5M NVDA",
        description="**Ticker**: $NVDA\n**Chamber**: House\n**Amount**: $1,000,001 - $5,000,000",
        url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20023456.pdf",
        color=0x10b981 # Green
    )
