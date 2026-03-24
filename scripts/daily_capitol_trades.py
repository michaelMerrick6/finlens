"""
Daily Capitol Trades Monitor — checks for new trades published today/recently.
Designed to run frequently (every 2-4 hours) to catch new filings quickly.
Only scrapes the first 5 pages (most recent ~480 trades) for speed.
"""
import os
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv
from legacy_congress_guard import require_legacy_write_opt_in

load_dotenv(dotenv_path=".env.local")
require_legacy_write_opt_in("daily_capitol_trades.py")

url_env: str = os.environ.get("SUPABASE_URL", "")
key_env: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url_env, key_env)

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "")
TRADES_URL = "https://www.capitoltrades.com/trades"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

AMOUNT_MAP = {
    "1K–15K": "$1,001 - $15,000",
    "15K–50K": "$15,001 - $50,000",
    "50K–100K": "$50,001 - $100,000",
    "100K–250K": "$100,001 - $250,000",
    "250K–500K": "$250,001 - $500,000",
    "500K–1M": "$500,001 - $1,000,000",
    "1M–5M": "$1,000,001 - $5,000,000",
    "5M–25M": "$5,000,001 - $25,000,000",
    "25M–50M": "$25,000,001 - $50,000,000",
    "50M+": "Over $50,000,000",
}


def parse_trade_date(cell_text: str) -> str:
    cell_text = cell_text.strip()
    match = re.search(r'(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})', cell_text)
    if match:
        day, month, year = match.groups()
        try:
            return datetime.strptime(f"{day} {month} {year}", "%d %b %Y").strftime("%Y-%m-%d")
        except:
            pass
    return ""


def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    trades = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue
        try:
            pol_link = cells[0].find("a")
            politician_name = pol_link.get_text(strip=True) if pol_link else ""
            if not politician_name:
                continue

            party_span = cells[0].select_one("[class*='party--']")
            party = "Unknown"
            if party_span:
                classes = " ".join(party_span.get("class", []))
                if "republican" in classes: party = "Republican"
                elif "democrat" in classes: party = "Democrat"
                elif "independent" in classes: party = "Independent"

            chamber_span = cells[0].select_one("[class*='chamber--']")
            chamber = "House"
            if chamber_span and "senate" in " ".join(chamber_span.get("class", [])):
                chamber = "Senate"

            state_span = cells[0].select_one("[class*='us-state-compact--']")
            state = state_span.get_text(strip=True).upper() if state_span else ""

            ticker_span = cells[1].select_one("[class*='issuer-ticker']")
            ticker_raw = ticker_span.get_text(strip=True) if ticker_span else ""
            ticker = ticker_raw.split(":")[0].strip().upper() if ticker_raw else ""

            issuer_link = cells[1].find("a")
            asset_name = issuer_link.get_text(strip=True) if issuer_link else ""

            if not ticker or not re.match(r'^[A-Z]{1,6}$', ticker):
                continue

            tx_date = parse_trade_date(cells[3].get_text(strip=True))
            if not tx_date:
                continue

            pub_date = parse_trade_date(cells[2].get_text(strip=True))
            if not pub_date:
                pub_date = tx_date

            type_span = cells[6].select_one("[class*='tx-type']")
            tx_type = type_span.get_text(strip=True).lower() if type_span else ""
            if tx_type not in ("buy", "sell"):
                cell_text = cells[6].get_text(strip=True).lower()
                if "buy" in cell_text: tx_type = "buy"
                elif "sell" in cell_text: tx_type = "sell"
                else: tx_type = "exchange"

            size_span = cells[7].select_one("[class*='trade-size']")
            size_text = size_span.get_text(strip=True) if size_span else ""
            amount = AMOUNT_MAP.get(size_text, size_text)

            trades.append({
                "politician_name": politician_name[:100],
                "party": party,
                "chamber": chamber,
                "state": state,
                "ticker": ticker[:10],
                "asset_name": asset_name[:255],
                "transaction_type": tx_type,
                "transaction_date": tx_date,
                "published_date": pub_date,
                "amount_range": amount[:255],
            })
        except:
            continue
    return trades


def send_discord_alert(trade: dict):
    """Send a Discord notification for a new trade."""
    if not DISCORD_WEBHOOK:
        return
    emoji = "🟢" if trade["transaction_type"] == "buy" else "🔴"
    msg = (
        f"{emoji} **New {trade['transaction_type'].upper()}** by "
        f"**{trade['politician_name']}** ({trade['party']}, {trade['chamber']})\n"
        f"📊 **{trade['ticker']}** — {trade['asset_name']}\n"
        f"💰 {trade['amount_range']}\n"
        f"📅 Traded: {trade['transaction_date']} | Filed: {trade['published_date']}"
    )
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=5)
    except:
        pass


def fetch_recent_keys() -> set:
    """Fetch dedup keys for trades from the last 90 days (enough for daily check)."""
    keys = set()
    try:
        cutoff = datetime.now().strftime("%Y-%m-%d")
        # Get recent trades for dedup
        offset = 0
        while True:
            resp = supabase.table("politician_trades").select(
                "politician_name,ticker,transaction_date,transaction_type"
            ).order("published_date", desc=True).range(offset, offset + 4999).execute()
            if not resp.data:
                break
            for row in resp.data:
                k = f"{row.get('politician_name','')}-{row.get('ticker','')}-{row.get('transaction_date','')}-{row.get('transaction_type','')}"
                keys.add(k.lower())
            if len(resp.data) < 5000:
                break
            offset += 5000
    except Exception as e:
        print(f"Warning fetching keys: {e}")
    return keys


def resolve_member(name: str, chamber: str, party: str, state: str, cache: dict) -> str:
    cache_key = name.lower()
    if cache_key in cache:
        return cache[cache_key]

    parts = name.strip().split()
    first = parts[0] if parts else name
    last = parts[-1] if len(parts) > 1 else ""

    try:
        resp = supabase.table("congress_members").select("id").or_(
            f"last_name.ilike.%{last}%"
        ).execute()
        if resp.data:
            cache[cache_key] = resp.data[0]["id"]
            return resp.data[0]["id"]
    except:
        pass

    member_id = f"unknown-{first.lower()}-{last.lower()}"[:50]
    try:
        supabase.table("congress_members").upsert({
            "id": member_id, "first_name": first, "last_name": last,
            "chamber": chamber, "party": party, "state": state,
        }).execute()
    except:
        pass
    cache[cache_key] = member_id
    return member_id


def run_daily_monitor():
    """Check the first 5 pages of Capitol Trades for new filings."""
    print(f"[{datetime.now()}] Capitol Trades Daily Monitor starting...")

    existing_keys = fetch_recent_keys()
    print(f"  Loaded {len(existing_keys)} existing trade keys for dedup")

    members_cache = {}
    new_trades = []
    dupes = 0
    MAX_PAGES = 5  # Only check first 5 pages for speed

    for page in range(1, MAX_PAGES + 1):
        try:
            resp = SESSION.get(TRADES_URL, params={"page": page, "pageSize": 96}, timeout=30)
            if resp.status_code != 200:
                break
        except:
            break

        trades = parse_page(resp.text)
        if not trades:
            break

        for trade in trades:
            dedup_key = f"{trade['politician_name']}-{trade['ticker']}-{trade['transaction_date']}-{trade['transaction_type']}".lower()
            if dedup_key in existing_keys:
                dupes += 1
                continue

            existing_keys.add(dedup_key)
            member_id = resolve_member(trade["politician_name"], trade["chamber"],
                                        trade["party"], trade["state"], members_cache)

            try:
                supabase.table("companies").upsert({
                    "ticker": trade["ticker"], "name": trade["asset_name"] or trade["ticker"],
                    "sector": "Unknown", "industry": "Unknown",
                }).execute()
            except:
                pass

            doc_id = f"capitol-{dedup_key}".replace(" ", "-")[:100]

            db_trade = {
                "member_id": member_id,
                "politician_name": trade["politician_name"],
                "chamber": trade["chamber"],
                "party": trade["party"],
                "ticker": trade["ticker"],
                "transaction_date": trade["transaction_date"],
                "published_date": trade["published_date"],
                "transaction_type": trade["transaction_type"],
                "asset_type": "Stock",
                "amount_range": trade["amount_range"],
                "source_url": "https://www.capitoltrades.com/trades",
                "doc_id": doc_id,
            }
            new_trades.append(db_trade)

            # Send Discord alert for each new trade
            send_discord_alert(trade)

        time.sleep(1)

    # Insert new trades
    if new_trades:
        print(f"  Inserting {len(new_trades)} new trades...")
        for i in range(0, len(new_trades), 50):
            chunk = new_trades[i:i + 50]
            try:
                supabase.table("politician_trades").insert(chunk).execute()
            except Exception as e:
                print(f"  Insert error: {e}")

    print(f"[{datetime.now()}] Capitol Trades Monitor complete: {len(new_trades)} new, {dupes} dupes skipped")
    return len(new_trades)


if __name__ == "__main__":
    run_daily_monitor()
