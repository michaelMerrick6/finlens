"""
Capitol Trades Comprehensive Scraper — fills ALL gaps for members with handwritten PDFs.
Scrapes server-rendered HTML tables from capitoltrades.com.
Deduplicates against existing DB records by matching politician+ticker+date+type.
"""
import os
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url_env: str = os.environ.get("SUPABASE_URL", "")
key_env: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url_env, key_env)

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
    """Parse date from Capitol Trades format like '6 Feb2026' or '27 Feb2026'."""
    cell_text = cell_text.strip()
    # The format is day+space+month+year concatenated: "6 Feb2026"
    match = re.search(r'(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})', cell_text)
    if match:
        day, month, year = match.groups()
        try:
            return datetime.strptime(f"{day} {month} {year}", "%d %b %Y").strftime("%Y-%m-%d")
        except:
            pass
    return ""


def parse_page(html: str) -> list[dict]:
    """Parse a single page of Capitol Trades HTML into trade records."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    trades = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        try:
            # Cell 0: Politician
            pol_link = cells[0].find("a")
            politician_name = pol_link.get_text(strip=True) if pol_link else ""
            if not politician_name:
                continue

            # Party
            party_span = cells[0].select_one("[class*='party--']")
            party = "Unknown"
            if party_span:
                classes = " ".join(party_span.get("class", []))
                if "republican" in classes:
                    party = "Republican"
                elif "democrat" in classes:
                    party = "Democrat"
                elif "independent" in classes:
                    party = "Independent"

            # Chamber
            chamber_span = cells[0].select_one("[class*='chamber--']")
            chamber = "House"
            if chamber_span:
                classes = " ".join(chamber_span.get("class", []))
                if "senate" in classes:
                    chamber = "Senate"

            # State
            state_span = cells[0].select_one("[class*='us-state-compact--']")
            state = ""
            if state_span:
                state = state_span.get_text(strip=True).upper()

            # Cell 1: Issuer + Ticker
            ticker_span = cells[1].select_one("[class*='issuer-ticker']")
            ticker_raw = ticker_span.get_text(strip=True) if ticker_span else ""
            ticker = ticker_raw.split(":")[0].strip().upper() if ticker_raw else ""

            issuer_link = cells[1].find("a")
            asset_name = issuer_link.get_text(strip=True) if issuer_link else ""

            if not ticker or not re.match(r'^[A-Z]{1,6}$', ticker):
                continue

            # Cell 3: Trade date
            tx_date = parse_trade_date(cells[3].get_text(strip=True))
            if not tx_date:
                continue

            # Cell 2: Published date (may be relative like "Yesterday")
            pub_date = parse_trade_date(cells[2].get_text(strip=True))
            if not pub_date:
                pub_date = tx_date  # Fallback

            # Cell 6: Type
            type_span = cells[6].select_one("[class*='tx-type']")
            tx_type = type_span.get_text(strip=True).lower() if type_span else ""
            if tx_type not in ("buy", "sell"):
                if "buy" in cells[6].get_text(strip=True).lower():
                    tx_type = "buy"
                elif "sell" in cells[6].get_text(strip=True).lower():
                    tx_type = "sell"
                else:
                    tx_type = "exchange"

            # Cell 7: Size
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

        except Exception as e:
            continue

    return trades


def fetch_existing_keys() -> set:
    """Build a set of existing trade keys for deduplication."""
    keys = set()
    offset = 0
    while True:
        try:
            resp = supabase.table("politician_trades").select(
                "politician_name,ticker,transaction_date,transaction_type"
            ).range(offset, offset + 999).execute()
            if not resp.data:
                break
            for row in resp.data:
                k = f"{row.get('politician_name','')}-{row.get('ticker','')}-{row.get('transaction_date','')}-{row.get('transaction_type','')}"
                keys.add(k.lower())
            if len(resp.data) < 1000:
                break
            offset += 1000
        except:
            break
    return keys


def resolve_member(name: str, chamber: str, party: str, state: str, members_cache: dict) -> str:
    """Resolve or create a congress member."""
    cache_key = name.lower()
    if cache_key in members_cache:
        return members_cache[cache_key]

    parts = name.strip().split()
    first = parts[0] if parts else name
    last = parts[-1] if len(parts) > 1 else ""

    # Check existing
    try:
        resp = supabase.table("congress_members").select("id").or_(
            f"last_name.ilike.%{last}%"
        ).execute()
        if resp.data:
            for m in resp.data:
                members_cache[cache_key] = m["id"]
                return m["id"]
    except:
        pass

    member_id = f"unknown-{first.lower()}-{last.lower()}"[:50]
    try:
        supabase.table("congress_members").upsert({
            "id": member_id,
            "first_name": first,
            "last_name": last,
            "chamber": chamber,
            "party": party,
            "state": state,
        }).execute()
    except:
        pass

    members_cache[cache_key] = member_id
    return member_id


def run():
    """Main scraper: scrape all Capitol Trades pages and insert missing trades."""
    print("=" * 60)
    print("CAPITOL TRADES COMPREHENSIVE SCRAPER")
    print("=" * 60)

    print("\nFetching existing trade keys for deduplication...")
    existing_keys = fetch_existing_keys()
    print(f"Found {len(existing_keys)} existing trades in database")

    members_cache: dict = {}
    new_trades_total = 0
    dupes_total = 0
    batch = []
    max_pages = 500  # Safety limit

    for page in range(1, max_pages + 1):
        print(f"\n--- Page {page} ---")

        try:
            resp = SESSION.get(TRADES_URL, params={"page": page, "pageSize": 96}, timeout=30)
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}, stopping.")
                break
        except Exception as e:
            print(f"Request error: {e}, stopping.")
            break

        trades = parse_page(resp.text)
        if not trades:
            print("No trades found, stopping.")
            break

        print(f"Parsed {len(trades)} trades")

        new_on_page = 0
        dupes_on_page = 0

        for trade in trades:
            dedup_key = f"{trade['politician_name']}-{trade['ticker']}-{trade['transaction_date']}-{trade['transaction_type']}".lower()

            if dedup_key in existing_keys:
                dupes_on_page += 1
                continue

            existing_keys.add(dedup_key)
            member_id = resolve_member(
                trade["politician_name"], trade["chamber"],
                trade["party"], trade["state"], members_cache
            )

            # Upsert company
            try:
                supabase.table("companies").upsert({
                    "ticker": trade["ticker"],
                    "name": trade["asset_name"] or trade["ticker"],
                    "sector": "Unknown",
                    "industry": "Unknown",
                }).execute()
            except:
                pass

            doc_id = f"capitol-{dedup_key}".replace(" ", "-")[:100]

            batch.append({
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
            })
            new_on_page += 1

        print(f"  New: {new_on_page} | Dupes skipped: {dupes_on_page}")
        new_trades_total += new_on_page
        dupes_total += dupes_on_page

        # Upload in batches of 200
        if len(batch) >= 200:
            print(f"  Uploading batch of {len(batch)}...")
            for i in range(0, len(batch), 50):
                chunk = batch[i:i + 50]
                try:
                    supabase.table("politician_trades").insert(chunk).execute()
                except Exception as e:
                    print(f"  Insert error: {e}")
            batch = []

        # If we got mostly dupes on this page (>80%), we're deep into existing data
        if dupes_on_page > 0 and dupes_on_page / len(trades) > 0.95 and page > 5:
            print(f"  95%+ duplicates — deep into existing data on page {page}.")
            # Keep going, we might find gaps

        time.sleep(1.5)  # Rate limit

    # Upload remaining
    if batch:
        print(f"\nUploading final batch of {len(batch)}...")
        for i in range(0, len(batch), 50):
            chunk = batch[i:i + 50]
            try:
                supabase.table("politician_trades").insert(chunk).execute()
            except Exception as e:
                print(f"  Insert error: {e}")

    print(f"\n{'=' * 60}")
    print(f"SCRAPE COMPLETE")
    print(f"  New trades inserted: {new_trades_total}")
    print(f"  Duplicates skipped: {dupes_total}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run()
