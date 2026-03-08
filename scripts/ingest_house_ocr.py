"""
Capitol Trades Scraper — Fills gaps for members with handwritten PDFs.
Scrapes all congress trades from capitoltrades.com and inserts any missing ones into Supabase.
"""
import os
import re
import time
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

BASE_URL = "https://www.capitoltrades.com"
TRADES_URL = f"{BASE_URL}/trades"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})


def parse_amount_range(amount_text: str) -> str:
    """Normalize Capitol Trades amount format to our DB format."""
    amount_text = amount_text.strip()
    # Capitol Trades uses: $1K–$15K, $15K–$50K, $50K–$100K, etc.
    mapping = {
        "$1K–$15K": "$1,001 - $15,000",
        "$15K–$50K": "$15,001 - $50,000",
        "$50K–$100K": "$50,001 - $100,000",
        "$100K–$250K": "$100,001 - $250,000",
        "$250K–$500K": "$250,001 - $500,000",
        "$500K–$1M": "$500,001 - $1,000,000",
        "$1M–$5M": "$1,000,001 - $5,000,000",
        "$5M–$25M": "$5,000,001 - $25,000,000",
        "$25M–$50M": "$25,000,001 - $50,000,000",
        "$50M+": "Over $50,000,000",
    }
    return mapping.get(amount_text, amount_text)


def parse_date(date_text: str) -> str:
    """Parse Capitol Trades date format (e.g., '6 Nov 2025') to YYYY-MM-DD."""
    date_text = date_text.strip()
    for fmt in ["%d %b %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_text, fmt).strftime("%Y-%m-%d")
        except:
            continue
    return ""


def clean_ticker(ticker_text: str) -> str:
    """Clean ticker from Capitol Trades format (e.g., 'META:US' -> 'META')."""
    ticker = ticker_text.strip().split(":")[0].strip()
    # Remove any non-alpha characters
    ticker = re.sub(r'[^A-Z]', '', ticker.upper())
    return ticker[:10] if ticker else ""


def fetch_existing_doc_ids() -> set:
    """Get all doc_ids already in the database."""
    existing = set()
    try:
        offset = 0
        while True:
            resp = supabase.table("politician_trades").select("doc_id").range(offset, offset + 999).execute()
            if not resp.data:
                break
            for row in resp.data:
                if row.get("doc_id"):
                    existing.add(row["doc_id"])
            if len(resp.data) < 1000:
                break
            offset += 1000
    except Exception as e:
        print(f"Warning: {e}")
    return existing


def resolve_member_id(politician_name: str, chamber: str, members_db: list) -> str:
    """Resolve or create member ID."""
    parts = politician_name.strip().split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
    else:
        first = politician_name
        last = ""

    for m in members_db:
        if last.lower() in m.get("last_name", "").lower() and first.lower() in m.get("first_name", "").lower():
            return m["id"]

    member_id = f"unknown-{first.lower()}-{last.lower()}"[:50]
    try:
        supabase.table("congress_members").upsert({
            "id": member_id, "first_name": first, "last_name": last, "chamber": chamber
        }).execute()
        members_db.append({"id": member_id, "first_name": first, "last_name": last})
    except:
        pass
    return member_id


def scrape_trades_page(page_num: int = 1) -> tuple[list[dict], bool]:
    """Scrape a single page of trades from Capitol Trades. Returns (trades, has_next_page)."""
    trades = []

    params = {"page": page_num, "pageSize": 96}
    try:
        resp = SESSION.get(TRADES_URL, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  Page {page_num}: HTTP {resp.status_code}")
            return [], False
    except Exception as e:
        print(f"  Page {page_num}: Error {e}")
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find trade rows — Capitol Trades uses table rows or card divs
    # Try table-based layout
    rows = soup.select("table tbody tr")
    if not rows:
        rows = soup.select(".trades-table tr, .trade-row, [data-trade]")

    for row in rows:
        try:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            # Extract data from cells
            # Capitol Trades layout: Politician | Traded Issuer | Published | Traded | Owned | Type | Size
            politician_text = cells[0].get_text(strip=True)
            issuer_cell = cells[1]
            published_date = cells[2].get_text(strip=True)
            traded_date = cells[3].get_text(strip=True)

            # Get ticker from issuer cell
            ticker_el = issuer_cell.select_one(".q-field-issuer-ticker, .ticker, [class*='ticker']")
            ticker = clean_ticker(ticker_el.get_text()) if ticker_el else ""

            asset_name = issuer_cell.select_one(".q-field-issuer-name, .name")
            asset_name = asset_name.get_text(strip=True) if asset_name else issuer_cell.get_text(strip=True)

            # Type and size
            type_text = ""
            size_text = ""
            for cell in cells:
                text = cell.get_text(strip=True).lower()
                if text in ["buy", "sell", "exchange"]:
                    type_text = text
                if "$" in cell.get_text():
                    size_text = cell.get_text(strip=True)

            if not ticker or not type_text:
                continue

            # Determine chamber
            chamber = "House"  # Default
            chamber_el = row.select_one("[class*='chamber'], .q-field-politician-chamber")
            if chamber_el:
                ch = chamber_el.get_text(strip=True).lower()
                if "senate" in ch:
                    chamber = "Senate"

            trades.append({
                "politician_name": politician_text,
                "ticker": ticker,
                "asset_name": asset_name,
                "transaction_type": type_text,
                "transaction_date": parse_date(traded_date),
                "published_date": parse_date(published_date),
                "amount_range": parse_amount_range(size_text),
                "chamber": chamber,
            })

        except Exception as e:
            continue

    # Check for next page
    has_next = bool(soup.select_one("a[rel='next'], .pagination .next, button.next-page"))

    return trades, has_next


def scrape_all_trades(max_pages: int = 50):
    """Scrape all available pages from Capitol Trades."""
    print("Loading congress members from database...")
    try:
        members_req = supabase.table("congress_members").select("id, first_name, last_name").execute()
        members_db = members_req.data if members_req else []
    except:
        members_db = []

    existing_doc_ids = fetch_existing_doc_ids()
    print(f"Found {len(existing_doc_ids)} existing trades in database")

    all_new_trades = []
    duplicates = 0

    for page in range(1, max_pages + 1):
        print(f"\nScraping page {page}...")
        trades, has_next = scrape_trades_page(page)

        if not trades:
            print(f"  No trades found on page {page}, stopping.")
            break

        print(f"  Found {len(trades)} trades")

        for trade in trades:
            # Create unique doc_id for dedup
            doc_id = f"capitol-{trade['politician_name']}-{trade['ticker']}-{trade['transaction_date']}-{trade['transaction_type']}"
            doc_id = doc_id[:100].lower().replace(" ", "-")

            if doc_id in existing_doc_ids:
                duplicates += 1
                continue

            member_id = resolve_member_id(trade["politician_name"], trade["chamber"], members_db)

            # Upsert company
            try:
                supabase.table("companies").upsert({
                    "ticker": trade["ticker"],
                    "name": trade["asset_name"][:255],
                    "sector": "Unknown",
                    "industry": "Unknown"
                }).execute()
            except:
                pass

            db_trade = {
                "member_id": member_id,
                "politician_name": trade["politician_name"][:100],
                "chamber": trade["chamber"],
                "party": "Unknown",
                "ticker": trade["ticker"],
                "transaction_date": trade["transaction_date"],
                "published_date": trade["published_date"],
                "transaction_type": trade["transaction_type"],
                "asset_type": "Stock",
                "amount_range": trade["amount_range"][:255],
                "source_url": f"https://www.capitoltrades.com/trades",
                "doc_id": doc_id,
            }
            all_new_trades.append(db_trade)
            existing_doc_ids.add(doc_id)

        # Upload in batches
        if len(all_new_trades) >= 100:
            print(f"  Uploading batch of {len(all_new_trades)} trades...")
            for i in range(0, len(all_new_trades), 50):
                chunk = all_new_trades[i:i + 50]
                try:
                    supabase.table("politician_trades").insert(chunk).execute()
                except Exception as e:
                    print(f"  Insert error: {e}")
            all_new_trades = []

        if not has_next:
            print("  No more pages.")
            break

        time.sleep(1)  # Rate limit

    # Upload remaining
    if all_new_trades:
        print(f"\nUploading final batch of {len(all_new_trades)} trades...")
        for i in range(0, len(all_new_trades), 50):
            chunk = all_new_trades[i:i + 50]
            try:
                supabase.table("politician_trades").insert(chunk).execute()
            except Exception as e:
                print(f"  Insert error: {e}")

    print(f"\n{'='*60}")
    print(f"CAPITOL TRADES SCRAPE COMPLETE")
    print(f"  New trades inserted: {len(all_new_trades)}")
    print(f"  Duplicates skipped: {duplicates}")
    print(f"{'='*60}")


if __name__ == "__main__":
    scrape_all_trades()
