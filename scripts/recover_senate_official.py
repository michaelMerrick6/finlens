import os
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv
from legacy_congress_guard import require_legacy_write_opt_in
from politician_schema_support import politician_trades_has_asset_name_column

load_dotenv(dotenv_path=".env.local")
require_legacy_write_opt_in("recover_senate_official.py")

url_env: str = os.environ.get("SUPABASE_URL", "")
key_env: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url_env, key_env)
SUPPORTS_ASSET_NAME = politician_trades_has_asset_name_column(supabase)

SENATE_BASE_URL = "https://efdsearch.senate.gov"
SENATE_HOME_URL = f"{SENATE_BASE_URL}/search/home/"
SENATE_REPORT_DATA_URL = f"{SENATE_BASE_URL}/search/report/data/"

def recover_senate_backfill():
    print("--- Senate Recovery Mission (2015-2026) ---")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    
    # Setup session / agree to TOS
    home = session.get(SENATE_HOME_URL)
    csrf = re.search(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"', home.text).group(1)
    session.post(SENATE_HOME_URL, data={"csrfmiddlewaretoken": csrf, "prohibition_agreement": "1"})
    cookie_csrf = session.cookies.get("csrftoken")
    
    # We will search specifically for Periodic Transaction Reports
    # and we will iterate by date ranges to ensure we get everything
    ranges = [
        ("01/01/2015", "12/31/2017"),
        ("01/01/2018", "12/31/2020"),
        ("01/01/2021", "12/31/2023"),
        ("01/01/2024", "03/17/2026"),
    ]
    
    total_recovered = 0
    
    for start_d, end_d in ranges:
        print(f"Scanning range: {start_d} to {end_d}")
        
        payload = {
            "start": "0", "length": "1000", "report_types": "[11]",
            "submitted_start_date": start_d, "submitted_end_date": end_d,
            "csrfmiddlewaretoken": cookie_csrf,
        }
        
        r = session.post(SENATE_REPORT_DATA_URL, data=payload, headers={"Referer": f"{SENATE_BASE_URL}/search/"})
        if r.status_code != 200:
            print(f" Error: {r.status_code}")
            continue
            
        try:
            data = r.json()
            filings = data.get("data", [])
        except Exception as e:
            print(f" JSON Error: {e}")
            if "<title>eFD: Find Reports</title>" in r.text or "Term of Service" in r.text:
                print(" Blocked by TOS/Session reset.")
            else:
                print(f" Raw Response (first 100): {r.text[:100]}")
            continue
        print(f" Found {len(filings)} filings in range.")
        
        for f in filings:
            fname, lname = f[0], f[1]
            pub_date = datetime.strptime(f[4], "%m/%d/%Y").strftime("%Y-%m-%d")
            link_html = f[3]
            href = re.search(r'href="(.*?)"', link_html).group(1)
            
            if "/search/view/paper/" in href: continue
            
            doc_id = f"senate-{href.split('/')[-2]}"
            
            # Check if exists
            exists = supabase.table("politician_trades").select("id").eq("doc_id", doc_id).execute()
            if exists.data: continue
            
            # Deep Scrape the detail page
            detail_url = f"{SENATE_BASE_URL}{href}"
            try:
                det = session.get(detail_url, timeout=10)
                soup = BeautifulSoup(det.text, "html.parser")
                rows = soup.select("table.table tbody tr")
                
                trades = []
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 8: continue
                    
                    tx_date_raw = cells[1].get_text(strip=True)
                    ticker_raw = cells[3].get_text(strip=True).split('<')[0].strip()
                    issuer = cells[4].get_text(strip=True)
                    tx_type_raw = cells[6].get_text(strip=True).lower()
                    amount = cells[7].get_text(strip=True)
                    
                    if not ticker_raw or ticker_raw == "--": continue
                    
                    tx_type = "buy" if "purchase" in tx_type_raw else "sell" if "sale" in tx_type_raw else "exchange"
                    
                    trades.append({
                        "politician_name": f"{fname} {lname}",
                        "chamber": "Senate",
                        "ticker": ticker_raw,
                        **({"asset_name": issuer[:255]} if SUPPORTS_ASSET_NAME and issuer else {}),
                        "transaction_date": datetime.strptime(tx_date_raw, "%m/%d/%Y").strftime("%Y-%m-%d"),
                        "published_date": pub_date,
                        "transaction_type": tx_type,
                        "amount_range": amount,
                        "source_url": detail_url,
                        "doc_id": doc_id + f"-{len(trades)}"
                    })
                
                if trades:
                    supabase.table("politician_trades").upsert(trades, on_conflict="doc_id").execute()
                    total_recovered += len(trades)
                    print(f" [+] Recovered {len(trades)} trades for {fname} {lname}")
                
                time.sleep(0.5) # Ratelimit
            except: pass

    print(f"Senate Recovery Complete! Total Recovered: {total_recovered}")

if __name__ == "__main__":
    recover_senate_backfill()
