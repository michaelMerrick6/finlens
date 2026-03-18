import os
import json
import re
import time
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

SENATE_BASE_URL = "https://efdsearch.senate.gov"
SENATE_HOME_URL = f"{SENATE_BASE_URL}/search/home/"
SENATE_SEARCH_URL = f"{SENATE_BASE_URL}/search/"
SENATE_REPORT_DATA_URL = f"{SENATE_BASE_URL}/search/report/data/"

CSRF_INPUT_RE = re.compile(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"')
DOCUMENT_HREF_RE = re.compile(r'href="(.*?)"')

def fetch_senate_trades():
    print("Starting Official Senate eFD Scraper...")
    session = requests.Session()
    # Spoof a real browser
    session.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
    
    # Step 1: Hit home to get initial CSRF
    print("1. Bypassing Senate eFD Terms of Service Gateway...")
    home = session.get(SENATE_HOME_URL, timeout=10)
    home.raise_for_status()
    
    match = CSRF_INPUT_RE.search(home.text)
    if not match:
        print("Failed to find CSRF token.")
        return
    csrf_token = match.group(1)
    
    # Step 2: Agree to prohibition
    resp = session.post(
        SENATE_HOME_URL,
        data={
            "csrfmiddlewaretoken": csrf_token,
            "prohibition_agreement": "1",
        },
        headers={"Referer": SENATE_HOME_URL},
        timeout=10
    )
    resp.raise_for_status()
    
    # We must grab the live cookie CSRF for the ajax
    cookie_csrf = session.cookies.get("csrftoken") or session.cookies.get("csrf")
    if not cookie_csrf:
        print("Failed to get csrf cookie.")
        return
        
    # Step 3: Hit the search API to get Periodic Transaction Reports (type 11)
    print("2. Fetching historical PTR reports via pagination...")
    
    all_rows = []
    daily_mode = os.environ.get("FINLENS_DAILY_MODE", "0") == "1"
    max_pagination = 200 if daily_mode else 10000
    
    for start_offset in range(0, max_pagination, 100):
        print(f" -> Fetching offset {start_offset}...")
        payload = {
            "start": str(start_offset),
            "length": "100",
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": "01/01/2012 00:00:00", # Expanded to Absolute Limit
            "submitted_end_date": "",
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
            "csrfmiddlewaretoken": cookie_csrf,
        }
        try:
            search_resp = session.post(
                SENATE_REPORT_DATA_URL,
                data=payload,
                headers={"Referer": SENATE_SEARCH_URL},
                timeout=30
            )
            search_resp.raise_for_status()
            data = search_resp.json()
            chunk_rows = data.get("data", [])
            if not chunk_rows:
                break
            all_rows.extend(chunk_rows)
            time.sleep(1) # wait between api pagination calls
        except Exception as e:
            print(f"Pagination error at offset {start_offset}: {e}")
            break
            
    print(f"Found {len(all_rows)} historical Senate PTR filings across pagination. Processing ALL records for maximum historical depth.")
    
    # Pre-fetch known congress members to map IDs natively
    try:
        members_req = supabase.table("congress_members").select("id, first_name, last_name").execute()
        members_db = members_req.data if members_req else []
    except Exception as e:
        print(f"Warn: Could not fetch congress_members for mapping ({e})")
        members_db = []
        
    def resolve_member_id(first, last):
         for m in members_db:
              m_first = m["first_name"].lower()
              m_last = m["last_name"].lower()
              if (m_last in last.lower() or last.lower() in m_last) and \
                 (m_first in first.lower() or first.lower() in m_first):
                   return m["id"]
         
         # Fallback generation to prevent Foreign Key constraint crash
         new_id = f"unknown-{first.lower().replace(' ','')}-{last.lower().replace(' ','')}"[:50]
         # Lazy insert fallback - but only if NOT already in DB as an unknown
         found_existing_unknown = False
         for m in members_db:
              if m["id"] == new_id:
                   found_existing_unknown = True
                   break
         
         if not found_existing_unknown:
             try:
                 supabase.table("congress_members").upsert({
                     "id": new_id, "first_name": first, "last_name": last, "chamber": "Senate"
                 }).execute()
                 members_db.append({"id": new_id, "first_name": first, "last_name": last})
             except: pass
         return new_id
    
    formatted_trades = []
    
    # Process filings
    consecutive_existing = 0
    for row in all_rows:
        first_name = str(row[0]).strip()
        last_name = str(row[1]).strip()
        
        # Link is in row[3]
        link_str = str(row[3])
        href_match = DOCUMENT_HREF_RE.search(link_str)
        if not href_match: continue
        detail_path = href_match.group(1)
        
        # Skip paper for this scraper
        if "/search/view/paper/" in detail_path: continue
        
        member_id = resolve_member_id(first_name, last_name)
        
        try:
            recv_dt = datetime.strptime(str(row[4]).strip(), "%m/%d/%Y")
            filed_date = recv_dt.strftime("%Y-%m-%d")
        except:
            filed_date = "1970-01-01"
        
        # Check if the FIRST trade of this filing already exists
        # We use -0 as an anchor to see if we've processed this filing before
        doc_id_filing_anchor = f"senate-{detail_path.split('/')[-2]}-0"
        check = supabase.table("politician_trades").select("id").eq("doc_id", doc_id_filing_anchor).execute()
        if check.data:
            consecutive_existing += 1
            if daily_mode and consecutive_existing > 10:
                print(" -> Hit 10 consecutive existing Senate filings. Stopping.")
                break
            continue
            
        consecutive_existing = 0
            
        print(f"Scraping eFD for {first_name} {last_name} ({detail_path})...")
        url = f"{SENATE_BASE_URL}{detail_path}"
        try:
             detail_resp = session.get(url, headers={"Referer": SENATE_SEARCH_URL}, timeout=30)
        except Exception as e:
             print(f"Failed to fetch {url}: {e}")
             continue
        
        if "<title>eFD: Find Reports</title>" in detail_resp.text:
             continue
             
        soup = BeautifulSoup(detail_resp.text, "html.parser")
        table = soup.select_one("table.table")
        if not table:
            continue
        tbody = table.find("tbody")
        if not tbody:
            continue
            
        trade_idx = 0
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 8:
                continue
                
            tx_date_raw = cells[1].get_text(strip=True)
            ticker_raw = cells[3].get_text(strip=True)
            issuer_raw = cells[4].get_text(strip=True)
            tx_type_raw = cells[6].get_text(strip=True)
            amount_raw = cells[7].get_text(strip=True)
            
            ticker = ticker_raw.split('<')[0].strip() if ticker_raw and ticker_raw != "--" else "N/A"
            # No continue here, we want the trade even if ticker is N/A
                
            try:
                 supabase.table("companies").upsert({
                     "ticker": ticker[:10], "name": issuer_raw[:255], "sector": "Unknown", "industry": "Unknown"
                 }).execute()
            except Exception as e: pass
                 
            try:
                dt = datetime.strptime(tx_date_raw, "%m/%d/%Y")
                tx_date = dt.strftime("%Y-%m-%d")
            except:
                tx_date = "1970-01-01"
                
            tx_type_upper = tx_type_raw.upper()
            if "SALE" in tx_type_upper: tx_type = "sell"
            elif "PURCHASE" in tx_type_upper: tx_type = "buy"
            else: tx_type = "exchange"
                
            doc_id_slug = f"senate-{detail_path.split('/')[-2]}-{trade_idx}"
                
            formatted_trade = {
                "member_id": member_id,
                "politician_name": f"{first_name} {last_name}"[:100],
                "chamber": "Senate",
                "party": "Unknown",
                "ticker": ticker[:10],
                "transaction_date": tx_date,
                "published_date": filed_date,
                "transaction_type": tx_type[:10],
                "asset_type": "Stock",
                "amount_range": amount_raw[:255],
                "source_url": url[:500],
                "doc_id": doc_id_slug
            }
            formatted_trades.append(formatted_trade)
            trade_idx += 1
            
        time.sleep(0.5) # strict rate limit against senate.gov
        
    print(f"Parsed {len(formatted_trades)} detailed Senate trades from deep backlog.")
    
    if formatted_trades:
        print(f"Uploading {len(formatted_trades)} real Senate trades to Supabase...")
        # Insert in chunks of 50
        for i in range(0, len(formatted_trades), 50):
            chunk = formatted_trades[i:i + 50]
            try:
                # Manual Upsert: filter out already existing doc_ids
                doc_ids = [t["doc_id"] for t in chunk]
                existing = supabase.table("politician_trades").select("doc_id").in_("doc_id", doc_ids).execute()
                existing_ids = {r["doc_id"] for r in existing.data}
                
                to_insert = [t for t in chunk if t["doc_id"] not in existing_ids]
                if to_insert:
                    supabase.table("politician_trades").insert(to_insert).execute()
                    print(f" -> Inserted {len(to_insert)} new Senate trades.")
            except Exception as e:
                print(f"Error manual-upserting chunk: {e}")
                
        print("Successfully seeded SENATE trades!")

if __name__ == "__main__":
    fetch_senate_trades()
