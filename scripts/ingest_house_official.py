import os
import io
import csv
import re
from datetime import datetime
import requests
from pypdf import PdfReader
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

HOUSE_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt"
HOUSE_PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

TX_DETAIL_RE = re.compile(
    r"^(?P<tx_code>[A-Z])(?:\s+\((?P<qualifier>[^)]+)\))?\s*"
    r"(?P<tx_date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<notif_date>\d{2}/\d{2}/\d{4})\s*"
    r"(?P<amount>.+)$"
)
ASSET_LINE_RE = re.compile(r"^(?P<asset_prefix>.*)\[(?P<asset_type>[A-Z]{2})\]\s*(?P<tail>.*)$")
TABLE_HEADER_LINES = {
    "T",
    "ID OwnerAsset Transaction",
    "ID Owner Asset Transaction",
    "TypeDate Notification",
    "Type",
    "Date Notification",
    "Date",
    "DateAmount Cap .",
    "Amount Cap.",
    "Gains >",
    "$200?",
}
TABLE_STOP_PREFIXES = ("* For the complete list", "I V D")
TABLE_RESET_PREFIXES = ("F S:", "S O:")

def extract_pdf_lines(pdf_bytes: bytes) -> list[str]:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        lines = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                # Strip null bytes — many House PDFs embed \x00 chars that break regex
                text = text.replace('\x00', '')
                lines.extend(text.splitlines())
        return lines
    except Exception as e:
        print(f"Error reading PDF: {e}")
        return []

def extract_transactions_from_lines(lines: list[str], doc_id: str, first_name: str, last_name: str, tx_year: int, members_db: list) -> list[dict]:
    transactions = []
    full_text = " ".join(lines).replace('\x00', '')
    
    # Resolve Bioguide ID - More robust logic
    member_id = None
    
    # Try exact match first
    for m in members_db:
         m_first = m["first_name"].lower()
         m_last = m["last_name"].lower()
         # Check for name overlaps (e.g. April McClain vs April McClain Delaney)
         if (m_last in last_name.lower() or last_name.lower() in m_last) and \
            (m_first in first_name.lower() or first_name.lower() in m_first):
              member_id = m["id"]
              break
              
    if not member_id:
         # Fuzzy placeholder
         member_id = f"unknown-{first_name.lower().replace(' ','')}-{last_name.lower().replace(' ','')}"[:50]
         # Lazy insert fallback - but only if NOT already in DB as an unknown
         found_existing_unknown = False
         for m in members_db:
              if m["id"] == member_id:
                   found_existing_unknown = True
                   break
         
         if not found_existing_unknown:
             try:
                 supabase.table("congress_members").upsert({
                     "id": member_id, "first_name": first_name, "last_name": last_name, "chamber": "House"
                 }).execute()
                 members_db.append({"id": member_id, "first_name": first_name, "last_name": last_name})
             except: pass

    tx_pattern = re.compile(
        r"(?P<asset_name>.+?)\s*"
        r"\[([A-Z]{2})\]\s*"
        r"([PSE])\s*(?:\([^)]*\)\s*)?"
        r"(\d{1,2}/\d{1,2}/\d{4})\s*"
        r"(\d{1,2}/\d{1,2}/\d{4})\s*"
        r"(\$[0-9,]+\s*(?:-\s*\$[0-9,]+)?)",
        flags=re.DOTALL
    )
    
    matches = list(tx_pattern.finditer(full_text))

    for trade_idx, match in enumerate(matches):
        asset_name = match.group("asset_name").strip()
        asset_type = match.group(2)
        tx_code = match.group(3)
        tx_date_raw = match.group(4)
        published_date_raw = match.group(5)
        amount_text = match.group(6).strip()

        ticker_match = re.search(r'\(([A-Z]{1,6})\)', asset_name)
        if ticker_match:
            ticker = ticker_match.group(1)[:10]
        else:
            ticker = "N/A"
            # Best effort mapping for Treasuries
            if "UNITED STATES TREAS" in asset_name.upper():
                ticker = "US-TREAS"
            elif "ALPHABET" in asset_name.upper():
                ticker = "GOOGL"
            elif "CHUBB" in asset_name.upper():
                ticker = "CB"

        tx_type = {"P": "buy", "S": "sell", "E": "exchange"}.get(tx_code, "unknown")
        
        try:
            tx_date = datetime.strptime(tx_date_raw, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue # Skip if transaction date is invalid
        
        try:
            published_date = datetime.strptime(published_date_raw, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            published_date = tx_date # Default to transaction date if notification date is invalid
        
        # Clean asset_name for company name
        company_name = re.sub(r'\([^)]*\)', '', asset_name).strip()
        # Remove common PDF noise/metadata
        for noise in ["F S: New", "F S: Amended", "S O:", "D:", "ID Owner", "Owner"]:
            if noise in company_name:
                company_name = company_name.split(noise)[-1].strip()
                
        if "$200?" in company_name:
            company_name = company_name.split("$200?")[-1].strip()
        elif "Amount Cap." in company_name:
            company_name = company_name.split("Amount Cap.")[-1].strip()
            
        # If the name is still messy with multi-line junk, take the last part
        if "\n" in company_name:
            company_name = company_name.split("\n")[-1].strip()
            
        try:
            supabase.table("companies").upsert({
                "ticker": ticker, "name": company_name[:255] if company_name else ticker, "sector": "Unknown", "industry": "Unknown"
            }).execute()
        except Exception: # Catch any exception during upsert
            pass

        transactions.append({
            "member_id": member_id,
            "politician_name": f"{first_name} {last_name}"[:100],
            "chamber": "House",
            "party": "Unknown",
            "ticker": ticker,
            "transaction_date": tx_date,
            "published_date": str(tx_year), # Placeholder, will be replaced by index date
            "transaction_type": tx_type,
            "asset_type": asset_type[:50] if asset_type else "Stock",
            "amount_range": amount_text[:255],
            "source_url": HOUSE_PTR_PDF_URL.format(year=tx_year, doc_id=doc_id),
            "doc_id": f"house-{tx_year}-{doc_id}-{trade_idx}"
        })
        
    return transactions


def fetch_house_trades():
    print("Starting Official House Clerk PTR Scraper...")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"})
    
    try:
        members_req = supabase.table("congress_members").select("id, first_name, last_name").execute()
        members_db = members_req.data if members_req else []
    except Exception as e:
        print(f"Warn: Could not fetch congress_members for mapping ({e})")
        members_db = []
        
    all_transactions = []
    
    daily_mode = os.environ.get("FINLENS_DAILY_MODE", "0") == "1"
    start_year = datetime.now().year
    end_year = (start_year - 1) if daily_mode else 2012
    
    for y in range(start_year, start_year - 1, -1):
        print(f"\n1. Fetching Bulk Index for {y}...")
        
        url = HOUSE_INDEX_URL.format(year=y)
        response = session.get(url, timeout=30)
        
        if response.status_code != 200:
             print(f"Skipping {y}, index not available.")
             continue
             
        payload = response.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
        
        bulk_filings = []
        for row in reader:
            if (row.get("FilingType") or "").strip().upper() != "P":
                continue
            doc_id = (row.get("DocID") or "").strip()
            if not doc_id:
                continue
            bulk_filings.append({
                "doc_id": doc_id,
                "first_name": (row.get("First") or "").strip(),
                "last_name": (row.get("Last") or "").strip(),
                "filing_date": (row.get("FilingDate") or "").strip(),
                "year": y
            })

        print(f"Found {len(bulk_filings)} House PTR filings in the {y} index. Processing ALL records for maximum historical depth.")
        
        # Process records from the index
        print(f"Index check: {len(bulk_filings)} records.")
        
        consecutive_existing = 0
        for filing in reversed(bulk_filings): # Start from most recent (highest DocID)
            doc_id = filing["doc_id"]
            fname = filing["first_name"]
            lname = filing["last_name"]
            year = filing["year"]
            idx_filing_date_raw = filing["filing_date"]

            check = supabase.table("politician_trades").select("id").eq("doc_id", f"house-{year}-{doc_id}-0").execute()
            if check.data:
                continue
            
            consecutive_existing = 0
            
            # Normalize index filing date 3/14/2026 -> 2026-03-14
            try:
                dt = datetime.strptime(idx_filing_date_raw, "%m/%d/%Y")
                idx_filing_date = dt.strftime("%Y-%m-%d")
            except:
                idx_filing_date = datetime.now().strftime("%Y-%m-%d")

            pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
            print(f"Fetching NEW PDF {doc_id} for {fname} {lname}...")
            
            try:
                 pdf_resp = session.get(pdf_url, timeout=15)
                 pdf_resp.raise_for_status()
                 
                 lines = extract_pdf_lines(pdf_resp.content)
                 txs = extract_transactions_from_lines(lines, doc_id, fname, lname, year, members_db)
                 if txs:
                     for t in txs:
                         t["published_date"] = idx_filing_date # Use the official Filing Date
                     all_transactions.extend(txs)
                     print(f" -> Extracted {len(txs)} trades")
            except Exception as e:
                 print(f" -> PDF Exception: {e}")
                 
    print(f"\nFinished extracting {len(all_transactions)} standard House trades.")
    
    if all_transactions:
        print(f"Uploading {len(all_transactions)} real House trades to Supabase...")
        for i in range(0, len(all_transactions), 50):
            chunk = all_transactions[i:i + 50]
            try:
                # Manual Upsert: filter out already existing doc_ids
                doc_ids = [t["doc_id"] for t in chunk]
                existing = supabase.table("politician_trades").select("doc_id").in_("doc_id", doc_ids).execute()
                existing_ids = {r["doc_id"] for r in existing.data}
                
                to_insert = [t for t in chunk if t["doc_id"] not in existing_ids]
                if to_insert:
                    supabase.table("politician_trades").insert(to_insert).execute()
                    print(f" -> Inserted {len(to_insert)} new House trades.")
            except Exception as e:
                print(f"Error manual-upserting chunk: {e}")
                
        print("Successfully seeded HOUSE trades!")

if __name__ == "__main__":
    fetch_house_trades()
