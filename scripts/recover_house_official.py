import os
import io
import csv
import re
import time
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

# This regex is much more aggressive - it looks for the patterns but is less strict about exact line breaks
TRADE_BLOB_RE = re.compile(
    r"(?P<asset_name>.+?)\s+\[(?P<asset_type>[A-Z]{2})\]\s+(?P<type>[PSE])\s+(?P<tx_date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<notif_date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<amount>\$[0-9,]+\s*-\s*\$[0-9,]+|\$[0-9,]+)",
    re.DOTALL
)

TICKER_RE = re.compile(r"\((?P<ticker>[A-Z]{1,6})\)")

def extract_pdf_content(pdf_bytes: bytes):
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages:
            t = page.extract_text() or ""
            pages_text.append(t.replace('\x00', ''))
        return "\n".join(pages_text)
    except Exception as e:
        return None

def parse_robust_trades(text, doc_id, fname, lname, year, member_id):
    trades = []
    # Clean up common PDF artifacts that break regex
    text = text.replace('ID OwnerAsset Transaction TypeDate Notification Amount Cap. Gains > $200?', '')
    
    matches = list(TRADE_BLOB_RE.finditer(text))
    for i, m in enumerate(matches):
        asset_raw = m.group('asset_name').strip()
        asset_type = m.group('asset_type')
        tx_code = m.group('type')
        tx_date_str = m.group('tx_date')
        pub_date_str = m.group('notif_date')
        amount_str = m.group('amount')
        
        # Extract ticker from asset_raw
        ticker_match = TICKER_RE.search(asset_raw)
        ticker = ticker_match.group('ticker') if ticker_match else None
        
        if not ticker:
            continue
            
        try:
            tx_date = datetime.strptime(tx_date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
            pub_date = datetime.strptime(pub_date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
        except:
            continue
            
        tx_type = {"P": "buy", "S": "sell", "E": "exchange"}.get(tx_code, "unknown")
        
        trades.append({
            "member_id": member_id,
            "politician_name": f"{fname} {lname}",
            "chamber": "House",
            "ticker": ticker,
            "transaction_date": tx_date,
            "published_date": pub_date,
            "transaction_type": tx_type,
            "asset_type": asset_type,
            "amount_range": amount_str,
            "source_url": HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id),
            "doc_id": f"house-robust-{year}-{doc_id}-{i}"
        })
    return trades

def recover_house_trades(year):
    print(f"--- House Recovery Mission: {year} ---")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    
    # 1. Fetch ALL existing trades for this year to dedup in memory
    res = supabase.table("politician_trades").select("politician_name, ticker, transaction_date, amount_range").eq("chamber", "House").gte("transaction_date", f"{year}-01-01").lte("transaction_date", f"{year}-12-31").execute()
    existing_keys = {f"{row['politician_name']}|{row['ticker']}|{row['transaction_date']}|{row['amount_range']}".lower() for row in res.data}
    print(f"  Loaded {len(existing_keys)} existing trades for {year}")
    
    # 2. Fetch index
    r = session.get(HOUSE_INDEX_URL.format(year=year))
    if r.status_code != 200: return
    
    payload = r.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    
    total_found = 0
    scanned_count = 0
    
    # Pre-resolve members
    members_res = supabase.table("congress_members").select("id, first_name, last_name").execute()
    members_map = {f"{m['first_name']} {m['last_name']}".lower(): m['id'] for m in members_res.data}

    for row in reader:
        if row.get("FilingType") != "P": continue
        doc_id = row.get("DocID")
        fname = row.get("First")
        lname = row.get("Last")
        
        full_name = f"{fname} {lname}".lower()
        member_id = members_map.get(full_name, f"unknown-{fname.lower()}-{lname.lower()}")
        
        pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
        try:
            pdf_resp = session.get(pdf_url, timeout=10)
            if pdf_resp.status_code != 200: continue
            
            content = extract_pdf_content(pdf_resp.content)
            if not content or len(content.strip()) < 100:
                scanned_count += 1
                continue
                
            trades = parse_robust_trades(content, doc_id, fname, lname, year, member_id)
            new_trades = []
            for t in trades:
                key = f"{t['politician_name']}|{t['ticker']}|{t['transaction_date']}|{t['amount_range']}".lower()
                if key not in existing_keys:
                    existing_keys.add(key)
                    new_trades.append(t)
            
            if new_trades:
                # Upsert companies
                tickers = {t['ticker'] for t in new_trades}
                for tk in tickers:
                    try: supabase.table("companies").upsert({"ticker": tk, "name": tk}).execute()
                    except: pass
                
                # Insert trades
                supabase.table("politician_trades").upsert(new_trades, on_conflict="doc_id").execute()
                total_found += len(new_trades)
                print(f" [+] Recovered {len(new_trades)} NEW trades from {fname} {lname} ({doc_id})")
        except Exception as e:
            pass

    print(f"Summary {year}: Recovered {total_found} NEW trades. Skipped {scanned_count} scanned PDFs.")

if __name__ == "__main__":
    import sys
    year_to_run = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    recover_house_trades(year_to_run)
