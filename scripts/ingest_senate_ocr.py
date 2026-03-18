import os
import io
import re
import time
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from pdf2image import convert_from_bytes
import pytesseract
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

from datetime import datetime
CURRENT_YEAR = datetime.now().year

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

SENATE_HOME = "https://efdsearch.senate.gov/search/home/"
SENATE_DATA = "https://efdsearch.senate.gov/search/report/data/"

# Regex for finding dates in OCR text
DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")

def norm_date(d):
    d = d.replace("o", "0").replace("O", "0").replace("i", "1").replace("l", "1")
    parts = re.findall(r"\d+", d)
    if len(parts) < 3: return None
    m, d_day, y = parts[0], parts[1], parts[2]
    if not (1 <= int(m) <= 12) or not (1 <= int(d_day) <= 31): return None
    if len(y) == 2: y = "20" + y
    return f"{y}-{m.zfill(2)}-{d_day.zfill(2)}"

def parse_senate_ocr(text, doc_id, member_id, pol_name, valid_tickers):
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    trades = []
    
    # Senate paper forms are often tables. We look for Ticker and Date.
    ticker_matches = []
    date_matches = []
    
    for i, line in enumerate(lines):
        # Look for tickers (1-5 uppercase)
        t_matches = re.findall(r"\b([A-Z]{1,5})\b", line)
        for t in t_matches:
            if t in valid_tickers and t not in (
                "USA", "THE", "AND", "FOR", "PTR", "FD", "OFF", "ST", "INC", "CORP", 
                "PURCH", "SALE", "STOCK", "PAGE", "PART", "NEW", "OLD", "LINE", "BANK",
                "TYPE", "IR", "RT", "AM", "PM", "TOTAL", "DATE", "VAL", "CO", "LLC"
            ):
                ticker_matches.append((t, i))
        
        # Look for dates
        d_matches = DATE_RE.findall(line)
        for d in d_matches:
            date_matches.append((d, i))

    for tic, t_idx in ticker_matches:
        nearby_dates = [d for d, d_idx in date_matches if abs(d_idx - t_idx) <= 15]
        if len(nearby_dates) >= 1:
            tx_date_raw = nearby_dates[0]
            n_tx = norm_date(tx_date_raw)
            if not n_tx: continue
            
            # Date safety check
            tx_year = int(n_tx.split("-")[0])
            if tx_year < 2012 or tx_year > CURRENT_YEAR:
                continue
            
            # published date is usually at the bottom or top of form, or near the signature
            # for now we use the same as tx_date or current if not found
            
            window = " ".join(lines[max(0, t_idx-3):min(len(lines), t_idx+4)]).lower()
            tx_type = "sell" if "sale" in window or "sell" in window or "sold" in window else "buy"
            
            trades.append({
                "member_id": member_id,
                "politician_name": pol_name,
                "chamber": "Senate",
                "ticker": tic,
                "transaction_date": n_tx,
                "published_date": n_tx, # Placeholder
                "transaction_type": tx_type,
                "asset_type": "Stock",
                "amount_range": "Unknown",
                "source_url": f"https://efdsearch.senate.gov/search/view/paper/{doc_id}/",
                "doc_id": f"senate-ocr-{doc_id}-{len(trades)}"
            })
            
    final_trades = []
    seen = set()
    for t in trades:
        k = f"{t['ticker']}-{t['transaction_date']}"
        if k not in seen:
            seen.add(k)
            final_trades.append(t)
    return final_trades

def run_senate_ocr(last_name):
    print(f"--- Senate OCR Recovery: {last_name} ---")
    session = requests.Session()
    resp = session.get(SENATE_HOME)
    soup = BeautifulSoup(resp.text, 'html.parser')
    csrf = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
    session.post(SENATE_HOME, data={'prohibition_agreement': '1', 'csrfmiddlewaretoken': csrf})

    payload = {
        'start': '0', 'length': '100', 'report_types': '[11]', 
        'filer_types': '[]', 'submitted_start_date': '01/01/2020 00:00:00',
        'last_name': last_name, 'csrfmiddlewaretoken': csrf
    }
    res = session.post(SENATE_DATA, data=payload, headers={'Referer': 'https://efdsearch.senate.gov/search/'})
    data = res.json()
    rows = data.get('data', [])
    
    # Get member ID
    mem_res = supabase.table("congress_members").select("id, first_name, last_name").ilike("last_name", f"%{last_name}%").execute()
    if not mem_res.data:
        print("Member not found in DB")
        return
    member_id = mem_res.data[0]['id']
    full_name = f"{mem_res.data[0]['first_name']} {mem_res.data[0]['last_name']}"

    valid_tickers = set()
    offset = 0
    while True:
        comp_res = supabase.table("companies").select("ticker").neq("name", "Unknown (OCR)").range(offset, offset + 999).execute()
        if not comp_res.data: break
        valid_tickers.update(c['ticker'] for c in comp_res.data)
        if len(comp_res.data) < 1000: break
        offset += 1000
    valid_tickers |= {"US-TREAS", "N/A"}

    for r in rows:
        detail_link = BeautifulSoup(r[3], 'html.parser').find('a')['href']
        if '/paper/' not in detail_link: continue
        
        doc_id = detail_link.split('/')[-2]
        print(f" [!] Processing Paper doc_id: {doc_id}")
        
        detail_page = session.get(f"https://efdsearch.senate.gov{detail_link}")
        # The PDF is actually linked in the page directly
        soup_detail = BeautifulSoup(detail_page.text, 'html.parser')
        pdf_a = soup_detail.find('a', href=lambda h: h and h.endswith('.pdf'))
        if not pdf_a:
            print("  No PDF link found on paper detail page.")
            continue
            
        pdf_url = f"https://efdsearch.senate.gov{pdf_a['href']}"
        pdf_resp = session.get(pdf_url)
        
        images = convert_from_bytes(pdf_resp.content, dpi=200)
        full_ocr = ""
        for img in images: full_ocr += pytesseract.image_to_string(img) + "\n"
        
        trades = parse_senate_ocr(full_ocr, doc_id, member_id, full_name, valid_tickers)
        if trades:
             print(f"  [+] Recovered {len(trades)} trades")
             try:
                 supabase.table("politician_trades").upsert(trades, on_conflict="doc_id").execute()
             except Exception as e:
                 print(f"  [Error] {e}")

if __name__ == "__main__":
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else "Blumenthal"
    run_senate_ocr(name)
