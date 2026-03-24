import os
import io
import csv
import re
import time
from datetime import datetime
import requests
from pypdf import PdfReader
from pdf2image import convert_from_bytes
import pytesseract
from supabase import create_client, Client
from dotenv import load_dotenv
from legacy_congress_guard import require_legacy_write_opt_in

load_dotenv(dotenv_path=".env.local")
require_legacy_write_opt_in("ingest_house_ocr.py")

CURRENT_YEAR = datetime.now().year

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

HOUSE_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.txt"
HOUSE_PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

# Regex for finding dates in OCR text
DATE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")

def resolve_ticker(company_name):
    """Attempt to find a ticker for a company name."""
    if not company_name or len(company_name) < 3:
        return None
    try:
        res = supabase.table("companies").select("ticker").ilike("name", f"%{company_name}%").limit(1).execute()
        if res.data:
            return res.data[0]["ticker"]
    except:
        pass
    return None

def parse_ocr_text_to_trades(text, doc_id, member_id, pol_name, year, valid_tickers):
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    trades = []
    clean_lines = []
    for l in lines:
        if "LEGISLATIVE RESOURCE" in l or "FOR OFFICIAL USE" in l.upper(): continue
        clean_lines.append(l)

    ticker_matches = []
    date_matches = []
    
    for i, line in enumerate(clean_lines):
        t_matches = re.findall(r"\b([A-Z]{1,5})\b", line)
        for t in t_matches:
            if t in valid_tickers and t not in (
                "USA", "THE", "AND", "FOR", "PTR", "FD", "OFF", "ST", "INC", "CORP", 
                "PURCH", "SALE", "STOCK", "PAGE", "PART", "NEW", "OLD", "LINE", "BANK",
                "TYPE", "IR", "RT", "AM", "PM", "TOTAL", "DATE", "VAL", "CO", "LLC"
            ):
                ticker_matches.append((t, i))
        
        d_matches = DATE_RE.findall(line)
        for d in d_matches:
            date_matches.append((d, i))

    for tic, t_idx in ticker_matches:
        nearby_dates = [d for d, d_idx in date_matches if abs(d_idx - t_idx) <= 15]
        if len(nearby_dates) >= 1:
            tx_date = nearby_dates[0]
            pub_date = nearby_dates[1] if len(nearby_dates) > 1 else tx_date
            window = " ".join(clean_lines[max(0, t_idx-2):min(len(clean_lines), t_idx+3)]).lower()
            tx_type = "sell" if "sale" in window or "sell" in window else "buy"
            
            try:
                def norm_date(d):
                    d = d.replace("o", "0").replace("O", "0").replace("i", "1").replace("l", "1")
                    parts = re.findall(r"\d+", d)
                    if len(parts) < 3: return None
                    m, d_day, y = parts[0], parts[1], parts[2]
                    if not (1 <= int(m) <= 12) or not (1 <= int(d_day) <= 31): return None
                    if len(y) == 2: y = "20" + y
                    return f"{y}-{m.zfill(2)}-{d_day.zfill(2)}"
                
                n_tx = norm_date(tx_date)
                n_pub = norm_date(pub_date)
                if not n_tx: continue
                
                # Date safety check
                tx_year = int(n_tx.split("-")[0])
                if tx_year < 2012 or tx_year > CURRENT_YEAR:
                    continue
                
                trades.append({
                    "member_id": member_id,
                    "politician_name": pol_name,
                    "chamber": "House",
                    "ticker": tic,
                    "transaction_date": n_tx,
                    "published_date": n_pub,
                    "transaction_type": tx_type,
                    "asset_type": "Stock",
                    "amount_range": "$1,001 - $15,000",
                    "source_url": HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id),
                    "doc_id": f"house-ocr-{doc_id}-{len(trades)}"
                })
            except: continue

    final_trades = []
    seen = set()
    for t in trades:
        k = f"{t['ticker']}-{t['transaction_date']}"
        if k not in seen:
            seen.add(k)
            final_trades.append(t)
    return final_trades

def recover_scanned_house(year):
    print(f"--- House Scanned Recovery: {year} ---")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    
    r = session.get(HOUSE_INDEX_URL.format(year=year))
    if r.status_code != 200: return
    
    payload = r.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(payload), delimiter="\t")
    
    members_res = supabase.table("congress_members").select("id, first_name, last_name").execute()
    members_map = {f"{m['first_name']} {m['last_name']}".lower(): m['id'] for m in members_res.data}

    recovered_total = 0
    valid_tickers = set()
    offset = 0
    while True:
        comp_res = supabase.table("companies").select("ticker").neq("name", "Unknown (OCR)").range(offset, offset + 999).execute()
        if not comp_res.data: break
        valid_tickers.update(c['ticker'] for c in comp_res.data)
        if len(comp_res.data) < 1000: break
        offset += 1000
    valid_tickers |= {"US-TREAS", "N/A"}

    for row in reader:
        if row.get("FilingType") != "P": continue
        doc_id = row.get("DocID")
        fname = row.get("First")
        lname = row.get("Last")
        full_name = f"{fname} {lname}"
        
        # Resolve member
        fn_lower = full_name.lower()
        if fn_lower in members_map:
            member_id = members_map[fn_lower]
        else:
            member_id = f"unknown-{fname.lower().replace(' ','')}-{lname.lower().replace(' ','')}"[:50]
            try:
                supabase.table("congress_members").upsert({
                    "id": member_id, "first_name": fname, "last_name": lname, "chamber": "House"
                }).execute()
                members_map[fn_lower] = member_id
            except: pass

        # Check if already processed
        check = supabase.table("politician_trades").select("id").eq("doc_id", f"house-ocr-{doc_id}-0").execute()
        if check.data:
            continue

        pdf_url = HOUSE_PTR_PDF_URL.format(year=year, doc_id=doc_id)
        try:
            pdf_resp = session.get(pdf_url, timeout=15)
            if pdf_resp.status_code != 200: continue
            
            # Scanned check
            reader_pdf = PdfReader(io.BytesIO(pdf_resp.content))
            text = ""
            for p in reader_pdf.pages: text += p.extract_text() or ""
            
            if len(text.strip()) < 150:
                print(f" [!] Processing Scan: {full_name} ({doc_id})")
                images = convert_from_bytes(pdf_resp.content, dpi=300)
                full_ocr = ""
                for img in images: full_ocr += pytesseract.image_to_string(img) + "\n"
                
                trades = parse_ocr_text_to_trades(full_ocr, doc_id, member_id, full_name, year, valid_tickers)
                if trades:
                    try:
                        supabase.table("politician_trades").insert(trades).execute()
                        recovered_total += len(trades)
                        print(f"  [+] Recovered {len(trades)} trades")
                    except Exception as ins_e:
                        print(f"  [Insert Error] {doc_id}: {ins_e}")
        except Exception as e:
            print(f"  [Error] {doc_id}: {e}")

    print(f"Summary {year}: Added {recovered_total} OCR trades.")

if __name__ == "__main__":
    import sys
    year = sys.argv[1] if len(sys.argv) > 1 else "2025"
    recover_scanned_house(year)
