import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import json
from supabase import create_client
from dotenv import load_dotenv
import sys
from requests.exceptions import RequestException

# Load environment variables
load_dotenv(".env.local")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("Error: Missing Supabase credentials in environment.")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
SEC_HEADERS = {"User-Agent": "FinLens/1.0 vtdfor@gmail.com"}
RSS_TIMEOUT_SECONDS = int(os.environ.get("SEC_RSS_TIMEOUT_SECONDS", "20"))
DOC_TIMEOUT_SECONDS = int(os.environ.get("SEC_DOC_TIMEOUT_SECONDS", "10"))
REQUEST_RETRIES = int(os.environ.get("SEC_REQUEST_RETRIES", "3"))
REQUEST_RETRY_SLEEP_SECONDS = float(os.environ.get("SEC_REQUEST_RETRY_SLEEP_SECONDS", "1.5"))

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def fetch_with_retry(sec_session, url, *, timeout, label):
    last_error = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = sec_session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except RequestException as exc:
            last_error = exc
            log(f"{label} attempt {attempt}/{REQUEST_RETRIES} failed: {exc}")
            if attempt < REQUEST_RETRIES:
                time.sleep(REQUEST_RETRY_SLEEP_SECONDS * attempt)
    raise last_error or RuntimeError(f"{label} failed without an exception")

def get_most_recent_url():
    """Retrieve the URL of the most recently ingested Form 4 trade to use as a stopping condition."""
    try:
        res = sb.table('insider_trades').select('source_url').order('created_at', desc=True).limit(1).execute()
        if res.data and len(res.data) > 0:
            return res.data[0]['source_url']
    except Exception as e:
        log(f"Warning: Failed to fetch most recent URL from Supabase: {e}")
    return None

def parse_xml_robust(index_url, sec_session):
    r = fetch_with_retry(sec_session, index_url, timeout=DOC_TIMEOUT_SECONDS, label=f"Form 4 document fetch {index_url}")

    text = r.text
    try:
        xml_start = text.index("<XML>") + 5
        xml_end = text.index("</XML>")
        xml_body = text[xml_start:xml_end].strip()
    except ValueError:
        return []

    root = ET.fromstring(xml_body)
    issuer = root.find(".//issuer")
    if issuer is None:
        return []
    sym_node = issuer.find(".//issuerTradingSymbol")
    ticker = sym_node.text.strip().upper() if sym_node is not None and sym_node.text else "UNKNOWN"
    if ticker in ["NONE", "UNKNOWN", ""]:
        return []

    owner = root.find(".//reportingOwner")
    name_node = owner.find(".//rptOwnerName")
    insider = name_node.text.strip().title() if name_node is not None and name_node.text else "Unknown"

    role_node = owner.find(".//officerTitle")
    if role_node is None:
        role_node = owner.find(".//reportingOwnerRelationship/otherText")
    role = role_node.text.strip() if role_node is not None and role_node.text else "Insider"

    period_node = root.find(".//periodOfReport")
    pub_date = period_node.text.strip() if period_node is not None and period_node.text else None

    trades = []
    for tx in root.findall(".//nonDerivativeTransaction"):
        tc_node = tx.find(".//transactionCoding/transactionCode")
        tc = tc_node.text.strip() if tc_node is not None and tc_node.text else ""
        if tc not in ["P", "S"]:
            continue

        date_node = tx.find(".//transactionDate/value")
        tx_date = date_node.text.strip()[:10] if date_node is not None and date_node.text else ""

        shares_node = tx.find(".//transactionAmounts/transactionShares/value")
        price_node = tx.find(".//transactionAmounts/transactionPricePerShare/value")

        shares = float(shares_node.text) if shares_node is not None and shares_node.text else 0.0
        price = float(price_node.text) if price_node is not None and price_node.text else 0.0

        if shares > 0:
            trades.append({
                "filer_name": insider[:100], "filer_relation": role[:100],
                "ticker": ticker[:10], "transaction_date": tx_date,
                "published_date": pub_date or tx_date,
                "transaction_code": "buy" if tc == "P" else "sell",
                "amount": int(shares), "price": round(price, 4),
                "value": int(shares * price), "source_url": index_url[:500]
            })
    return trades

def main():
    log("Starting Daily SEC EDGAR Form 4 Scraper...")
    recent_url = get_most_recent_url()
    log(f"Most recent trade URL in DB: {recent_url}")
    
    session = requests.Session()
    session.headers.update(SEC_HEADERS)
    
    new_trades = []
    inserted_count = 0
    stop_scraping = False
    page_fetch_errors = 0
    document_fetch_errors = 0
    first_page_failed = False
    
    # Check current day and previous 2 days (in case we run over a weekend)
    d_str = datetime.now().strftime("%Y%m%d")
    
    # Limit to 10 pages max (1,000 filings) to prevent runaway execution in GitHub Actions
    for page in range(10):
        if stop_scraping:
            break
            
        start = page * 100
        # Use datea= and dateb= dynamically or omit to just get latest overall
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&owner=only&start={start}&count=100&output=atom"
        
        try:
            r = fetch_with_retry(session, url, timeout=RSS_TIMEOUT_SECONDS, label=f"SEC RSS page {page}")
                
            root = ET.fromstring(r.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            
            if not entries:
                log(f"No more entries found on page {page}.")
                break
            
            log(f"Scanning page {page+1} ({len(entries)} filings latest)...")
            
            for entry in entries:
                link = entry.find("atom:link", ns)
                if link is None: continue
                href = link.get("href")
                
                parts = href.split("/")
                acc = parts[-1].replace("-index.htm", "")
                acc_cl = acc.replace("-", "")
                cik = parts[-3]
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_cl}/{acc}.txt"
                
                # STOP CONDITION: We hit a document we've already ingested
                if recent_url and doc_url == recent_url:
                    log("Reached already ingested document. Stopping pagination.")
                    stop_scraping = True
                    break
                
                try:
                    parsed = parse_xml_robust(doc_url, session)
                    if parsed:
                        new_trades.extend(parsed)
                except Exception as exc:
                    document_fetch_errors += 1
                    log(f"Failed to parse Form 4 document {doc_url}: {exc}")
                
                # Critical SEC rate limit delay (max 10 req/s)
                time.sleep(0.12)
                
        except Exception as e:
            page_fetch_errors += 1
            if page == 0:
                first_page_failed = True
            log(f"Error parsing page {page}: {e}")
            break

    if first_page_failed:
        print(
            "SUMMARY_JSON:"
            + json.dumps(
                {
                    "records_seen": len(new_trades),
                    "records_inserted": inserted_count,
                    "records_skipped": max(len(new_trades) - inserted_count, 0),
                    "companies_upserted": len({t["ticker"] for t in new_trades}),
                    "page_fetch_errors": page_fetch_errors,
                    "document_fetch_errors": document_fetch_errors,
                    "fatal_error": True,
                },
                sort_keys=True,
            )
        )
        log("SEC daily scraper failed before it could read the first page of filings.")
        sys.exit(1)
            
    if new_trades:
        log(f"Found {len(new_trades)} NEW trades. Uploading to Supabase...")
        comps = list({t["ticker"] for t in new_trades})
        
        # Upsert companies
        sb.table("companies").upsert(
            [{"ticker": t, "name": t, "sector": "Unknown", "industry": "Unknown"} for t in comps], 
            on_conflict="ticker"
        ).execute()
        
        # Insert trades in chunks
        for i in range(0, len(new_trades), 500):
            try:
                chunk = new_trades[i:i+500]
                sb.table("insider_trades").insert(chunk).execute()
                inserted_count += len(chunk)
            except Exception as e:
                log(f"Failed to insert chunk: {e}")
                
        log(f"Successfully uploaded {len(new_trades)} fresh trades!")
    else:
        log("No completely new trades found since last run.")
        
    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "records_seen": len(new_trades),
                "records_inserted": inserted_count,
                "records_skipped": max(len(new_trades) - inserted_count, 0),
                "companies_upserted": len({t["ticker"] for t in new_trades}),
                "page_fetch_errors": page_fetch_errors,
                "document_fetch_errors": document_fetch_errors,
            },
            sort_keys=True,
        )
    )
    log("Daily SEC Scraper Complete.")

if __name__ == "__main__":
    main()
