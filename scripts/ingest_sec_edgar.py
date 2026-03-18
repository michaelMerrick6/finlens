import os
import time
import re
from xml.etree import ElementTree as ET
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

# SEC requires a descriptive User-Agent
SEC_HEADERS = {
    "User-Agent": "FinLens/1.0 vtdfor@gmail.com"
}

SEC_RSS_BASE = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=only&count=100&output=atom"


def parse_form4_xml(index_url, sec_session):
    """Parse a single Form 4 filing XML and return trade rows."""
    trades = []
    try:
        form4_resp = sec_session.get(index_url, timeout=10)
        if form4_resp.status_code != 200:
            return trades

        content = form4_resp.content.decode("utf-8", errors="replace")
        xml_start = content.find("<XML>")
        xml_end = content.find("</XML>")

        if xml_start == -1 or xml_end == -1:
            return trades

        xml_content = content[xml_start + 5:xml_end].strip()
        xml_content_clean = re.sub(r'\sxmlns="[^"]+"', '', xml_content)
        form4_root = ET.fromstring(xml_content_clean)

        # Issuer info
        issuer_node = form4_root.find(".//issuer")
        if issuer_node is None:
            return trades

        ticker = (issuer_node.find("issuerTradingSymbol").text or "").strip().upper() if issuer_node.find("issuerTradingSymbol") is not None else ""
        company_name = issuer_node.find("issuerName").text if issuer_node.find("issuerName") is not None else ticker

        if not ticker or not re.match(r'^[A-Z]{1,10}$', ticker):
            return trades

        # Reporting Owner
        owner_node = form4_root.find(".//reportingOwner/reportingOwnerId/rptOwnerName")
        insider_name = owner_node.text.strip() if owner_node is not None and owner_node.text else "Unknown"

        # Role
        role_node = form4_root.find(".//reportingOwner/reportingOwnerRelationship")
        if role_node is not None:
            if role_node.find("isDirector") is not None and role_node.find("isDirector").text in ["1", "true"]:
                role = "Director"
            elif role_node.find("isOfficer") is not None and role_node.find("isOfficer").text in ["1", "true"]:
                role = (role_node.find("officerTitle").text or "Officer").strip() if role_node.find("officerTitle") is not None else "Officer"
            elif role_node.find("isTenPercentOwner") is not None and role_node.find("isTenPercentOwner").text in ["1", "true"]:
                role = "10% Owner"
            else:
                role = "Insider"
        else:
            role = "Insider"

        # Filing date (periodOfReport)
        period_node = form4_root.find(".//periodOfReport")
        filing_date = period_node.text.strip() if period_node is not None and period_node.text else None

        # Non-derivative transactions
        tx_idx = 0
        for nonDerivTx in form4_root.findall(".//nonDerivativeTransaction"):
            tx_date_node = nonDerivTx.find(".//transactionDate/value")
            tx_amounts = nonDerivTx.find(".//transactionAmounts")

            if tx_date_node is None or tx_amounts is None:
                continue

            shares_node = tx_amounts.find(".//transactionShares/value")
            price_node = tx_amounts.find(".//transactionPricePerShare/value")
            a_or_d_node = tx_amounts.find(".//transactionAcquiredDisposedCode/value")

            shares = shares_node.text if shares_node is not None and shares_node.text else "0"
            price = price_node.text if price_node is not None and price_node.text else "0.0"
            a_or_d = a_or_d_node.text if a_or_d_node is not None and a_or_d_node.text else "A"

            direction = "buy" if a_or_d == "A" else "sell"

            try:
                shares_val = int(float(shares))
                price_val = float(price)
            except (ValueError, TypeError):
                shares_val = 0
                price_val = 0.0

            tx_date = tx_date_node.text.strip()[:10]  # Strip timezone suffix like -05:00

            # Build a unique doc_id from accession + trade index
            accession = index_url.split("/")[-1].replace(".txt", "")
            doc_id = f"sec4-{accession}-{tx_idx}"

            trades.append({
                "filer_name": insider_name[:100],
                "filer_relation": role[:50],
                "ticker": ticker[:10],
                "transaction_date": tx_date,
                "published_date": filing_date or tx_date,
                "transaction_code": direction,
                "amount": shares_val,
                "price": round(price_val, 4),
                "value": int(shares_val * price_val),
                "source_url": index_url[:500],
                "_doc_id": doc_id,
                "_company_name": company_name,
            })
            tx_idx += 1

    except Exception as e:
        print(f"  Error parsing {index_url}: {e}")

    return trades


def fetch_sec_form4():
    print("Starting Official SEC EDGAR Insider Trades Scraper...")

    sec_session = requests.Session()
    sec_session.headers.update(SEC_HEADERS)

    # Fetch existing doc_ids for dedup
    print("Loading existing doc_ids for dedup...")
    existing_urls = set()
    offset = 0
    while True:
        r = supabase.table("insider_trades").select("source_url,filer_name,ticker,transaction_date,amount").range(offset, offset + 999).execute()
        if not r.data:
            break
        for row in r.data:
            key = f"{row['source_url']}|{row['filer_name']}|{row['ticker']}|{row['transaction_date']}|{row['amount']}"
            existing_urls.add(key)
        offset += len(r.data)
        if len(r.data) < 1000:
            break
    print(f"  Existing trades in DB: {len(existing_urls)}")

    all_trades = []

    # Paginate through RSS feed (10 pages × 100 = up to 1000 filings)
    pages = 5 if os.environ.get("FINLENS_DAILY_MODE") else 10
    for page in range(pages):
        start = page * 100
        rss_url = f"{SEC_RSS_BASE}&start={start}"
        print(f"Fetching RSS page {page + 1}/{pages} (start={start})...")

        try:
            response = sec_session.get(rss_url, timeout=15)
            if response.status_code != 200:
                print(f"  Failed: HTTP {response.status_code}")
                break
        except Exception as e:
            print(f"  Failed: {e}")
            break

        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        root = ET.fromstring(response.content)
        entries = root.findall("atom:entry", namespace)

        if not entries:
            print(f"  No more entries, stopping pagination.")
            break

        print(f"  Found {len(entries)} filings on page {page + 1}")

        for entry in entries:
            title_node = entry.find("atom:title", namespace)
            title = title_node.text if title_node is not None else ""
            if not title.startswith("4 - "):
                continue

            link_node = entry.find("atom:link", namespace)
            link = link_node.get("href") if link_node is not None else ""
            if not link:
                continue

            parts = link.split('/')
            accession = parts[-1].replace("-index.htm", "")
            accession_no_dash = accession.replace("-", "")
            cik = parts[-3]
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dash}/{accession}.txt"

            trades = parse_form4_xml(index_url, sec_session)
            all_trades.extend(trades)

            time.sleep(0.12)  # SEC rate limit: 10 req/sec

    print(f"\nExtracted {len(all_trades)} total insider trades from RSS.")

    # Dedup against existing DB
    new_trades = []
    for t in all_trades:
        key = f"{t['source_url']}|{t['filer_name']}|{t['ticker']}|{t['transaction_date']}|{t['amount']}"
        if key not in existing_urls:
            existing_urls.add(key)
            new_trades.append(t)

    print(f"New trades after dedup: {len(new_trades)}")

    if new_trades:
        # Upsert companies
        companies_done = set()
        for t in new_trades:
            ticker = t["ticker"]
            if ticker not in companies_done:
                try:
                    supabase.table("companies").upsert({
                        "ticker": ticker,
                        "name": t.pop("_company_name", ticker)[:255],
                        "sector": "Unknown",
                        "industry": "Unknown"
                    }).execute()
                    companies_done.add(ticker)
                except:
                    t.pop("_company_name", None)
            else:
                t.pop("_company_name", None)
            t.pop("_doc_id", None)

        # Insert in batches
        inserted = 0
        for i in range(0, len(new_trades), 50):
            chunk = new_trades[i:i + 50]
            try:
                supabase.table("insider_trades").insert(chunk).execute()
                inserted += len(chunk)
            except Exception as e:
                # Try one-by-one for failed chunks
                for trade in chunk:
                    try:
                        supabase.table("insider_trades").insert(trade).execute()
                        inserted += 1
                    except Exception as e2:
                        print(f"  Skip: {trade['filer_name']} {trade['ticker']} - {e2}")

        print(f"Inserted {inserted} new insider trades!")
    else:
        # Clean up internal fields
        for t in all_trades:
            t.pop("_company_name", None)
            t.pop("_doc_id", None)
        print("No new trades to insert — all already in DB.")

    # Final count
    r = supabase.table("insider_trades").select("id", count="exact").execute()
    print(f"Total insider trades in DB: {r.count}")


if __name__ == "__main__":
    fetch_sec_form4()
