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
    "User-Agent": "vtdfor@gmail.com"
}

SEC_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=only&start=0&count=100&output=atom"

def fetch_sec_form4():
    print("Starting Official SEC EDGAR Insider Trades Scraper...")
    
    response = requests.get(SEC_RSS_URL, headers=SEC_HEADERS, timeout=15)
    if response.status_code != 200:
        print(f"Failed to fetch SEC RSS. Status: {response.status_code}")
        return
        
    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(response.content)
    
    entries = root.findall("atom:entry", namespace)
    print(f"Found {len(entries)} recent Form 4 filings on EDGAR.")
    
    formatted_trades = []
    
    for entry in entries[:50]:
        title = entry.find("atom:title", namespace).text
        # Title format: "4 - COMPANY NAME (CIK) (Reporting)"
        if not title.startswith("4 - "):
            continue
            
        link = entry.find("atom:link", namespace).get("href") # e.g. /Archives/edgar/data/CIK/ACCESSION-index.htm
        
        # We need the raw XML which is the accession number but removing the hyphens
        # e.g. /Archives/edgar/data/123456/000123456-24-000001-index.htm -> /Archives/edgar/data/123456/00012345624000001/primary_doc.xml
        parts = link.split('/')
        accession = parts[-1].replace("-index.htm", "")
        accession_no_dash = accession.replace("-", "")
        cik = parts[-3]
        # Direct link to the accession text compilation which contains the raw XML embedded
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dash}/{accession}.txt"
        
        try:
            form4_resp = requests.get(index_url, headers=SEC_HEADERS, timeout=10)
            if form4_resp.status_code != 200:
                 time.sleep(0.2)
                 continue
                 
            # Extract only the XML portion from the larger SEC submission text wrapper
            content = form4_resp.content.decode("utf-8", errors="replace")
            xml_start = content.find("<XML>")
            xml_end = content.find("</XML>")
            
            if xml_start == -1 or xml_end == -1:
                continue
                
            xml_content = content[xml_start + 5:xml_end].strip()
            # Strip ALL namespaces to make findall easy across nested nodes
            xml_content_clean = re.sub(r'\sxmlns="[^"]+"', '', xml_content)
            form4_root = ET.fromstring(xml_content_clean)
            
            # Extract data from the XML schema
            issuer_node = form4_root.find(".//issuer")
            if issuer_node is None:
                 continue
                 
            ticker = issuer_node.find("issuerTradingSymbol").text if issuer_node.find("issuerTradingSymbol") is not None else ""
            company_name = issuer_node.find("issuerName").text if issuer_node.find("issuerName") is not None else ticker
            
            if not ticker:
                 continue
                 
            # Upsert Company
            company_upsert = None
            try:
                 company_upsert = supabase.table("companies").upsert({
                     "ticker": ticker[:10],
                     "name": company_name[:255] if company_name else ticker[:10],
                     "sector": "Unknown",
                     "industry": "Unknown"
                 }).execute()
            except:
                 pass
                 
            if not company_upsert or not company_upsert.data:
                continue

            # Reporting Owner
            owner_node = form4_root.find(".//reportingOwner/reportingOwnerId/rptOwnerName")
            insider_name = owner_node.text if owner_node is not None else "Unknown"
            
            # Role
            role_node = form4_root.find(".//reportingOwner/reportingOwnerRelationship")
            if role_node is not None:
                if role_node.find("isDirector") is not None and role_node.find("isDirector").text in ["1", "true"]:
                    role = "Director"
                elif role_node.find("isOfficer") is not None and role_node.find("isOfficer").text in ["1", "true"]:
                    role = role_node.find("officerTitle").text if role_node.find("officerTitle") is not None else "Officer"
                elif role_node.find("isTenPercentOwner") is not None and role_node.find("isTenPercentOwner").text in ["1", "true"]:
                    role = "10% Owner"
                else:
                    role = "Insider"
            else:
                role = "Insider"

            # Transactions
            for nonDerivTx in form4_root.findall(".//nonDerivativeTransaction"):
                security_title = nonDerivTx.find(".//securityTitle/value")
                tx_date = nonDerivTx.find(".//transactionDate/value")
                tx_amounts = nonDerivTx.find(".//transactionAmounts")
                
                if security_title is None or tx_date is None or tx_amounts is None:
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
                except ValueError:
                    shares_val = 0
                    price_val = 0.0
                
                formatted_trades.append({
                    "filer_name": insider_name[:100],
                    "filer_relation": role[:50],
                    "ticker": ticker[:10],
                    "transaction_date": tx_date.text,
                    "published_date": tx_date.text,
                    "transaction_code": direction,
                    "amount": shares_val,
                    "price": price_val,
                    "value": int(shares_val * price_val),
                    "source_url": index_url[:500]
                })

            time.sleep(0.12) # Strict SEC Rate Limit: 10 requests per second
            
        except Exception as e:
            print(f"Error parsing {link}: {e}")
            
    print(f"Finished extracting {len(formatted_trades)} SEC Insider Trades.")
    
    if formatted_trades:
        print(f"Uploading {len(formatted_trades)} real Insider trades to Supabase...")
        # Clear out the mock trades first to maintain data integrity
        try:
            print("Clearing old mock Form 4 data...")
            supabase.table("insider_trades").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        except:
            pass

        for i in range(0, len(formatted_trades), 50):
            chunk = formatted_trades[i:i + 50]
            try:
                supabase.table("insider_trades").insert(chunk).execute()
            except Exception as e:
                print(f"Error inserting chunk: {e}")
                
        print("Successfully seeded INSIDER TRADES!")

if __name__ == "__main__":
    fetch_sec_form4()
