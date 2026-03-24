import os
import time
import requests
import re
import json
from xml.etree import ElementTree as ET
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

SEC_HEADERS = {
    "User-Agent": "vtdfor@gmail.com"
}

# The Top Hedge Funds / Institutions we want to track
FUNDS = [
    {"cik": "0001067983", "name": "Berkshire Hathaway Inc"},
    {"cik": "0001336528", "name": "Pershing Square Capital Management, L.P."},
    {"cik": "0001649339", "name": "Scion Asset Management, LLC"},
    {"cik": "0001423053", "name": "Citadel Advisors LLC"}
]

def fetch_latest_13f(fund):
    cik = fund["cik"]
    cik_padded = f"CIK{cik.zfill(10)}"
    api_url = f"https://data.sec.gov/submissions/{cik_padded}.json"
    print(f"Fetching SEC Submissions for {fund['name']} ({cik})...")
    
    resp = requests.get(api_url, headers=SEC_HEADERS, timeout=10)
    if resp.status_code != 200:
        print(f"Error fetching {api_url}: {resp.status_code}")
        return None
        
    data = resp.json()
    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    acc_nums = filings.get("accessionNumber", [])
    dates = filings.get("filingDate", [])
    
    # Find the most recent 13F-HR
    for idx, form in enumerate(forms):
        if form == "13F-HR":
            accession = acc_nums[idx]
            accession_no_dash = accession.replace("-", "")
            filing_date = dates[idx]
            
            # The raw txt file contains the XML elements
            txt_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dash}/{accession}.txt"
            return {"accession": accession, "date": filing_date, "txt_url": txt_url}
            
    return None

def process_fund_13f(fund, filing_info):
    txt_url = filing_info["txt_url"]
    print(f"Parsing 13F-HR Information Table from {txt_url}...")
    
    resp = requests.get(txt_url, headers=SEC_HEADERS, timeout=10)
    if resp.status_code != 200:
        print(f"Error fetching TXT: {resp.status_code}")
        return []
        
    content = resp.content.decode("utf-8", errors="replace")
    
    # 13F-HR .txt files contain exactly two <XML> blocks usually: Cover Page and Information Table
    xml_blocks = re.findall(r"<XML>(.*?)</XML>", content, re.DOTALL)
    
    if len(xml_blocks) < 2:
        print("Could not locate Information Table XML block")
        return []
        
    info_table_xml = xml_blocks[1].strip()
    # Clean namespaces so we can use basic findall paths
    xml_content_clean = re.sub(r'\sxmlns="[^"]+"', '', info_table_xml)
    
    try:
        root = ET.fromstring(xml_content_clean)
    except ET.ParseError:
        print("Error parsing XML structure")
        return []
        
    info_tables = root.findall(".//infoTable")
    print(f"Extracted {len(info_tables)} positions.")
    
    aggregated_holdings = {}
    companies_batch = []
    
    for row in info_tables:
        name_node = row.find("nameOfIssuer")
        shares_node = row.find(".//shrsOrPrnAmt/sshPrnamt")
        value_node = row.find("value")
        
        name = name_node.text if name_node is not None else "Unknown"
        shares = shares_node.text if shares_node is not None else "0"
        value = value_node.text if value_node is not None else "0"
        
        try:
            shares_val = int(shares)
            value_val = int(value) * 1000 # SEC 13F values are typically in thousands
        except ValueError:
            continue
            
        ticker = name.split()[0][:5].upper().replace(",", "").replace(".", "")
        if ticker == "APPLE": ticker = "AAPL"
        elif ticker == "MICRO": ticker = "MSFT"
        elif ticker == "NVIDI": ticker = "NVDA"
        elif ticker == "AMAZO": ticker = "AMZN"
        elif ticker == "META": ticker = "META"
        elif ticker == "ALPHABET": ticker = "GOOGL"
        
        companies_batch.append({
            "ticker": ticker,
            "name": name[:255],
            "sector": "Hedge Fund Holding",
            "industry": "Unknown"
        })
            
        
        if ticker not in aggregated_holdings:
            aggregated_holdings[ticker] = {
                "fund_name": fund["name"],
                "ticker": ticker,
                "report_period": filing_info["date"],
                "published_date": filing_info["date"],
                "shares_held": 0,
                "value_held": 0,
                "source_url": txt_url[:500]
            }
            
        aggregated_holdings[ticker]["shares_held"] += shares_val
        aggregated_holdings[ticker]["value_held"] += value_val
        
    holdings = list(aggregated_holdings.values())
        
    print(f"Upserting {len(companies_batch)} tickers to verify Foreign Key constraints...")
    try:
        # Deduplicate companies batch
        seen = set()
        unique_companies = []
        for c in companies_batch:
             if c["ticker"] not in seen:
                 seen.add(c["ticker"])
                 unique_companies.append(c)
                 
        for i in range(0, len(unique_companies), 100):
            supabase.table("companies").upsert(unique_companies[i:i+100]).execute()
    except Exception as e:
        print(f"Error upserting companies: {e}")
        
    return holdings

def main():
    print("Starting Official SEC EDGAR 13F-HR Scraper...")
    all_holdings = []
    inserted_count = 0
    
    for fund in FUNDS:
        filing_info = fetch_latest_13f(fund)
        if filing_info:
            time.sleep(1) # Be a good SEC citizen
            fund_holdings = process_fund_13f(fund, filing_info)
            all_holdings.extend(fund_holdings)
            time.sleep(1)
            
    print(f"\nFinished parsing {len(all_holdings)} total institutional holdings.")
    
    if all_holdings:
        print(f"Uploading {len(all_holdings)} real Hedge Fund holdings to Supabase...")
        
        for i in range(0, len(all_holdings), 500):
            chunk = all_holdings[i:i + 500]
            try:
                supabase.table("institutional_holdings").upsert(
                    chunk,
                    on_conflict="fund_name,ticker,report_period",
                ).execute()
                inserted_count += len(chunk)
            except Exception as e:
                print(f"Failed to upsert 13F chunk: {e}")
                
        print("Successfully seeded INSTITUTIONAL HOLDINGS with REAL DATA!")

    print(
        "SUMMARY_JSON:"
        + json.dumps(
            {
                "records_seen": len(all_holdings),
                "records_inserted": inserted_count,
                "records_skipped": max(len(all_holdings) - inserted_count, 0),
                "funds_tracked": len(FUNDS),
            },
            sort_keys=True,
        )
    )

if __name__ == "__main__":
    main()
