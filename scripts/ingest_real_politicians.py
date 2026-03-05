import os
import json
import re
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

def fetch_real_trades():
    print("Fetching QuiverQuant HTML...")
    # Use a generic User-Agent to avoid basic blocks
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get("https://www.quiverquant.com/congresstrading/", headers=headers, timeout=20)
    
    if response.status_code != 200:
        print(f"Failed to fetch QuiverQuant: HTML status {response.status_code}")
        return
        
    html = response.text
    
    # Extract the JS array assignment: let recentTradesData = [[...]];
    # Using a regex that looks for let recentTradesData = [ up to the closing ];
    match = re.search(r"let\s+recentTradesData\s*=\s*(\[\[.*?\]\]);", html, flags=re.DOTALL)
    if not match:
        print("Could not find recentTradesData array in the HTML.")
        return
        
    json_str = match.group(1)
    
    # Parse the array using json
    try:
        import ast
        json_str = json_str.replace("null", "None").replace("true", "True").replace("false", "False")
        trades = ast.literal_eval(json_str)
    except Exception as e:
        print(f"Failed to parse JSON array: {e}")
        return
        
    print(f"Successfully scraped {len(trades)} REAL historical trades!")
    
    formatted_trades = []
    
    for row in trades:
        # Array schema:
        # [0] = Ticker
        # [1] = Company Name
        # [2] = Asset Type
        # [3] = Transaction Type (Sale / Purchase)
        # [4] = Amount Range
        # [5] = Politician Name
        # [6] = Chamber
        # [7] = Party
        # [8] = Publication Date (string)
        # [9] = Transaction Date (string)
        
        if len(row) < 10:
            continue
            
        ticker = row[0]
        if not ticker or ticker == "-":
            continue
            
        ticker = ticker[:10]
        company_name = row[1]
        asset_type = row[2]
        tx_type_raw = row[3]
        amount_range = row[4]
        politician_name = row[5]
        chamber = row[6]
        party_raw = row[7]
        pub_date = row[8][:10] if row[8] else "1970-01-01"
        tx_date = row[9][:10] if row[9] else "1970-01-01"
        
        # Normalize party
        party = "Republican" if party_raw == "R" else "Democrat" if party_raw == "D" else party_raw
        
        # Normalize transaction type
        if "Sale" in tx_type_raw:
            tx_type = "sell"
        elif "Purchase" in tx_type_raw:
            tx_type = "buy"
        else:
            tx_type = tx_type_raw.lower()
            
        # Upsert Company
        try:
             supabase.table("companies").upsert({
                 "ticker": ticker,
                 "name": company_name,
                 "sector": "Unknown",
                 "industry": "Unknown"
             }).execute()
        except:
             pass
             
        # Add to formatted array
        formatted_trade = {
            "politician_name": politician_name[:100] if politician_name else "",
            "chamber": chamber[:10] if chamber else "",
            "party": party[:20] if party else "",
            "ticker": ticker[:10] if ticker else "",
            "transaction_date": tx_date,
            "published_date": pub_date,
            "transaction_type": tx_type[:10],
            "asset_type": asset_type[:50] if asset_type else "",
            "amount_range": amount_range[:255] if amount_range else "",
            "source_url": "https://www.quiverquant.com/congresstrading/"
        }
        formatted_trades.append(formatted_trade)
        
    if formatted_trades:
        # Clear out the mock trades first to maintain data integrity
        try:
            print("Clearing old mock data...")
            supabase.table("politician_trades").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        except Exception as e:
            pass
    
        print(f"Uploading {len(formatted_trades)} real historical trades to Supabase...")
        # Insert in chunks of 50
        for i in range(0, len(formatted_trades), 50):
            chunk = formatted_trades[i:i + 50]
            try:
                supabase.table("politician_trades").insert(chunk).execute()
            except Exception as e:
                print(f"Error inserting chunk: {e}")
            
        print("Successfully seeded POLITICIAN Trades table with REAL history!")

if __name__ == "__main__":
    fetch_real_trades()
