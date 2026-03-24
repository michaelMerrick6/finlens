import os
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from legacy_congress_guard import require_legacy_write_opt_in

load_dotenv(dotenv_path=".env.local")
require_legacy_write_opt_in("ingest_historical_politicians.py")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

# Capitol Trades is a known aggregator that we can paginate through
CAPITOL_TRADES_API = "https://bff.capitoltrades.com/trades"

def fetch_and_ingest_capitol_trades():
    print("Fetching Capitol Trades History...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    
    # We will simulate fetching the first page of 100 historical trades to seed the DB
    params = {
        'page': 1,
        'pageSize': 100
    }
    
    try:
        response = requests.get(CAPITOL_TRADES_API, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Failed to fetch Capitol Trades data: {response.status_code}")
            return
            
        data = response.json()
        trades = data.get('data', [])
        print(f"Loaded {len(trades)} historical trades from CapitolTrades.")
        
        formatted_trades = []
        
        for trade in trades:
            issuer = trade.get('issuer', {})
            ticker = issuer.get('ticker')
            
            if not ticker:
                continue
                
            # Upsert company
            try:
                 supabase.table("companies").upsert({
                     "ticker": ticker,
                     "name": issuer.get('issuerName', ticker),
                     "sector": issuer.get('sector', 'Unknown'),
                     "industry": "Unknown"
                 }).execute()
            except Exception as e:
                 pass
            
            politician = trade.get('politician', {})
            formatted_trade = {
                "politician_name": f"{politician.get('firstName', '')} {politician.get('lastName', '')}".strip(),
                "chamber": "House" if politician.get('chamber') == 'house' else "Senate",
                "party": politician.get('party', 'Unknown'),
                "ticker": ticker,
                "transaction_date": trade.get("txDate", "1970-01-01")[:10],
                "published_date": trade.get("pubDate", "1970-01-01")[:10],
                "transaction_type": trade.get("txType", "Unknown"),
                "asset_type": trade.get("assetType", "Stock"),
                "amount_range": f"${trade.get('value', 0)}", # Capitol Trades often estimates value
                "source_url": f"https://www.capitoltrades.com/trades/{trade.get('_txId', '')}"
            }
            formatted_trades.append(formatted_trade)
            
        if formatted_trades:
            print(f"Uploading {len(formatted_trades)} trades to Supabase...")
            res = supabase.table("politician_trades").insert(formatted_trades).execute()
            print("Successfully seeded Politician Trades table with CapitolTrades history!")
            
    except Exception as e:
        print(f"Error fetching/inserting trades: {e}")

if __name__ == "__main__":
    fetch_and_ingest_capitol_trades()
