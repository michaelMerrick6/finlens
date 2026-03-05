import os
from supabase import create_client, Client
from dotenv import load_dotenv
import random

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

companies = ["NVDA", "AAPL", "MSFT", "TSLA", "META"]

funds = [
    "Bridgewater Associates, LP",
    "Renaissance Technologies LLC",
    "Citadel Advisors LLC",
    "Two Sigma Investments, LP",
    "Point72 Asset Management, L.P.",
    "AQR Capital Management, LLC"
]

def seed_fund_database():
    print("Seeding 20 realistic historical Hedge Fund 13F-HR holdings...")
    holdings_to_insert = []
    
    for _ in range(20):
        fund = random.choice(funds)
        ticker = random.choice(companies)
        
        shares = random.randint(100000, 5000000)
        qoq_percent = round(random.uniform(-100.0, 300.0), 2)
        
        holdings_to_insert.append({
            "fund_name": fund,
            "ticker": ticker,
            "report_period": "2025-12-31",
            "published_date": "2026-02-14", # Usually 45 days after quarter end
            "shares_held": shares,
            "value_held": int(shares * random.uniform(50.0, 500.0)),
            "qoq_change_shares": int(shares * (qoq_percent / 100)),
            "qoq_change_percent": qoq_percent,
            "source_url": "https://www.sec.gov/edgar/search/"
        })
        
    try:
        res = supabase.table("institutional_holdings").insert(holdings_to_insert).execute()
        print("Successfully seeded 20 mock 13F-HR holdings into Supabase!")
    except Exception as e:
        print(f"Postgres unique constraint usually drops duplicates, suppressing error: {e}")

if __name__ == "__main__":
    seed_fund_database()
