import os
from supabase import create_client, Client
from dotenv import load_dotenv
import random
import datetime

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

companies = [
    {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology"},
    {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Technology"},
    {"ticker": "TSLA", "name": "Tesla, Inc.", "sector": "Consumer Discretionary"},
    {"ticker": "META", "name": "Meta Platforms, Inc.", "sector": "Communication Services"}
]

politicians = [
    {"name": "Nancy Pelosi", "chamber": "House", "party": "Democrat"},
    {"name": "Tommy Tuberville", "chamber": "Senate", "party": "Republican"},
    {"name": "Ro Khanna", "chamber": "House", "party": "Democrat"},
    {"name": "Mark Green", "chamber": "House", "party": "Republican"},
    {"name": "Rick Scott", "chamber": "Senate", "party": "Republican"}
]

amount_ranges = ["$1,001 - $15,000", "$15,001 - $50,000", "$50,001 - $100,000", "$100,001 - $250,000", "$500,001 - $1,000,000", "$1,000,001 - $5,000,000"]
transaction_types = ["Purchase", "Sale (Full)", "Sale (Partial)"]

def random_date(start_year=2024):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date.today()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)

def seed_database():
    print("Seeding initial companies...")
    for comp in companies:
        supabase.table("companies").upsert(comp).execute()

    print("Seeding 20 realistic historical trades...")
    trades_to_insert = []
    
    for _ in range(20):
        comp = random.choice(companies)
        pol = random.choice(politicians)
        tx_date = random_date()
        pub_date = tx_date + datetime.timedelta(days=random.randint(10, 40))
        
        trades_to_insert.append({
            "politician_name": pol["name"],
            "chamber": pol["chamber"],
            "party": pol["party"],
            "ticker": comp["ticker"],
            "transaction_date": tx_date.isoformat(),
            "published_date": pub_date.isoformat(),
            "transaction_type": random.choice(transaction_types),
            "asset_type": "Stock",
            "amount_range": random.choice(amount_ranges),
            "source_url": "https://disclosures-clerk.house.gov/"
        })
        
    res = supabase.table("politician_trades").insert(trades_to_insert).execute()
    print("Successfully seeded 20 mock politician trades into the Supabase database to verify the frontend works!")

if __name__ == "__main__":
    seed_database()
