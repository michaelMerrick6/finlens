import os
from supabase import create_client, Client
from dotenv import load_dotenv
import random
import datetime

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

companies = ["NVDA", "AAPL", "MSFT", "TSLA", "META"]

insiders = [
    {"name": "Mark Zuckerberg", "relation": "CEO/Director", "ticker": "META"},
    {"name": "Jensen Huang", "relation": "CEO/Director", "ticker": "NVDA"},
    {"name": "Tim Cook", "relation": "CEO/Director", "ticker": "AAPL"},
    {"name": "Elon Musk", "relation": "CEO/Director", "ticker": "TSLA"},
    {"name": "Satya Nadella", "relation": "CEO/Director", "ticker": "MSFT"},
    {"name": "Colette Kress", "relation": "CFO", "ticker": "NVDA"},
]

def random_date(start_year=2024):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date.today()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)

def seed_insider_database():
    print("Seeding 20 realistic historical Insider Form 4s...")
    trades_to_insert = []
    
    for _ in range(20):
        insider = random.choice(insiders)
        tx_date = random_date()
        pub_date = tx_date + datetime.timedelta(days=2) # Form 4s must be filed within 2 days
        
        is_buy = random.random() > 0.8 # Insiders mostly sell for tax reasons
        shares = random.randint(1000, 500000)
        price = round(random.uniform(50.0, 500.0), 2)
        
        trades_to_insert.append({
            "ticker": insider["ticker"],
            "filer_name": insider["name"],
            "filer_relation": insider["relation"],
            "transaction_date": tx_date.isoformat(),
            "published_date": pub_date.isoformat(),
            "transaction_code": "P" if is_buy else "S",
            "amount": shares,
            "price": price,
            "value": shares * price,
            "source_url": "https://www.sec.gov/edgar/search/"
        })
        
    res = supabase.table("insider_trades").insert(trades_to_insert).execute()
    print("Successfully seeded 20 mock Insider Form 4s into Supabase!")

if __name__ == "__main__":
    seed_insider_database()
