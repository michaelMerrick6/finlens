import os
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables (mostly for local testing)
load_dotenv()

# Setup Supabase client
url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

# SEC requires a user-agent declaring who you are
HEADERS = {
    'User-Agent': 'Mike Vail mvail@example.com' # Replace with your email for SEC compliance
}

def fetch_latest_form4s():
    """Fetches the latest Form 4 submissions from SEC EDGAR RSS feed."""
    rss_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include&start=0&count=40&output=atom"
    print(f"Fetching {rss_url}...")
    
    response = requests.get(rss_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Error fetching SEC RSS: {response.status_code}")
        return []
    
    # Parse atom feed
    soup = BeautifulSoup(response.content, 'xml')
    entries = soup.find_all('entry')
    
    parsed_trades = []
    
    for entry in entries:
        title = entry.title.text if entry.title else ""
        link = entry.link['href'] if entry.link else ""
        updated = entry.updated.text if entry.updated else ""
        
        # We need the actual XML of the filing to get precise trade data (shares, price, ticker)
        # This points to the -index.htm, we need to find the raw XML form.
        parsed_trades.append({
            "title": title,
            "url": link,
            "published_at": updated
        })
    
    return parsed_trades

def process_and_upload_trades(trades):
    """(Skeleton) Parse raw EDGAR XML and push to Supabase."""
    print(f"Found {len(trades)} recent Form 4 filings.")
    # In a full implementation, you'd extract the accession number, fetch the XML filing,
    # extract the reportingOwner, derivativeTable, nonDerivativeTable, and ticker.
    # Then insert into supabase: supabase.table("insider_trades").insert({...}).execute()
    pass

if __name__ == "__main__":
    trades = fetch_latest_form4s()
    process_and_upload_trades(trades)
    print("Form 4 scraping complete.")
