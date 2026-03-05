import os
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Setup Supabase client
url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

HEADERS = {
    'User-Agent': 'Mike Vail mvail@example.com' # Replace with your email
}

def fetch_latest_13fhr():
    """Fetches the latest 13F-HR submissions from SEC EDGAR RSS feed."""
    rss_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&company=&dateb=&owner=include&start=0&count=40&output=atom"
    print(f"Fetching {rss_url}...")
    
    response = requests.get(rss_url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Error fetching SEC RSS for 13F-HR: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.content, 'xml')
    entries = soup.find_all('entry')
    
    parsed_filings = []
    
    for entry in entries:
        title = entry.title.text if entry.title else ""
        link = entry.link['href'] if entry.link else ""
        updated = entry.updated.text if entry.updated else ""
        
        # In a complete implementation, we'd navigate into the link
        # Find the 'Information Table' XML file
        # Iterate over `<infoTable>` sections to get <nameOfIssuer>, <cusip>, <value>, <shrsOrPrnAmt>
        parsed_filings.append({
            "title": title,
            "url": link,
            "published_at": updated
        })
    
    return parsed_filings

def process_and_upload_holdings(filings):
    print(f"Found {len(filings)} recent 13F-HR filings vs the previous scrape.")
    # supabase.table("institutional_holdings").insert({...}).execute()
    pass

if __name__ == "__main__":
    filings = fetch_latest_13fhr()
    process_and_upload_holdings(filings)
    print("13F-HR scraping complete.")
