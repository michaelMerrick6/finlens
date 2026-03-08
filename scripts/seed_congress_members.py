import os
import requests
import yaml
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")
url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")

try:
    supabase: Client = create_client(url, key)
except Exception as e:
    print(f"Error creating supabase client: {e}")
    exit(1)

# The open-source standard for congressional data moved exclusively to YAML
LEGISLATORS_CURRENT_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
LEGISLATORS_HISTORICAL_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-historical.yaml"

def fetch_members(url, active=True):
    print(f"Fetching {url}...")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = yaml.safe_load(r.text)
        return data, active
    except Exception as e:
        print(f"Failed fetching data from {url}: {e}")
        return [], active

def process_and_seed():
    print("Beginning Congressional Bioguide ID Database Seeding...")
    current_data, _ = fetch_members(LEGISLATORS_CURRENT_URL, active=True)
    historical_data, _ = fetch_members(LEGISLATORS_HISTORICAL_URL, active=False)
    
    all_members = current_data + historical_data[-500:] # Only grab recent historicals to save DB space
    
    members_batch = []
    
    for member in all_members:
        id_obj = member.get("id", {})
        bioguide = id_obj.get("bioguide")
        if not bioguide:
            continue
            
        name_obj = member.get("name", {})
        first_name = name_obj.get("first", "")
        last_name = name_obj.get("last", "")
        
        terms = member.get("terms", [])
        if not terms:
             continue
             
        latest_term = terms[-1]
        state = latest_term.get("state", "")
        party = latest_term.get("party", "")
        chamber_raw = latest_term.get("type", "")
        
        chamber = "Senate" if chamber_raw == "sen" else "House"
        is_active = member in current_data
        
        members_batch.append({
            "id": bioguide,
            "first_name": first_name[:100],
            "last_name": last_name[:100],
            "state": state[:50],
            "party": party[:50],
            "chamber": chamber,
            "active": is_active,
            "source_url": f"https://bioguide.congress.gov/search/bio/{bioguide}"
        })
        
    print(f"Parsed {len(members_batch)} unified Congress Members.")
    
    # Deduplicate
    seen = set()
    unique_members = []
    for m in members_batch:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique_members.append(m)
            
    print(f"Upserting {len(unique_members)} unique bioguide records into Supabase `congress_members` table...")
    
    for i in range(0, len(unique_members), 200):
        chunk = unique_members[i:i + 200]
        try:
             res = supabase.table("congress_members").upsert(chunk).execute()
        except Exception as e:
             print(f"Error inserting chunk: {e}")
             
    print("✅ Completed seeding `congress_members` table.")

if __name__ == "__main__":
    process_and_seed()
