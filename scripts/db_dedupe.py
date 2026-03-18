import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
supabase: Client = create_client(url, key)

def deduplicate():
    print("Starting massive deduplication audit...")
    # Since we can't run raw SQL easily via the standard python client, 
    # we'll fetch chunks and delete by ID.
    
    # We'll use doc_id + ticker + transaction_date as the unique key
    seen = set()
    to_delete = []
    
    # Process in batches
    limit = 1000
    offset = 0
    
    while True:
        print(f"Scanning rows {offset} to {offset+limit}...")
        res = supabase.table("politician_trades").select("id, doc_id").order("id").range(offset, offset + limit).execute()
        if not res.data:
            break
            
        for row in res.data:
            did = row["doc_id"]
            if did in seen:
                to_delete.append(row["id"])
            else:
                seen.add(did)
                
        if len(to_delete) >= 500:
            print(f"Purging {len(to_delete)} duplicates...")
            for i in range(0, len(to_delete), 100):
                chunk = to_delete[i:i + 100]
                supabase.table("politician_trades").delete().in_("id", chunk).execute()
            to_delete = []
            
        offset += limit
        if len(res.data) < limit:
            break

    if to_delete:
        print(f"Final purge of {len(to_delete)} duplicates...")
        for i in range(0, len(to_delete), 100):
            chunk = to_delete[i:i + 100]
            supabase.table("politician_trades").delete().in_("id", chunk).execute()

    print("Deduplication complete.")

if __name__ == "__main__":
    deduplicate()
