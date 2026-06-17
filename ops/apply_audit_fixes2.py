from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import os
import sys
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local")

from supabase import create_client

url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
sb = create_client(url, key)

def catch_remaining_dupes():
    print("Finding remaining duplicates using stable offset pagination...")
    seen = {}
    dupes = []
    limit = 1000
    last_id = None
    total_scanned = 0
    
    while True:
        try:
            query = sb.table("insider_trades") \
                .select("id,source_url,ticker,transaction_date,filer_name") \
                .not_.is_("source_url", "null") \
                .order("id", desc=False) \
                .limit(limit)
            
            if last_id:
                query = query.gt("id", last_id)
                
            res = query.execute()
        except Exception as exc:
            print(f"API Error at scanned {total_scanned}: {exc}")
            time.sleep(2)
            continue
            
        rows = res.data or []
        if not rows:
            break
            
        for row in rows:
            k = (
                row.get("source_url") or "", 
                row.get("ticker") or "", 
                row.get("transaction_date") or "", 
                row.get("filer_name") or ""
            )
            if k in seen:
                dupes.append(row["id"])
            else:
                seen[k] = row["id"]
                
            last_id = row["id"]
                
        total_scanned += len(rows)
        print(f"Scanned {total_scanned:,} rows, found {len(dupes):,} dupes remaining...", end="\r")
        
        if len(rows) < limit:
            break

    print()
    if dupes:
        print(f"Deleting {len(dupes):,} missed duplicates...")
        for i in range(0, len(dupes), 200):
            chunk = dupes[i:i+200]
            for attempt in range(3):
                try:
                    sb.table("insider_trades").delete().in_("id", chunk).execute()
                    break
                except Exception as e:
                    time.sleep(1)
            print(f"  Deleted {min(i+200, len(dupes)):,} / {len(dupes):,}")
    print("Done! You can now create the index.")

catch_remaining_dupes()
