import os
import io
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv(".env.local")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials in environment.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
HEADERS = {"User-Agent": "FinLens/1.0 vtdfor@gmail.com"}

def get_base_url(year, quarter):
    return f"https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/{year}q{quarter}_form345.zip"

def process_quarter(year, quarter):
    url = get_base_url(year, quarter)
    print(f"\n--- Downloading {year} Q{quarter} ---")
    print(f"URL: {url}")
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Skipping {year} Q{quarter} - Not Found (may not be released yet)")
            return
        raise e
        
    print("Download complete. Extracting TSVs to pandas in memory...")
    
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Load Non-Derivative Transactions
        with z.open('NONDERIV_TRANS.tsv') as f:
            # We only care about Open Market Buys (P) and Sells (S)
            cols = ['ACCESSION_NUMBER', 'TRANS_DATE', 'TRANS_CODE', 'TRANS_SHARES', 'TRANS_PRICEPERSHARE']
            df_trans = pd.read_csv(f, sep='\t', usecols=lambda c: c in cols, dtype=str)
        
        # Filter before joining to save massive memory
        df_trans = df_trans[df_trans['TRANS_CODE'].isin(['P', 'S'])]
        df_trans.dropna(subset=['TRANS_DATE', 'TRANS_SHARES', 'TRANS_PRICEPERSHARE'], inplace=True)
        
        if df_trans.empty:
            print("No P/S transactions found in this quarter.")
            return

        valid_accessions = df_trans['ACCESSION_NUMBER'].unique()
        
        # Load Submissions
        with z.open('SUBMISSION.tsv') as f:
            cols = ['ACCESSION_NUMBER', 'FILING_DATE', 'ISSUERCIK', 'ISSUERTRADINGSYMBOL']
            df_sub = pd.read_csv(f, sep='\t', usecols=lambda c: c in cols, dtype=str)
        
        df_sub = df_sub[df_sub['ACCESSION_NUMBER'].isin(valid_accessions)]
        
        # Load Reporting Owners
        with z.open('REPORTINGOWNER.tsv') as f:
            cols = ['ACCESSION_NUMBER', 'RPTOWNERNAME', 'RPTOWNER_RELATIONSHIP', 'RPTOWNER_TITLE']
            df_owner = pd.read_csv(f, sep='\t', usecols=lambda c: c in cols, dtype=str)
            
        df_owner = df_owner[df_owner['ACCESSION_NUMBER'].isin(valid_accessions)]
        # De-duplicate owners if there are multiple per accession (just take first)
        df_owner = df_owner.drop_duplicates(subset=['ACCESSION_NUMBER'])
        
    print(f"Extracted {len(df_trans)} filtered trades. Joining data...")
    
    # Merge
    df = df_trans.merge(df_sub, on='ACCESSION_NUMBER', how='inner')
    df = df.merge(df_owner, on='ACCESSION_NUMBER', how='inner')
    
    records = []
    print("Formatting payload...")
    for idx, row in df.iterrows():
        try:
            tx_type = "buy" if row["TRANS_CODE"] == "P" else "sell"
            shares = float(row["TRANS_SHARES"])
            price = float(row["TRANS_PRICEPERSHARE"])
            val = shares * price
            
            if val < 100 or shares <= 0:
                continue # Skip trivial or error records
                
            ticker = str(row["ISSUERTRADINGSYMBOL"]).strip().upper()
            if ticker == "NAN" or not ticker:
                continue
                
            # SEC Document URL
            cik = str(row["ISSUERCIK"]).strip()
            acc = str(row["ACCESSION_NUMBER"]).strip()
            acc_no_hyphen = acc.replace("-", "")
            source_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no_hyphen}/{acc}.txt"
            
            relation = str(row.get("RPTOWNER_TITLE", ""))
            if relation == "nan" or not relation:
                relation = "Insider"
            elif isinstance(relation, float):
                relation = "Insider"
                
            try:
                tx_date_raw = str(row["TRANS_DATE"]).strip()[:11]
                tx_date = datetime.strptime(tx_date_raw, "%d-%b-%Y").strftime("%Y-%m-%d")
            except Exception:
                tx_date = str(row["TRANS_DATE"])
                
            try:
                pub_date_raw = str(row["FILING_DATE"]).strip()[:11]
                pub_date = datetime.strptime(pub_date_raw, "%d-%b-%Y").strftime("%Y-%m-%d")
            except Exception:
                pub_date = str(row["FILING_DATE"])
                
            records.append({
                "source_url": source_url,
                "filer_name": str(row["RPTOWNERNAME"]).strip().title(),
                "filer_relation": relation[:100],
                "ticker": ticker[:10],
                "transaction_date": tx_date,
                "published_date": pub_date,
                "transaction_code": tx_type,
                "amount": int(shares),
                "price": price,
                "value": val
            })
        except Exception as e:
            continue
            
    print(f"Ready to insert {len(records)} verified trades for {year} Q{quarter}.")

    # Insert in chunks of 1000
    chunk_size = 1000
    inserted = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        try:
            # 1. Upsert companies first to satisfy foreign key constraint
            unique_tickers = list({t["ticker"] for t in chunk})
            comp_payload = [{"ticker": tk, "name": tk, "sector": "Unknown", "industry": "Unknown"} for tk in unique_tickers]
            
            # Upsert companies in smaller batches if necessary, but max 1000 is fine
            supabase.table("companies").upsert(comp_payload, on_conflict="ticker").execute()
            
            # 2. Insert the trades
            supabase.table("insider_trades").insert(chunk).execute()
            inserted += len(chunk)
            print(f"Inserted chunk: {inserted} / {len(records)}", end="\r")
        except Exception as e:
            print(f"\nError inserting chunk: {e}")
            continue # Skip bad chunks
            
    print(f"\nCompleted {year} Q{quarter}!")

def main():
    print("Starting 10-Year SEC DERA Bulk Ingestion...")
    # 2015 to 2026
    for year in range(2015, 2027):
        for quarter in [1, 2, 3, 4]:
            if year == 2026 and quarter > 1:
                continue # Future quarters
            try:
                process_quarter(year, quarter)
            except Exception as e:
                print(f"Failed to process {year} Q{quarter}: {e}")
            time.sleep(2) # Polite delay between zips

if __name__ == "__main__":
    main()
