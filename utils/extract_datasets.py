import sys
import os
import time
import json
import pandas as pd

# Add the parent directory to sys.path so we can import from src.auth
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.auth import authenticate

def fetch_all_datasets():
    print("🔐 Authenticating...")
    sess = authenticate()
    if not sess:
        print("❌ Authentication failed. Please check your .env credentials.")
        return

    # API Endpoint for Datasets
    # We first try to get EVERYTHING without filters. If it demands parameters, 
    # we fall back to a broad equity scope to ensure the request goes through.
    base_url = "https://api.worldquantbrain.com/data-sets?limit=50&offset={x}"
    
    print("🌐 Requesting dataset information from Brain API...")
    initial_res = sess.get(base_url.format(x=0))
    
    # Fallback in case the API requires specific scope parameters
    if initial_res.status_code != 200:
        print(f"⚠️ Initial request failed: {initial_res.text}")
        print("🔄 Retrying with broad scope parameters (EQUITY, USA)...")
        base_url = ("https://api.worldquantbrain.com/data-sets?"
                    "instrumentType=EQUITY&region=USA&delay=1&universe=TOP3000&limit=50&offset={x}")
        initial_res = sess.get(base_url.format(x=0))
        if initial_res.status_code != 200:
            print(f"❌ Error fetching data-sets: {initial_res.text}")
            return
            
    res_json = initial_res.json()
    count = res_json.get('count', 0)
    print(f"📊 Total datasets found: {count}")

    datasets_list =[]
    
    # Paginate through the results (limit 50 per request)
    for x in range(0, count, 50):
        print(f"   📥 Fetching records {x} to {min(x + 50, count)}...")
        response = sess.get(base_url.format(x=x))
        if response.status_code == 200:
            results = response.json().get('results',[])
            datasets_list.extend(results)
        else:
            print(f"   ⚠️ Error at offset {x}: {response.text}")
        
        # Respect API rate limits
        time.sleep(1) 

    if not datasets_list:
        print("❌ No datasets retrieved.")
        return

    # Convert results into a Pandas DataFrame
    df = pd.DataFrame(datasets_list)
    
    # API data often contains nested dicts/lists (like coverage stats or tags). 
    # We convert those to strings so they write to CSV cleanly.
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            
    # Reorder columns to put the most important ones first (if they exist in the payload)
    cols = df.columns.tolist()
    preferred_order = ['id', 'name', 'description', 'category', 'subCategory']
    ordered_cols = [c for c in preferred_order if c in cols] +[c for c in cols if c not in preferred_order]
    df = df[ordered_cols]

    # Create export directory if it doesn't exist
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the dataframe to a CSV file
    output_file = os.path.join(output_dir, "all_worldquant_datasets.csv")
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"\n✅ Successfully exported {len(df)} datasets to: {output_file}")
    
    # Show a brief preview in the console
    print("\nPreview of the first 5 datasets:")
    if 'id' in df.columns and 'description' in df.columns:
        print(df[['id', 'name', 'description']].head())
    else:
        print(df.head())

if __name__ == "__main__":
    fetch_all_datasets()