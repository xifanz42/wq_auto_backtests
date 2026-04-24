import sys
import os
import time
import json
import pandas as pd

# Add the parent directory to sys.path so we can import from src.auth
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.auth import authenticate

def fetch_all_operators():
    print("🔐 Authenticating...")
    sess = authenticate()
    if not sess:
        print("❌ Authentication failed.")
        return

    base_url = "https://api.worldquantbrain.com/operators?limit=50&offset={x}"
    
    print("🌐 Requesting operators information from Brain API...")
    initial_res = sess.get(base_url.format(x=0))
    
    if initial_res.status_code != 200:
        print(f"❌ Error fetching operators: {initial_res.text}")
        return
            
    res_json = initial_res.json()
    operators_list =[]
    
    # Handle both paginated dictionaries and flat lists
    if isinstance(res_json, dict) and 'results' in res_json:
        count = res_json.get('count', 0)
        print(f"📊 Total operators found: {count}")
        
        for x in range(0, count, 50):
            print(f"   📥 Fetching records {x} to {min(x + 50, count)}...")
            response = sess.get(base_url.format(x=x))
            if response.status_code == 200:
                results = response.json().get('results',[])
                operators_list.extend(results)
            time.sleep(1) 
            
    elif isinstance(res_json, list):
        print(f"📊 Total operators found: {len(res_json)}")
        operators_list = res_json
    else:
        print("⚠️ Unexpected response format from /operators endpoint.")
        return

    if not operators_list: return

    df = pd.DataFrame(operators_list)
    
    # Flatten nested data to strings for clean CSV export
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            
    cols = df.columns.tolist()
    preferred_order = ['id', 'name', 'description', 'category', 'type', 'syntax']
    ordered_cols = [c for c in preferred_order if c in cols] +[c for c in cols if c not in preferred_order]
    df = df[ordered_cols]

    # Save to the data/ directory located one level up
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "all_worldquant_operators.csv")
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"\n✅ Successfully exported {len(df)} operators to: {output_file}")

if __name__ == "__main__":
    fetch_all_operators()