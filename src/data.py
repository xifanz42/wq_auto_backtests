# src/data.py
import os
import time
import sqlite3
import pandas as pd
import json

def get_datafields(sess, searchScope, dataset_id):
    instrument_type = searchScope.get('instrumentType', 'EQUITY')
    region = searchScope.get('region', 'USA')
    delay = str(searchScope.get('delay', '1'))
    universe = searchScope.get('universe', 'TOP3000')

    # ENHANCED CACHE NAME: Now includes scope so you don't corrupt data if you change universe/delay!
    cache_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'datasets')
    os.makedirs(cache_dir, exist_ok=True)
    
    # Example: fundamental6_USA_TOP3000_d1.db
    db_name = f"{dataset_id}_{region}_{universe}_d{delay}.db"
    db_path = os.path.join(cache_dir, db_name)

    # 1. If already cached, load the ENTIRE unfiltered dataset
    if os.path.exists(db_path):
        print(f"      📥 Loading FULL unfiltered {dataset_id} from local cache...")
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM datafields", conn)
        conn.close()
        return df

    # 2. Otherwise, request the ENTIRE unfiltered dataset from API
    print(f"      🌐 Requesting FULL dataset for {dataset_id} from API (Pre-filter)...")
    
    url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        
    initial_res = sess.get(url_template.format(x=0))
    if initial_res.status_code != 200:
        print(f"      ❌ Error fetching data: {initial_res.text}")
        return pd.DataFrame()
        
    count = initial_res.json().get('count', 0)

    datafields_list =[]
    for x in range(0, count, 50):
        datafields = sess.get(url_template.format(x=x))
        if 'results' in datafields.json():
            datafields_list.append(datafields.json()['results'])
        time.sleep(1) # rate limiting sleep

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    df = pd.DataFrame(datafields_list_flat)

    # 3. Save the ENTIRE dataset to SQLite so it can be filtered locally later
    if not df.empty:
        # Convert dict/list columns to string so sqlite can store them
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(json.dumps)

        conn = sqlite3.connect(db_path)
        df.to_sql('datafields', conn, if_exists='replace', index=False)
        conn.close()
        print(f"      💾 Saved full {dataset_id} (Unfiltered) to local SQLite database.")

    return df