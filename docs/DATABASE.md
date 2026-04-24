# Inspecting the Centralized SQLite Database

Since all your tested alpha templates, metrics, and JSON settings are now being stored efficiently in a centralized SQLite Database (`results/alpha_history.db`), you might be wondering how to interact with it, especially if you are only used to traditional distributed databases like PostgreSQL.

This guide reveals how incredibly easy it is to inspect, query, and analyze your generated data.

## Will SQLite get "Messed Up" with Too Much Data?
**Absolutely not.**  
While SQLite has "Lite" in the name, it refers to its administration architecture (it requires zero background servers or port setups—the entire database exists elegantly as a single `.db` file). 

SQLite processes are embedded in almost every phone, web browser, and airplane systems globally. It can effortlessly query tables containing **Millions of Rows** and store up to **140 Terabytes** of data. Your alphanumeric simulation strings and JSON settings will barely scratch its surface.  

---

## 1. Inspecting via DBeaver (GUI)

[DBeaver](https://dbeaver.io/) is an excellent free, universal database GUI that natively supports connecting to local SQLite files just as easily as PostgreSQL.

### Instructions:
1. Open DBeaver.
2. Click **Database > New Database Connection** (or the plug icon in the top left).
3. Search for and select **SQLite** from the list of connection types and click **Next**.
4. In the **Path** box, click the Folder icon (`Browse...`) and simply locate your file: `community/results/alpha_history.db`.
5. Click **Finish**. (If DBeaver asks you to download SQLite drivers, click `Download`—it will automatically fetch what it needs).

You can now expand the Database tree in DBeaver under **Tables > alphas_tested** and double-click to view your data in a beautiful, filterable Excel-like spreadsheet or run standard SQL queries like:
```sql
SELECT expression, is_sharpe, is_turnover, settings_json 
FROM alphas_tested 
WHERE is_quality = 'Spectacular';
```

---

## 2. Inspecting via Python / Jupyter Notebook

If you prefer to inspect your historical database visually using Python in your `notebook.ipynb`, this process is natively supported via Pandas without installing any extra architecture.

Simply create a new cell in your Jupyter notebook and execute:

```python
import sqlite3
import pandas as pd
import json

# 1. Connect to the local SQLite file
db_path = "results/alpha_history.db"
conn = sqlite3.connect(db_path)

# 2. Run your SQL Query to grab results
# (Example: find all Success alphas with an Average or better rating)
df = pd.read_sql("""
    SELECT * 
    FROM alphas_tested 
    WHERE status = 'Success' 
""", conn)

# 3. Always close the connection when done
conn.close()

# 4. (Optional) Parse your saved JSON settings if you want to inspect them as Dicts
if not df.empty and 'settings_json' in df.columns:
    df['parsed_settings'] = df['settings_json'].apply(lambda x: json.loads(x) if pd.notna(x) else {})
    df['extracted_delay'] = df['parsed_settings'].apply(lambda x: x.get('delay'))

# 5. Display horizontally in Jupyter
display(df.head())
```

Once loaded into Pandas, you can effortlessly visualize your massive historical alpha distributions or filter specific subindustries purely via Python variables!
