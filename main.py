# main.py  —  WorldQuant Brain Alpha Testing Pipeline
import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.auth     import authenticate
from src.data     import get_datafields
from src.alphas   import generate_alphas
from src.simulate import simulate_alphas
from src.report   import generate_markdown_report, generate_error_report


def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    print("=" * 60)
    print("  WorldQuant Brain — Alpha Testing Pipeline")
    print("=" * 60)

    # ── 1. Load config ───────────────────────────────────────────────────────
    config = load_config()
    search_scope  = config.get("searchScope", {})
    alpha_groups  = config.get("alpha_groups", [])
    save_interval = config.get("save_interval", 10)

    if not alpha_groups:
        print("❌ No 'alpha_groups' found in config.json. Please add at least one group.")
        return

    print(f"\n[1/5] Loaded {len(alpha_groups)} alpha group(s) from config.")

    # ── 2. Authenticate ──────────────────────────────────────────────────────
    print("\n[2/5] Authenticating with WorldQuant Brain...")
    sess = authenticate()
    if not sess:
        print("❌ Authentication failed. Check your .env credentials.")
        return
    print("   ✅ Authenticated.")

    # ── 3. Fetch datafields for every unique dataset across all groups ────────
    print("\n[3/5] Fetching datafields...")

    # Collect unique (dataset_id) across all groups
    seen_datasets = {}
    for group in alpha_groups:
        for ds_conf in group.get("datasets", []):
            ds_id = ds_conf["id"]
            if ds_id not in seen_datasets:
                seen_datasets[ds_id] = ds_conf

    import pandas as pd
    all_dfs = []
    for ds_id, ds_conf in seen_datasets.items():
        df = get_datafields(sess, search_scope, ds_id)
        if not df.empty:
            df["target_dataset_id"] = ds_id
            all_dfs.append(df)

    if not all_dfs:
        print("❌ No datafields retrieved. Aborting.")
        return

    datafields_df = pd.concat(all_dfs, ignore_index=True)
    print(f"   ✅ {len(datafields_df)} total datafield rows loaded.")

    # ── 4. Generate alpha payloads ───────────────────────────────────────────
    print("\n[4/5] Generating alpha simulation payloads...")
    alpha_list = generate_alphas(datafields_df, alpha_groups)

    if not alpha_list:
        print("❌ No alpha payloads generated. Check your filters and templates.")
        return

    print(f"   ✅ Generated {len(alpha_list)} alpha payloads across all groups.")

    # ── 5. Simulate ──────────────────────────────────────────────────────────
    print("\n[5/5] Running simulations...\n")
    start_time = time.time()

    results, breaker_info = simulate_alphas(
        sess,
        alpha_list,
        save_interval=save_interval,
        authenticate_callback=authenticate,
    )

    overall_time = time.time() - start_time
    avg_time     = (overall_time / len(results)) if results else 0

    # ── Report ───────────────────────────────────────────────────────────────
    if breaker_info and breaker_info.get("triggered"):
        generate_error_report(breaker_info, overall_time)
    
    # Always generate the main report, even on partial runs
    generate_markdown_report(results, config, overall_time, avg_time)

    passed = [r for r in results if r.get("status") == "Success"]
    print(f"\n✅ Done. {len(passed)}/{len(results)} alphas passed.")
    print(f"   Total time: {overall_time/60:.1f} minutes")


if __name__ == "__main__":
    main()