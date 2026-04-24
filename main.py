# main.py  —  WorldQuant Brain Alpha Testing Pipeline
import sys
import os
import json
import time
import sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.auth     import authenticate
from src.data     import get_datafields
from src.alphas   import generate_alphas
from src.simulate import simulate_alphas
from src.report   import generate_markdown_report, generate_error_report


def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Deduplication helpers ─────────────────────────────────────────────────────

def load_tested_expressions(db_path: str) -> set:
    """
    Returns a set of expression strings already saved in alpha_history.db.
    Safe to call even when the file or table does not exist yet (first run).
    """
    if not os.path.exists(db_path):
        return set()
    try:
        conn   = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='alphas_tested'"
        )
        if cursor.fetchone() is None:
            conn.close()
            return set()
        cursor.execute(
            "SELECT DISTINCT expression FROM alphas_tested "
            "WHERE expression IS NOT NULL"
        )
        tested = {row[0] for row in cursor.fetchall()}
        conn.close()
        return tested
    except Exception as exc:
        print(f"   ⚠️  Could not read tested expressions: {exc}")
        return set()


def filter_untested(alpha_list: list, tested: set) -> list:
    before   = len(alpha_list)
    filtered = [a for a in alpha_list if a["regular"] not in tested]
    skipped  = before - len(filtered)
    if skipped:
        print(f"   ⏭  Skipped {skipped} already-tested expression(s).")
    if filtered:
        print(f"   🆕 {len(filtered)} new expression(s) queued for simulation.")
    return filtered


# ── Settings extractor for the report ────────────────────────────────────────

def extract_report_settings(alpha_groups: list) -> dict:
    """
    The report needs a flat settings dict to render delay/universe/etc.
    config.json nests simulation_settings inside each alpha_group, not at
    the top level — that is why the report was showing 'Unknown' for every
    field.  This function extracts the first group's settings and injects
    it as a top-level key so generate_markdown_report can find it.

    If multiple groups exist with different settings, a warning note is added.
    """
    all_settings = [
        g.get("simulation_settings", {})
        for g in alpha_groups
        if g.get("simulation_settings")
    ]
    if not all_settings:
        return {}

    first    = all_settings[0]
    all_same = all(s == first for s in all_settings)

    if all_same:
        return first

    merged = dict(first)
    merged["_note"] = (
        f"⚠️  {len(all_settings)} groups with differing settings — "
        "displaying first group's values in report."
    )
    return merged


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  WorldQuant Brain — Alpha Testing Pipeline")
    print("=" * 60)

    # 1. Config
    config        = load_config()
    search_scope  = config.get("searchScope", {})
    alpha_groups  = config.get("alpha_groups", [])
    save_interval = config.get("save_interval", 10)

    if not alpha_groups:
        print("❌ No 'alpha_groups' found in config.json.")
        return

    print(f"\n[1/6] Loaded {len(alpha_groups)} alpha group(s) from config.")

    # 2. Authenticate
    print("\n[2/6] Authenticating with WorldQuant Brain...")
    sess = authenticate()
    if not sess:
        print("❌ Authentication failed. Check your .env credentials.")
        return
    print("   ✅ Authenticated.")

    # 3. Fetch datafields
    print("\n[3/6] Fetching datafields...")
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

    # 4. Generate payloads
    print("\n[4/6] Generating alpha simulation payloads...")
    alpha_list = generate_alphas(datafields_df, alpha_groups)

    if not alpha_list:
        print("❌ No alpha payloads generated. Check your filters and templates.")
        return

    print(f"   ✅ Generated {len(alpha_list)} alpha payloads across all groups.")

    # 5. Deduplicate
    print("\n[5/6] Deduplication check...")
    db_path = os.path.join(os.path.dirname(__file__), 'results', 'alpha_history.db')
    tested  = load_tested_expressions(db_path)
    print(f"   📂 Local DB contains {len(tested)} previously-tested expression(s).")
    alpha_list = filter_untested(alpha_list, tested)

    if not alpha_list:
        print("\n✅ All expressions in this config have already been tested. Nothing to do.")
        return

    # 6. Simulate
    print("\n[6/6] Running simulations...\n")
    start_time = time.time()

    results, breaker_info = simulate_alphas(
        sess,
        alpha_list,
        save_interval=save_interval,
        authenticate_callback=authenticate,
    )

    overall_time = time.time() - start_time
    avg_time     = (overall_time / len(results)) if results else 0

    # Report
    # Bug fix: inject extracted simulation_settings at top level of config
    # so generate_markdown_report can read delay/universe/neutralization/truncation.
    report_settings  = extract_report_settings(alpha_groups)
    enriched_config  = {**config, "simulation_settings": report_settings}

    if breaker_info and breaker_info.get("triggered"):
        generate_error_report(breaker_info, overall_time)

    generate_markdown_report(results, enriched_config, overall_time, avg_time)

    passed = [r for r in results if r.get("status") == "Success"]
    print(f"\n✅ Done. {len(passed)}/{len(results)} alphas passed.")
    print(f"   Total time: {overall_time/60:.1f} minutes")


if __name__ == "__main__":
    main()