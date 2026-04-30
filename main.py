# main.py  —  WorldQuant Brain Alpha Testing Pipeline
import sys, os, json, time, sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.auth     import authenticate
from src.data     import get_datafields
from src.alphas   import generate_alphas
from src.simulate import simulate_alphas
from src.report   import generate_markdown_report, generate_error_report

import pandas as pd


# ── Config ────────────────────────────────────────────────────────────────────

def load_config(path="config.json") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Deduplication ─────────────────────────────────────────────────────────────

def load_tested_expressions(db_path: str) -> set:
    """Return the set of expression strings already in alpha_history.db."""
    if not os.path.exists(db_path):
        return set()
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='alphas_tested'"
            )
            if not cur.fetchone():
                return set()
            cur.execute(
                "SELECT DISTINCT expression FROM alphas_tested "
                "WHERE expression IS NOT NULL"
            )
            return {row[0] for row in cur.fetchall()}
    except Exception as exc:
        print(f"   ⚠️  Could not read tested expressions: {exc}")
        return set()


def filter_untested(alpha_list: list, tested: set) -> list:
    filtered = [a for a in alpha_list if a["regular"] not in tested]
    skipped  = len(alpha_list) - len(filtered)
    if skipped:
        print(f"   ⏭  Skipped {skipped} already-tested expression(s).")
    if filtered:
        print(f"   🆕 {len(filtered)} new expression(s) queued.")
    return filtered


# ── Report helpers ────────────────────────────────────────────────────────────

def extract_report_settings(alpha_groups: list) -> dict:
    """
    Pull simulation_settings from the first alpha_group and surface them at
    the top-level config key that generate_markdown_report expects.
    """
    all_settings = [g.get("simulation_settings", {}) for g in alpha_groups
                    if g.get("simulation_settings")]
    if not all_settings:
        return {}
    first = all_settings[0]
    if not all(s == first for s in all_settings):
        first["_note"] = (
            f"⚠️  {len(all_settings)} groups with differing settings — "
            "showing first group's values."
        )
    return first


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  WorldQuant Brain — Alpha Testing Pipeline")
    print("=" * 60)

    # 1. Config
    config       = load_config()
    alpha_groups = config.get("alpha_groups", [])
    sim_mode     = config.get("simulation_mode", "single")   # "single"|"batch"|"auto"
    save_interval = config.get("save_interval", 1)

    if not alpha_groups:
        print("❌ No 'alpha_groups' in config.json.")
        return
    print(f"\n[1/5] Loaded {len(alpha_groups)} alpha group(s).  mode={sim_mode}")

    # 2. Authenticate
    print("\n[2/5] Authenticating…")
    sess = authenticate()
    if not sess:
        print("❌ Authentication failed.")
        return
    print("   ✅ Authenticated.")

    # 3. Fetch datafields (deduplicate by dataset id)
    print("\n[3/5] Fetching datafields…")
    seen, all_dfs = {}, []
    for group in alpha_groups:
        for ds_conf in group.get("datasets", []):
            ds_id = ds_conf["id"]
            if ds_id not in seen:
                seen[ds_id] = ds_conf
                df = get_datafields(sess, config.get("searchScope", {}), ds_id)
                if not df.empty:
                    df["target_dataset_id"] = ds_id
                    all_dfs.append(df)

    if not all_dfs:
        print("❌ No datafields retrieved.")
        return

    datafields_df = pd.concat(all_dfs, ignore_index=True)
    print(f"   ✅ {len(datafields_df)} datafield rows loaded.")

    # 4. Generate & deduplicate payloads
    print("\n[4/5] Generating alpha payloads…")
    alpha_list = generate_alphas(datafields_df, alpha_groups)
    if not alpha_list:
        print("❌ No payloads generated.")
        return
    print(f"   ✅ {len(alpha_list)} payload(s) generated.")

    db_path = os.path.join(os.path.dirname(__file__), "results", "alpha_history.db")
    tested  = load_tested_expressions(db_path)
    print(f"   📂 DB has {len(tested)} previously-tested expression(s).")
    alpha_list = filter_untested(alpha_list, tested)
    if not alpha_list:
        print("\n✅ All expressions already tested. Nothing to do.")
        return

    # 5. Simulate
    print(f"\n[5/5] Running simulations…\n")
    start = time.time()

    results, breaker_info = simulate_alphas(
        sess,
        alpha_list,
        mode=sim_mode,
        save_interval=save_interval,
        authenticate_callback=authenticate,
    )

    elapsed  = time.time() - start
    avg_time = elapsed / len(results) if results else 0

    # Report
    enriched_config = {
        **config,
        "simulation_settings": extract_report_settings(alpha_groups),
    }
    if breaker_info and breaker_info.get("triggered"):
        generate_error_report(breaker_info, elapsed)

    generate_markdown_report(results, enriched_config, elapsed, avg_time)

    passed = [r for r in results if r.get("status") == "Success"]
    print(f"\n✅ Done. {len(passed)}/{len(results)} alphas passed. "
          f"({elapsed / 60:.1f} min total)")


if __name__ == "__main__":
    main()