# src/simulate.py
import time
import os
import json
import sqlite3
import pandas as pd


def get_alpha_quality(fitness, delay=1):
    if fitness is None:
        return "Unknown"
    try:
        f = float(fitness)
        d = int(delay)
    except (ValueError, TypeError):
        return "Unknown"

    if d == 0:
        if f > 3.25:  return "Spectacular"
        elif f > 2.60: return "Excellent"
        elif f > 1.95: return "Good"
        elif f > 1.30: return "Average"
        else:          return "Needs Improvement"
    else:
        if f > 2.50:  return "Spectacular"
        elif f > 2.00: return "Excellent"
        elif f > 1.50: return "Good"
        elif f > 1.00: return "Average"
        else:          return "Needs Improvement"


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.2f} s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{int(h)}h {int(m)}min {int(s)}s"
    return f"{int(m)}min {int(s)}s"


def simulate_alphas(sess, alpha_list, save_interval=10, authenticate_callback=None):
    """
    Runs Brain simulations for every payload in alpha_list.

    Each payload may carry an optional 'group_label' key (added by generate_alphas)
    which is stored in results for per-group reporting.

    Returns (results, breaker_info).
    """
    results        = []
    batch_results  = []
    session_start  = time.time()
    consec_errors  = 0
    error_log      = []
    breaker_info   = None
    total          = len(alpha_list)

    # ── Storage helpers ──────────────────────────────────────────────────────
    def flush_to_storage(batch):
        if not batch:
            return
        results_dir = os.path.join(os.path.dirname(__file__), '..', 'results')
        os.makedirs(results_dir, exist_ok=True)

        df = pd.DataFrame(batch)

        # SQLite
        db_path = os.path.join(results_dir, 'alpha_history.db')
        conn = sqlite3.connect(db_path)
        df.to_sql('alphas_tested', conn, if_exists='append', index=False)
        conn.close()

        # CSV mirror
        csv_path = os.path.join(results_dir, 'simulation_results.csv')
        df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)

        print(f"   💾 CHECKPOINT: saved {len(batch)} results to DB + CSV.")

    # ── Main loop ────────────────────────────────────────────────────────────
    print(f"\nStarting simulation loop — {total} alpha(s) queued. "
          f"Checkpoint every {save_interval}.\n")

    try:
        for count, alpha in enumerate(alpha_list, 1):

            # Session refresh every 3.5 hours
            if authenticate_callback and (time.time() - session_start > 3.5 * 3600):
                print("🔄 Session timeout approaching — re-authenticating...")
                new_sess = authenticate_callback()
                if new_sess:
                    sess         = new_sess
                    session_start = time.time()
                    print("✅ Re-authentication successful.")

            expression  = alpha['regular']
            group_label = alpha.get('group_label', 'unlabeled')
            alpha_delay = alpha.get('settings', {}).get('delay', 1)

            print(f"🧪 [{count}/{total}] Group='{group_label}'  |  {expression}")

            api_payload = {k: v for k, v in alpha.items() if k != 'group_label'}

            alpha_start = time.time()

            sim_resp = sess.post(
                'https://api.worldquantbrain.com/simulations',
                json=api_payload,
            )

            try:
                # ── Submission error ─────────────────────────────────────────
                if 'Location' not in sim_resp.headers:
                    err_msg = sim_resp.text
                    print(f"   ❌ Submit error ({sim_resp.status_code}): {err_msg}")
                    results.append({
                        "expression":  expression,
                        "group_label": group_label,
                        "alpha_id":    None,
                        "status":      "FAILED_SUBMIT",
                    })
                    consec_errors += 1
                    error_log.append({"expression": expression, "error": err_msg})
                    if consec_errors >= 3:
                        print("\n🚨 CIRCUIT BREAKER: 3 consecutive submit errors. Stopping.")
                        breaker_info = {"triggered": True, "error_msg": err_msg,
                                        "log": error_log[-3:]}
                        break
                    continue

                # ── Poll until complete ──────────────────────────────────────
                progress_url = sim_resp.headers['Location']
                while True:
                    prog_resp    = sess.get(progress_url)
                    retry_after  = float(prog_resp.headers.get("Retry-After", 0))
                    if retry_after == 0:
                        break
                    time.sleep(retry_after)

                resp_json = prog_resp.json()
                alpha_id  = resp_json.get("alpha")
                status    = resp_json.get("status", "SUCCESS")

                # ── Runtime error ────────────────────────────────────────────
                if status in ("ERROR", "FAIL") and not alpha_id:
                    err_detail = resp_json.get("message", "Unknown runtime error")
                    print(f"   ❌ Runtime error: {err_detail}")
                    results.append({
                        "expression":  expression,
                        "group_label": group_label,
                        "alpha_id":    None,
                        "status":      "FAILED_RUNTIME",
                    })
                    consec_errors += 1
                    error_log.append({"expression": expression, "error": err_detail})
                    if consec_errors >= 3:
                        print("\n🚨 CIRCUIT BREAKER: 3 consecutive runtime errors. Stopping.")
                        breaker_info = {"triggered": True, "error_msg": err_detail,
                                        "log": error_log[-3:]}
                        break
                    continue

                # Successful submission — reset error counter
                consec_errors = 0
                error_log.clear()

                # ── Fetch IS metrics ─────────────────────────────────────────
                is_sharpe = is_fitness = is_turnover = is_margin = None
                is_quality = "Unknown"
                pass_count = fail_count = 0

                if alpha_id:
                    ar = sess.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                    if ar.status_code == 200:
                        ad          = ar.json()
                        status      = ad.get("status", status)
                        is_stats    = ad.get("is", {})
                        is_sharpe   = is_stats.get("sharpe")
                        is_fitness  = is_stats.get("fitness")
                        is_turnover = is_stats.get("turnover")
                        is_margin   = is_stats.get("margin")
                        srv_delay   = ad.get("settings", {}).get("delay", alpha_delay)
                        is_quality  = get_alpha_quality(is_fitness, srv_delay)
                        for chk in is_stats.get("checks", []):
                            if chk.get("result") == "PASS":
                                pass_count += 1
                            elif chk.get("result") == "FAIL":
                                fail_count += 1

                # ── Determine pass/fail ──────────────────────────────────────
                if fail_count == 0 and pass_count > 0:
                    eval_status = "Success"
                elif status in ("ERROR", "FAIL"):
                    eval_status = "Failed"
                elif status in ("UNSUBMITTED", "WARNING", "SUCCESS"):
                    eval_status = "Failed (No Checks Passed)"
                else:
                    eval_status = "Failed"

                elapsed = time.time() - alpha_start

                print("   " + "─" * 44)
                print(f"   Alpha ID  : {alpha_id}")
                print(f"   Group     : {group_label}")
                print(f"   Quality   : {is_quality}")
                print(f"   Checks    : {pass_count} PASS  {fail_count} FAIL")
                print(f"   Time      : {format_time(elapsed)}")
                print(f"   Result    : {'✅ PASSED' if eval_status == 'Success' else '❌ FAILED'}")
                print()

                row = {
                    "expression":  expression,
                    "group_label": group_label,
                    "alpha_id":    alpha_id,
                    "settings_json": json.dumps(alpha.get("settings", {})),
                    "status":      eval_status,
                    "is_sharpe":   is_sharpe,
                    "is_fitness":  is_fitness,
                    "is_turnover": is_turnover,
                    "is_margin":   is_margin,
                    "is_quality":  is_quality,
                    "elapsed_time": elapsed,
                    "pass_count":  pass_count,
                    "fail_count":  fail_count,
                }
                results.append(row)
                batch_results.append(row)

                if count % save_interval == 0:
                    flush_to_storage(batch_results)
                    batch_results.clear()

            except Exception as exc:
                print(f"   ⚠️  Unexpected exception: {exc}")
                results.append({
                    "expression":  expression,
                    "group_label": group_label,
                    "alpha_id":    None,
                    "status":      "FAILED_ERROR",
                })
                time.sleep(10)

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. Saving progress...")

    finally:
        if batch_results:
            flush_to_storage(batch_results)

    return results, breaker_info