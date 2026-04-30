# src/simulate.py
"""
WorldQuant Brain simulation engine.

Brain enforces server-side concurrency limits — Python threading is not needed.
Instead we match the API's submission model:

  single mode  — submit up to 3 payloads one-by-one, collect their Location
                 URLs, then poll all 3 before moving to the next batch of 3.

  batch mode   — submit up to 10 payloads as a JSON *list* in one POST
                 (Brain returns one Location URL for the whole group).
                 Submit up to 8 such groups before polling them all.

Public API
----------
simulate_alphas(sess, alpha_list, *, mode="auto", save_interval=10,
                authenticate_callback=None)
    -> (results: list[dict], breaker_info: dict | None)
"""

from __future__ import annotations

import datetime
import json
import os
import sqlite3
import time
from itertools import islice
from typing import Any

import pandas as pd
import requests


# ── Constants ─────────────────────────────────────────────────────────────────

BRAIN_API        = "https://api.worldquantbrain.com"
SINGLE_WINDOW    = 3   # max simultaneous single-simulation tasks
BATCH_MAX_TASKS  = 10  # max payloads per batch POST
BATCH_MAX_GROUPS = 8   # max batch groups submitted before polling
SESSION_TTL      = 3.5 * 3600  # seconds before proactive token refresh


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_alpha_quality(fitness: float | None, delay: int = 1) -> str:
    if fitness is None:
        return "Unknown"
    try:
        f, d = float(fitness), int(delay)
    except (ValueError, TypeError):
        return "Unknown"
    thresholds = (3.25, 2.60, 1.95, 1.30) if d == 0 else (2.50, 2.00, 1.50, 1.00)
    labels = ("Spectacular", "Excellent", "Good", "Average", "Needs Improvement")
    for threshold, label in zip(thresholds, labels):
        if f > threshold:
            return label
    return labels[-1]


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def _chunk(lst: list, size: int):
    it = iter(lst)
    while chunk := list(islice(it, size)):
        yield chunk


# ── Session helpers ───────────────────────────────────────────────────────────

def _maybe_refresh(sess, session_start: float, authenticate_callback) -> tuple:
    """Proactively refresh session if TTL exceeded. Returns (sess, session_start)."""
    if authenticate_callback and (time.time() - session_start > SESSION_TTL):
        print("🔄 Session TTL reached — re-authenticating...")
        new = authenticate_callback()
        if new:
            print("✅ Re-authenticated.")
            return new, time.time()
        print("❌ Re-authentication failed.")
    return sess, session_start


def _reauth_on_401(response: requests.Response, sess, authenticate_callback,
                   session_start: float) -> tuple:
    """Re-authenticate if we got a 401. Returns (sess, session_start)."""
    if response.status_code == 401 and authenticate_callback:
        print("🔐 401 — re-authenticating...")
        new = authenticate_callback()
        if new:
            print("✅ Re-authenticated.")
            return new, time.time()
        print("❌ Re-authentication failed.")
    return sess, session_start


def _post_simulation(sess, payload, authenticate_callback,
                     session_start: float) -> tuple[requests.Response, Any, float]:
    """
    POST to /simulations with retry on 401 and 429.
    payload may be a dict (single) or a list (batch).
    Returns (response, sess, session_start).
    """
    try:
        resp = sess.post(f"{BRAIN_API}/simulations", json=payload)
    except requests.exceptions.ProxyError as exc:
        print(f"   ⚠️  Proxy error: {exc}. Retrying in 30s…")
        time.sleep(30)
        resp = sess.post(f"{BRAIN_API}/simulations", json=payload)

    if resp.status_code == 401:
        sess, session_start = _reauth_on_401(
            resp, sess, authenticate_callback, session_start)
        resp = sess.post(f"{BRAIN_API}/simulations", json=payload)

    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 60))
        print(f"   ⏳ Rate-limited — waiting {wait}s…")
        time.sleep(wait)
        resp = sess.post(f"{BRAIN_API}/simulations", json=payload)

    return resp, sess, session_start


def _poll_until_done(sess, url: str, authenticate_callback,
                     session_start: float) -> tuple[dict, Any, float]:
    """Poll a progress URL until Retry-After is absent. Returns (json, sess, session_start)."""
    while True:
        resp = sess.get(url)
        if resp.status_code == 401:
            sess, session_start = _reauth_on_401(
                resp, sess, authenticate_callback, session_start)
            resp = sess.get(url)
        retry_after = float(resp.headers.get("Retry-After", 0))
        if retry_after == 0:
            return resp.json(), sess, session_start
        time.sleep(retry_after)


# ── Metrics & evaluation ──────────────────────────────────────────────────────

def _fetch_metrics(sess, alpha_id: str, default_delay: int,
                   authenticate_callback, session_start: float
                   ) -> tuple[dict[str, Any], Any, float]:
    resp = sess.get(f"{BRAIN_API}/alphas/{alpha_id}")
    if resp.status_code == 401:
        sess, session_start = _reauth_on_401(
            resp, sess, authenticate_callback, session_start)
        resp = sess.get(f"{BRAIN_API}/alphas/{alpha_id}")

    if resp.status_code != 200:
        return {}, sess, session_start

    ad        = resp.json()
    is_stats  = ad.get("is", {})
    srv_delay = ad.get("settings", {}).get("delay", default_delay)
    checks    = is_stats.get("checks", [])

    return {
        "status":      ad.get("status", ""),
        "is_sharpe":   is_stats.get("sharpe"),
        "is_fitness":  is_stats.get("fitness"),
        "is_turnover": is_stats.get("turnover"),
        "is_margin":   is_stats.get("margin"),
        "is_quality":  get_alpha_quality(is_stats.get("fitness"), srv_delay),
        "pass_count":  sum(1 for c in checks if c.get("result") == "PASS"),
        "fail_count":  sum(1 for c in checks if c.get("result") == "FAIL"),
    }, sess, session_start


def _eval_status(metrics: dict) -> str:
    if metrics.get("fail_count", 0) == 0 and metrics.get("pass_count", 0) > 0:
        return "Success"
    if metrics.get("status", "") in ("ERROR", "FAIL"):
        return "Failed"
    return "Failed (No Checks Passed)"


def _make_base_row(alpha: dict) -> dict:
    now = datetime.datetime.now()
    return {
        "expression":    alpha["regular"],
        "group_label":   alpha.get("group_label", "unlabeled"),
        "alpha_id":      None,
        "settings_json": json.dumps(alpha.get("settings", {})),
        "is_quality":    "Unknown",
        "elapsed_time":  0,
        "test_date":     now.strftime("%Y-%m-%d"),
        "test_time":     now.strftime("%H:%M:%S"),
    }


# ── Persistence ───────────────────────────────────────────────────────────────

def _ensure_db_columns(conn: sqlite3.Connection, df: pd.DataFrame):
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='alphas_tested'"
    )
    if cur.fetchone() is None:
        return
    cur.execute("PRAGMA table_info(alphas_tested)")
    existing = {row[1] for row in cur.fetchall()}
    for col in df.columns:
        if col not in existing:
            dtype    = df[col].dtype
            sql_type = ("INTEGER" if pd.api.types.is_integer_dtype(dtype)
                        else "REAL" if pd.api.types.is_float_dtype(dtype)
                        else "TEXT")
            print(f"   🛠  DB: adding column '{col}' ({sql_type})")
            cur.execute(f"ALTER TABLE alphas_tested ADD COLUMN {col} {sql_type}")
    conn.commit()


def _flush_to_storage(batch: list[dict], results_dir: str):
    if not batch:
        return
    os.makedirs(results_dir, exist_ok=True)
    df = pd.DataFrame(batch)

    db_path = os.path.join(results_dir, "alpha_history.db")
    with sqlite3.connect(db_path) as conn:
        _ensure_db_columns(conn, df)
        df.to_sql("alphas_tested", conn, if_exists="append", index=False)

    csv_path = os.path.join(results_dir, "simulation_results.csv")
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            existing_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
            new_cols      = [c for c in df.columns if c not in existing_cols]
            if new_cols:
                pd.concat([pd.read_csv(csv_path), df], ignore_index=True).to_csv(
                    csv_path, index=False)
            else:
                df.reindex(columns=existing_cols).to_csv(
                    csv_path, mode="a", header=False, index=False)
        except pd.errors.EmptyDataError:
            df.to_csv(csv_path, index=False)
    else:
        df.to_csv(csv_path, index=False)

    print(f"   💾 Saved {len(batch)} result(s) to DB + CSV.")


# ── Single mode ───────────────────────────────────────────────────────────────

def _simulate_single(sess, alpha_list: list[dict], save_interval: int,
                     results_dir: str, authenticate_callback
                     ) -> tuple[list[dict], dict | None]:
    """
    Submit up to SINGLE_WINDOW payloads one-by-one, collect their Location
    URLs, then poll all of them before moving to the next window.
    Brain handles the 3-way parallelism server-side.
    """
    total         = len(alpha_list)
    results       = []
    pending_save  = []
    consec_errors = 0
    error_log     = []
    breaker_info  = None
    session_start = time.time()

    print(f"\n[single mode]  {total} alpha(s)  |  window={SINGLE_WINDOW}\n")

    try:
        for window_start in range(0, total, SINGLE_WINDOW):
            window = alpha_list[window_start: window_start + SINGLE_WINDOW]

            # ── Submit all in window ─────────────────────────────────────────
            pending: list[tuple[dict, str, float]] = []  # (alpha, location_url, submit_time)
            for alpha in window:
                sess, session_start = _maybe_refresh(
                    sess, session_start, authenticate_callback)

                expression  = alpha["regular"]
                group_label = alpha.get("group_label", "unlabeled")
                api_payload = {k: v for k, v in alpha.items() if k != "group_label"}
                idx         = window_start + len(pending) + 1
                print(f"📤 [{idx}/{total}] {group_label}  |  {expression}")

                resp, sess, session_start = _post_simulation(
                    sess, api_payload, authenticate_callback, session_start)

                if "Location" not in resp.headers:
                    err = resp.text[:200]
                    print(f"   ❌ Submit error ({resp.status_code}): {err}")
                    base = _make_base_row(alpha)
                    row  = {**base, "status": "FAILED_SUBMIT", "error": err}
                    results.append(row)
                    pending_save.append(row)
                    consec_errors += 1
                    error_log.append({"expression": expression, "error": err})
                    if consec_errors >= 3:
                        print("\n🚨 CIRCUIT BREAKER: 3 consecutive errors. Stopping.")
                        breaker_info = {"triggered": True, "log": error_log[-3:]}
                        return results, breaker_info
                    continue

                pending.append((alpha, resp.headers["Location"], time.time()))

            print(f"     Window submitted ({len(pending)} task(s)). Polling…")

            # ── Poll all in window ───────────────────────────────────────────
            for alpha, location_url, submit_time in pending:
                expression  = alpha["regular"]
                group_label = alpha.get("group_label", "unlabeled")
                delay       = alpha.get("settings", {}).get("delay", 1)

                result_json, sess, session_start = _poll_until_done(
                    sess, location_url, authenticate_callback, session_start)

                alpha_id   = result_json.get("alpha")
                api_status = result_json.get("status", "SUCCESS")
                base       = _make_base_row(alpha)

                if api_status in ("ERROR", "FAIL") and not alpha_id:
                    err = result_json.get("message", "Unknown runtime error")
                    print(f"   ❌ Runtime error '{expression}': {err}")
                    row = {**base, "status": "FAILED_RUNTIME", "error": err}
                    results.append(row)
                    pending_save.append(row)
                    consec_errors += 1
                    error_log.append({"expression": expression, "error": err})
                    if consec_errors >= 3:
                        print("\n🚨 CIRCUIT BREAKER: 3 consecutive errors. Stopping.")
                        breaker_info = {"triggered": True, "log": error_log[-3:]}
                        return results, breaker_info
                    continue

                consec_errors = 0

                metrics, sess, session_start = (
                    _fetch_metrics(sess, alpha_id, delay,
                                   authenticate_callback, session_start)
                    if alpha_id else ({}, sess, session_start)
                )

                eval_status = _eval_status(metrics)
                elapsed     = time.time() - submit_time
                icon        = "✅" if eval_status == "Success" else "❌"
                print(f"   {icon} {alpha_id or 'N/A'}  |  "
                      f"{metrics.get('is_quality', '?')}  | "
                      f"sharpe: {metrics.get('is_sharpe', '?')}  |  "
                      f"fitness: {metrics.get('is_fitness', '?')}  |  "
                      f"turnover: {metrics.get('is_turnover', '?')}  |  "
                      f"{metrics.get('pass_count', 0)} passed, {metrics.get('fail_count', 0)} failed checks  |  "
                      f"{format_time(elapsed)}")

                row = {
                    **base,
                    "alpha_id":     alpha_id,
                    "status":       eval_status,
                    "is_sharpe":    metrics.get("is_sharpe"),
                    "is_fitness":   metrics.get("is_fitness"),
                    "is_turnover":  metrics.get("is_turnover"),
                    "is_margin":    metrics.get("is_margin"),
                    "is_quality":   metrics.get("is_quality", "Unknown"),
                    "elapsed_time": elapsed,
                    "pass_count":   metrics.get("pass_count", 0),
                    "fail_count":   metrics.get("fail_count", 0),
                }
                results.append(row)
                pending_save.append(row)

            if len(results) % save_interval < SINGLE_WINDOW:
                _flush_to_storage(pending_save, results_dir)
                pending_save.clear()

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted.")

    finally:
        _flush_to_storage(pending_save, results_dir)

    return results, breaker_info


# ── Batch mode ────────────────────────────────────────────────────────────────

def _simulate_batch(sess, alpha_list: list[dict], save_interval: int,
                    results_dir: str, authenticate_callback
                    ) -> tuple[list[dict], dict | None]:
    """
    Brain batch API: POST a JSON list of up to BATCH_MAX_TASKS payloads in
    one request.  Brain returns one Location URL for the whole group; polling
    that URL yields a 'children' list of individual simulation IDs.

    We submit up to BATCH_MAX_GROUPS groups before polling them all.
    """
    tasks         = list(_chunk(alpha_list, BATCH_MAX_TASKS))
    total_tasks   = len(tasks)
    total_alphas  = len(alpha_list)
    results       = []
    pending_save  = []
    session_start = time.time()

    print(f"\n[batch mode]  {total_alphas} alpha(s)  |  "
          f"{total_tasks} group(s) of ≤{BATCH_MAX_TASKS}  |  "
          f"outer window={BATCH_MAX_GROUPS}\n")

    def _resolve_group(group_result_json: dict, group: list[dict],
                       group_submit_time: float) -> list[dict]:
        """Fetch metrics for each child simulation returned by a batch POST."""
        nonlocal sess, session_start

        children      = group_result_json.get("children", [])
        group_results = []

        for alpha, child_id in zip(group, children):
            group_label = alpha.get("group_label", "unlabeled")
            delay       = alpha.get("settings", {}).get("delay", 1)
            base        = _make_base_row(alpha)

            child_resp = sess.get(f"{BRAIN_API}/simulations/{child_id}")
            if child_resp.status_code == 401:
                sess, session_start = _reauth_on_401(
                    child_resp, sess, authenticate_callback, session_start)
                child_resp = sess.get(f"{BRAIN_API}/simulations/{child_id}")

            child_json = child_resp.json()
            alpha_id   = child_json.get("alpha")
            api_status = child_json.get("status", "SUCCESS")

            if api_status in ("ERROR", "FAIL") and not alpha_id:
                err = child_json.get("message", "Unknown runtime error")
                print(f"   ❌ Runtime error '{alpha['regular']}': {err}")
                group_results.append({**base, "status": "FAILED_RUNTIME", "error": err})
                continue

            metrics, sess, session_start = (
                _fetch_metrics(sess, alpha_id, delay,
                               authenticate_callback, session_start)
                if alpha_id else ({}, sess, session_start)
            )
            eval_status = _eval_status(metrics)
            elapsed     = time.time() - group_submit_time
            icon        = "✅" if eval_status == "Success" else "❌"
            print(f"   {icon} {alpha_id or 'N/A'}  |  {group_label}  |  "
                  f"{metrics.get('is_quality', '?')}  |  {format_time(elapsed)}")

            group_results.append({
                **base,
                "alpha_id":     alpha_id,
                "status":       eval_status,
                "is_sharpe":    metrics.get("is_sharpe"),
                "is_fitness":   metrics.get("is_fitness"),
                "is_turnover":  metrics.get("is_turnover"),
                "is_margin":    metrics.get("is_margin"),
                "is_quality":   metrics.get("is_quality", "Unknown"),
                "elapsed_time": elapsed,
                "pass_count":   metrics.get("pass_count", 0),
                "fail_count":   metrics.get("fail_count", 0),
            })

        return group_results

    try:
        for outer_start in range(0, total_tasks, BATCH_MAX_GROUPS):
            outer_window = tasks[outer_start: outer_start + BATCH_MAX_GROUPS]

            # ── Submit all groups in this outer window ───────────────────────
            submitted: list[tuple[list[dict], str, float]] = []
            for g_idx, group in enumerate(outer_window):
                sess, session_start = _maybe_refresh(
                    sess, session_start, authenticate_callback)

                api_payloads = [
                    {k: v for k, v in a.items() if k != "group_label"}
                    for a in group
                ]
                abs_idx = outer_start + g_idx + 1
                print(f"📦 Submitting group {abs_idx}/{total_tasks} "
                      f"({len(group)} alpha(s))…")

                resp, sess, session_start = _post_simulation(
                    sess, api_payloads, authenticate_callback, session_start)

                if "Location" not in resp.headers:
                    print(f"   ❌ Group submit error ({resp.status_code}): "
                          f"{resp.text[:200]}")
                    for alpha in group:
                        results.append({
                            **_make_base_row(alpha),
                            "status": "FAILED_SUBMIT",
                            "error":  resp.text[:200],
                        })
                    continue

                submitted.append((group, resp.headers["Location"], time.time()))

            print(f"   ✅ {len(submitted)} group(s) submitted. Polling…\n")

            # ── Poll all submitted groups ────────────────────────────────────
            for group, location_url, submit_time in submitted:
                group_json, sess, session_start = _poll_until_done(
                    sess, location_url, authenticate_callback, session_start)

                group_rows = _resolve_group(group_json, group, submit_time)
                results.extend(group_rows)
                pending_save.extend(group_rows)

            if len(results) % save_interval < BATCH_MAX_GROUPS * BATCH_MAX_TASKS:
                _flush_to_storage(pending_save, results_dir)
                pending_save.clear()

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted.")

    finally:
        _flush_to_storage(pending_save, results_dir)

    return results, None


# ── Public entry point ────────────────────────────────────────────────────────

def simulate_alphas(
    sess,
    alpha_list: list[dict],
    *,
    mode: str = "auto",
    save_interval: int = 10,
    authenticate_callback=None,
) -> tuple[list[dict], dict | None]:
    """
    Run Brain simulations for every payload in alpha_list.

    Parameters
    ----------
    sess                  : authenticated requests.Session
    alpha_list            : list of alpha payload dicts (may include 'group_label')
    mode                  : "single" | "batch" | "auto"
                            auto → batch if len > SINGLE_WINDOW, else single
    save_interval         : flush results to disk every N completions
    authenticate_callback : called with no args; must return new session or None
    """
    if not alpha_list:
        return [], None

    results_dir = os.path.join(os.path.dirname(__file__), "..", "results")

    resolved = mode
    if mode == "auto":
        resolved = "batch" if len(alpha_list) > SINGLE_WINDOW else "single"
    print(f"   ⚙️  Simulation mode: {resolved}")

    if resolved == "batch":
        return _simulate_batch(
            sess, alpha_list, save_interval, results_dir, authenticate_callback)
    else:
        return _simulate_single(
            sess, alpha_list, save_interval, results_dir, authenticate_callback)