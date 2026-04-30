# src/report.py
import os
import datetime


# ── Formatting ────────────────────────────────────────────────────────────────

def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h} hours {m} minutes {s} seconds" if h else f"{m} minutes {s} seconds"


# ── Table helpers ─────────────────────────────────────────────────────────────

_TABLE_HEADER = (
    "| Alpha ID | Expression | Quality | Sharpe | Fitness "
    "| Turnover | Margin | Compute Time |\n"
    "|----------|------------|---------|--------|---------|"
    "----------|--------|--------------|\n"
)


def _table_row(p: dict) -> str:
    expr = p.get("expression", "").replace("|", "\\|")
    return (
        f"| {p.get('alpha_id', 'N/A')} "
        f"| `{expr}` "
        f"| **{p.get('is_quality', '?')}** "
        f"| {p.get('is_sharpe', 'N/A')} "
        f"| {p.get('is_fitness', 'N/A')} "
        f"| {p.get('is_turnover', 'N/A')} "
        f"| {p.get('is_margin', 'N/A')} "
        f"| {format_time(p.get('elapsed_time', 0))} |"
    )


def _render_table(rows: list[dict]) -> str:
    if not rows:
        return "_No alphas passed._\n"
    return _TABLE_HEADER + "\n".join(_table_row(r) for r in rows) + "\n"


# ── Markdown report ───────────────────────────────────────────────────────────

def generate_markdown_report(
    results: list[dict],
    config: dict,
    overall_time: float,
    avg_time: float,
) -> str:
    reports_dir = os.path.join(os.path.dirname(__file__), "..", "reports")
    os.makedirs(reports_dir, exist_ok=True)

    passed  = [r for r in results if r.get("status") == "Success"]
    premium = [p for p in passed
               if p.get("is_quality") in ("Spectacular", "Excellent", "Good")]

    prefix      = "Passed_Backtest_Report" if passed else "No_Pass_Backtest_Report"
    ts          = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"{prefix}_{ts}.md")

    total        = len(results)
    success_rate = (len(passed) / total * 100) if total else 0

    # Templates block
    alpha_groups = config.get("alpha_groups", [])
    if alpha_groups:
        templates_block = ""
        for g in alpha_groups:
            templates_block += f"\n**Group: {g.get('label', 'unlabeled')}**\n"
            for t in g.get("alpha_templates", []):
                templates_block += f"- `{t}`\n"
    else:
        templates_block = "\n".join(
            f"- `{t}`" for t in config.get("alpha_templates", [])
        )

    datasets_block = "\n".join(
        f"- {ds['id']} (Type: {ds.get('type_filter', 'All')})"
        for ds in config.get("datasets", [])
    )

    sim = config.get("simulation_settings", {})

    md = f"""# WorldQuant Brain Alpha Backtest Report
**Date Generated:** {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## ⚙️ Configuration
### Alpha Templates Tested:
{templates_block}

### Datasets Used:
{datasets_block}

### Simulation Settings:
- **Delay:** {sim.get('delay', 'Unknown')}
- **Universe:** {sim.get('universe', 'Unknown')}
- **Neutralization:** {sim.get('neutralization', 'Unknown')}
- **Truncation:** {sim.get('truncation', 'Unknown')}

---

## ⏱️ Performance Summary
- **Total Alphas Tested:** {total}
- **Successfully Passed:** {len(passed)} ({success_rate:.2f}%)
- **Overall Time:** {format_time(overall_time)}
- **Avg Time / Alpha:** {format_time(avg_time)}

---
"""

    if premium:
        md += "## 🌟 Premium Alphas (Good & Above)\n"
        md += _render_table(premium)
        md += "\n---\n\n"

    # Per-group breakdown
    md += "## 📊 Results by Group\n\n"
    seen_groups: list[str] = []
    for r in results:
        lbl = r.get("group_label", "unlabeled")
        if lbl not in seen_groups:
            seen_groups.append(lbl)

    for lbl in seen_groups:
        group_all    = [r for r in results if r.get("group_label") == lbl]
        group_passed = [r for r in group_all  if r.get("status") == "Success"]
        rate = (len(group_passed) / len(group_all) * 100) if group_all else 0
        md += (
            f"### Group: `{lbl}`\n"
            f"Tested: {len(group_all)}  |  Passed: {len(group_passed)}  |  "
            f"Pass rate: {rate:.1f}%\n\n"
        )
        md += _render_table(group_passed)
        md += "\n"

    if passed:
        md += "---\n\n## ✅ All Passed Alphas\n"
        md += _render_table(passed)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\n📄 Report saved: {report_path}")
    return report_path


# ── Error report ──────────────────────────────────────────────────────────────

def generate_error_report(breaker_info: dict, overall_time: float) -> str:
    reports_dir = os.path.join(os.path.dirname(__file__), "..", "reports")
    os.makedirs(reports_dir, exist_ok=True)

    ts          = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"Error_Report_{ts}.md")

    error_section = "\n".join(
        f"**Failed Formula {i}:**\n```\n{e.get('expression', '')}\n```\n"
        f"**Error:** {e.get('error', '')}\n\n---"
        for i, e in enumerate(breaker_info.get("log", []), 1)
    )

    md = f"""# 🚨 Alpha Error Report
**Date:** {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Time before break:** {format_time(overall_time)}

## ⚠️ Circuit Breaker Triggered
3 consecutive errors were detected. Simulation halted.

## Failure Log
{error_section}
"""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\n🚨 Error report saved: {report_path}")
    return report_path