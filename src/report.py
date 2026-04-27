# src/report.py
import os
import datetime


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{int(h)} hours {int(m)} minutes {int(s)} seconds"
    return f"{int(m)} minutes {int(s)} seconds"


def _alpha_table_row(p):
    expr    = p.get('expression', '').replace('|', '\\|')
    return (
        f"| {p.get('alpha_id','N/A')} "
        f"| `{expr}` "
        f"| **{p.get('is_quality','?')}** "
        f"| {p.get('is_sharpe','N/A')} "
        f"| {p.get('is_fitness','N/A')} "
        f"| {p.get('is_turnover','N/A')} "
        f"| {p.get('is_margin','N/A')} "
        f"| {format_time(p.get('elapsed_time',0))} |"
    )

TABLE_HEADER = (
    "| Alpha ID | Expression | Quality | Sharpe | Fitness "
    "| Turnover | Margin | Compute Time |\n"
    "|----------|------------|---------|--------|---------|"
    "----------|--------|--------------|\n"
)


def generate_markdown_report(results, config, overall_time, avg_time):
    reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
    os.makedirs(reports_dir, exist_ok=True)

    ts           = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path  = os.path.join(reports_dir, f"Backtest_Report_{ts}.md")

    passed       =[r for r in results if r.get('status') == 'Success']
    
    # Smart naming based on if batch yielded any successful tests
    prefix       = "Passed_Backtest_Report" if passed else "No_Pass_Backtest_Report"
    ts           = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path  = os.path.join(reports_dir, f"{prefix}_{ts}.md")

    total        = len(results)
    success_rate = (len(passed) / total * 100) if total > 0 else 0
    premium      = [p for p in passed
                    if p.get('is_quality') in ('Spectacular', 'Excellent', 'Good')]

    # ── Collect all unique group labels in order ─────────────────────────────
    seen   = []
    groups = []
    for r in results:
        lbl = r.get('group_label', 'unlabeled')
        if lbl not in seen:
            seen.append(lbl)
            groups.append(lbl)

    # ── Header ───────────────────────────────────────────────────────────────
    # Reconstruct templates display from alpha_groups if present, else fallback
    alpha_groups = config.get('alpha_groups', [])
    if alpha_groups:
        templates_block = ""
        for g in alpha_groups:
            lbl = g.get('label', 'unlabeled')
            templates_block += f"\n**Group: {lbl}**\n"
            for t in g.get('alpha_templates', []):
                templates_block += f"- `{t}`\n"
    else:
        templates_block = "\n".join(
            f"- `{t}`" for t in config.get('alpha_templates', []))

    datasets_block = "\n".join(
        f"- {ds['id']} (Type: {ds.get('type_filter','All')})"
        for ds in config.get('datasets', []))

    sim = config.get('simulation_settings', {})

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

    # ── Premium section ───────────────────────────────────────────────────────
    if premium:
        md += "## 🌟 Premium Alphas (Good & Above)\n"
        md += TABLE_HEADER
        for p in premium:
            md += _alpha_table_row(p) + "\n"
        md += "\n---\n\n"

    # ── Per-group breakdown ───────────────────────────────────────────────────
    md += "## 📊 Results by Group\n\n"
    for lbl in groups:
        group_results = [r for r in results if r.get('group_label') == lbl]
        group_passed  = [r for r in group_results if r.get('status') == 'Success']
        rate = (len(group_passed) / len(group_results) * 100) if group_results else 0

        md += f"### Group: `{lbl}`\n"
        md += (f"Tested: {len(group_results)}  |  "
               f"Passed: {len(group_passed)}  |  "
               f"Pass rate: {rate:.1f}%\n\n")

        if group_passed:
            md += TABLE_HEADER
            for p in group_passed:
                md += _alpha_table_row(p) + "\n"
        else:
            md += "_No alphas passed in this group._\n"
        md += "\n"

    # ── All passed ───────────────────────────────────────────────────────────
    if passed:
        md += "---\n\n## ✅ All Passed Alphas\n"
        md += TABLE_HEADER
        for p in passed:
            md += _alpha_table_row(p) + "\n"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(md)

    print(f"\n📄 Report saved: {report_path}")
    return report_path


def generate_error_report(breaker_info, overall_time):
    reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
    os.makedirs(reports_dir, exist_ok=True)

    ts          = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"Error_Report_{ts}.md")

    error_section = ""
    for i, entry in enumerate(breaker_info.get("log", []), 1):
        error_section += (
            f"**Failed Formula {i}:**\n```\n{entry.get('expression','')}\n```\n"
            f"**Error:** {entry.get('error','')}\n\n---\n\n"
        )

    md = f"""# 🚨 Alpha Error Report
**Date:** {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Time before break:** {format_time(overall_time)}

## ⚠️ Circuit Breaker Triggered
3 consecutive errors were detected. Simulation halted.

## Failure Log
{error_section}
"""
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(md)

    print(f"\n🚨 Error report saved: {report_path}")
    return report_path