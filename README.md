
# WorldQuant Brain — Automated Alpha Testing Pipeline

This project automates the full workflow of fetching data fields, injecting them into alpha formula templates, submitting simulations to WorldQuant Brain, and generating a structured backtest report — with zero manual interaction between runs.

---

## Project Structure

```
project/
├── main.py                  # Entry point — run this to start everything
├── config.json              # All configuration lives here
├── .env                     # Your Brain credentials (never commit this)
├── requirements.txt
├── src/
│   ├── auth.py              # Handles Brain API authentication
│   ├── data.py              # Fetches and caches datafields from Brain API
│   ├── alphas.py            # Filters fields and generates alpha payloads
│   ├── simulate.py          # Submits alphas, polls results, saves checkpoints
│   └── report.py            # Generates Markdown backtest report
├── data/
│   └── datasets/            # Cached SQLite files (auto-created, one per dataset)
├── results/
│   ├── alpha_history.db     # Full SQLite history of every alpha ever tested
│   └── simulation_results.csv  # CSV mirror for easy spreadsheet viewing
└── reports/
    └── Backtest_Report_<timestamp>.md   # Auto-generated after each run
```

---

## Quickstart

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Set up credentials**

Create a `.env` file in the project root:
```
BRAIN_USERNAME=your_email@domain.com
BRAIN_PASSWORD=your_password
```

**3. Configure your test in `config.json`** (see full guide below)

**4. Run**
```bash
python3 main.py
```

The pipeline runs all alpha groups end-to-end and saves a full report to `reports/`.

---

## How the Pipeline Works

Each run follows five sequential steps printed in the terminal:

```
[1/5] Load config
[2/5] Authenticate with Brain API
[3/5] Fetch datafields (uses local cache if available)
[4/5] Generate alpha payloads from groups × templates × fields
[5/5] Submit simulations, poll results, save checkpoints
      → Generate Markdown report
```

---

## Configuration Guide (`config.json`)

The entire test logic is controlled by `config.json`. You never need to touch any Python file to change what you test.

### Top-level structure

```json
{
    "searchScope": { ... },
    "alpha_groups": [ ... ],
    "save_interval": 10
}
```

- `searchScope` — defines the universe used when fetching datafields from the API
- `alpha_groups` — the core of the config; each group is a self-contained test unit
- `save_interval` — how many alphas to run before writing a checkpoint to disk (default: 10)

---

### `searchScope`

```json
"searchScope": {
    "region": "USA",
    "delay": "1",
    "universe": "TOP3000",
    "instrumentType": "EQUITY"
}
```

This controls which datafield catalog is queried. Keep `delay` as a string (`"1"`).

---

### `alpha_groups` — the main concept

Previously the config had a single flat `alpha_templates` list and one global `simulation_settings`. This meant testing different formula types required stopping the run, manually editing the config, and restarting.

Now each **alpha group** is a fully independent test unit with its own:
- dataset filters (which fields to loop over)
- alpha templates (what formula structure to apply)
- simulation settings (neutralization, decay, universe, etc.)

You define as many groups as you want. The pipeline runs them all in sequence automatically.

**Minimal group example:**
```json
{
    "label": "my_group_name",
    "datasets": [ { "id": "pv1", ... } ],
    "alpha_templates": [
        "group_rank(ts_mean({datafield}, 20) - ts_mean({datafield}, 60), sector)"
    ],
    "simulation_settings": {
        "instrumentType": "EQUITY",
        "region": "USA",
        "universe": "TOP3000",
        "delay": 1,
        "decay": 0,
        "neutralization": "SECTOR",
        "truncation": 0.08,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "ON",
        "language": "FASTEXPR",
        "visualization": false
    }
}
```

**How payloads are generated per group:**

For each group, the pipeline does a nested loop:

```
for each dataset in group.datasets:
    filter fields according to filter rules
    for each surviving field:
        for each template in group.alpha_templates:
            generate one alpha payload
```

So a group with 3 templates and 2 matching fields produces 6 alpha payloads, all sharing the same `simulation_settings`.

---

### Dataset filters inside each group

Each dataset entry supports the following optional filters. Set any to `null` to skip it.

```json
{
    "id": "pv1",
    "type_filter": "MATRIX",

    "alpha_count_filter":   [10000, null],
    "data_coverage_filter": [null, null],
    "coverage_filter":      [0.4, null],
    "user_count_filter":    [null, null],

    "field_name_patterns":          ["^close$", "^vwap$"],
    "field_name_exclude_patterns":  null,

    "description_patterns":         ["operating income", "gross profit"],
    "description_exclude_patterns": ["per share", "tax", "deferred"]
}
```

**Filter explanations:**

| Filter | What it does |
|--------|-------------|
| `type_filter` | Keep only fields of this type, usually `"MATRIX"` |
| `alpha_count_filter` | `[min, max]` range on how many existing alphas use this field. Use `[10000, null]` to avoid completely untested fields |
| `coverage_filter` | `[min, max]` on data coverage ratio. `[0.4, null]` means at least 40% of stocks have this field populated |
| `field_name_patterns` | Regex whitelist on field ID. A field is kept if its ID matches **any** pattern |
| `field_name_exclude_patterns` | Regex blacklist on field ID. A field is dropped if its ID matches **any** pattern |
| `description_patterns` | Regex whitelist on the field's human-readable description. More semantic than name matching — use this for fundamental data categories |
| `description_exclude_patterns` | Regex blacklist on description. Use this to remove noise fields that slip through the whitelist (e.g. tax adjustments, per-share variants) |

**Choosing between name patterns vs description patterns:**

- Use `field_name_patterns` for price-volume data where field names are clean and predictable (`close`, `vwap`, `volume`, `open`)
- Use `description_patterns` + `description_exclude_patterns` for fundamental data (fundamental6, fundamental2) where there are hundreds of fields and the description is more reliable than the cryptic field ID

**Regex tips:**
- `^close$` matches exactly the field named `close` (anchors prevent partial matches)
- `^implied_volatility_(call|put)_(30|60|90)$` matches specific option tenor fields
- All patterns are case-insensitive

---

### `simulation_settings` per group

Each group has its own `simulation_settings`. This is what gets sent to the Brain API for every alpha in that group.

```json
"simulation_settings": {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 0,
    "neutralization": "SECTOR",
    "truncation": 0.08,
    "pasteurization": "ON",
    "unitHandling": "VERIFY",
    "nanHandling": "ON",
    "language": "FASTEXPR",
    "visualization": false
}
```

**Key parameter guidance:**

| Parameter | Recommended | Notes |
|-----------|-------------|-------|
| `universe` | `TOP3000` for discovery, `TOP500` for robustness checks | Start broad, validate narrow |
| `delay` | `1` | Never use `0` — it introduces look-ahead bias |
| `decay` | `0` for fundamental data, `3–5` for sentiment/social data | Decay smooths the signal over time; quarterly data doesn't need it |
| `neutralization` | `SECTOR` for price-volume alphas, `INDUSTRY` for fundamental alphas | Match this to the grouping operator used inside your formula |
| `truncation` | `0.08` | Maximum single-stock weight; keep at 0.08 unless you have a specific reason |
| `pasteurization` | `ON` | Filters out illiquid and low-quality stocks automatically; always keep ON |

> ⚠️ All string values in `simulation_settings` must be **UPPERCASE**. The Brain API rejects lowercase variants silently. Use `"SECTOR"` not `"sector"`, `"ON"` not `"on"`.

---

### Alpha templates and the `{datafield}` placeholder

Each template string must contain exactly one `{datafield}` placeholder. The pipeline replaces it with each field ID that survives the dataset filters.

**Single-line template:**
```json
"alpha_templates": [
    "group_rank(ts_mean({datafield}, 20) - ts_mean({datafield}, 60), sector)"
]
```

**Multi-line template (use semicolons as statement separators):**
```json
"alpha_templates": [
    "my_group = market; my_group2 = bucket(rank(cap), range='0,1,0.1'); alpha = rank(group_rank(ts_decay_linear(volume / ts_sum(volume, 252), 10), my_group) * group_rank(ts_rank(vec_avg({datafield})), my_group) * group_rank(-ts_delta(close, 5), my_group)); trade_when(volume > adv20, group_neutralize(alpha, my_group2), -1)"
]
```

**Multiple templates in one group** — all share the same dataset filters and simulation settings:
```json
"alpha_templates": [
    "group_rank(ts_rank({datafield}, 20), sector)",
    "group_rank(ts_delta(ts_mean({datafield}, 5), 10), sector)",
    "-group_rank(ts_corr({datafield}, volume, 15), sector)"
]
```

---

### Known formula constraints

**Do not nest `group_rank` inside another `group_rank`.** Brain's FASTEXPR engine tracks unit types strictly. A field that has already been through a group operation carries a `Group` unit tag and cannot be passed into another group operator directly. This causes the error:

```
Incompatible unit for input of "group_rank", expected "Unit[CSPrice:1,CSShare:1]",
found "Unit[CSPrice:1,CSShare:1,Group:1]"
```

If your formula needs multiple group operations, store intermediate results in named variables using semicolon-separated statements (see multi-line template example above).

---

## Adding a new group of alphas

To add a new test group, append a new object to the `alpha_groups` array in `config.json`:

```json
{
    "label": "G6_my_new_idea",
    "datasets": [
        {
            "id": "pv1",
            "type_filter": "MATRIX",
            "alpha_count_filter": [null, null],
            "data_coverage_filter": [null, null],
            "coverage_filter": [null, null],
            "user_count_filter": [null, null],
            "field_name_patterns": ["^close$"],
            "field_name_exclude_patterns": null,
            "description_patterns": null,
            "description_exclude_patterns": null
        }
    ],
    "alpha_templates": [
        "group_rank(ts_zscore({datafield}, 20), sector)"
    ],
    "simulation_settings": {
        "instrumentType": "EQUITY",
        "region": "USA",
        "universe": "TOP3000",
        "delay": 1,
        "decay": 0,
        "neutralization": "SECTOR",
        "truncation": 0.08,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "ON",
        "language": "FASTEXPR",
        "visualization": false
    }
}
```

Then run `python3 main.py`. No Python files need to be touched.

---

## Output and reports

After each run, two outputs are produced:

**`results/alpha_history.db`** — SQLite database accumulating every alpha ever tested across all runs. Columns include: `expression`, `group_label`, `alpha_id`, `status`, `is_sharpe`, `is_fitness`, `is_turnover`, `is_margin`, `is_quality`, `elapsed_time`, `pass_count`, `fail_count`.

**`results/simulation_results.csv`** — CSV mirror of the same data for easy viewing in Excel or any spreadsheet tool.

**`reports/Backtest_Report_<timestamp>.md`** — Markdown report generated at the end of each run, containing:
- Configuration summary (which groups, templates, and datasets were tested)
- Overall pass rate and timing
- Premium alphas section (Good, Excellent, Spectacular only)
- Per-group breakdown with individual pass rates
- Full table of all passing alphas

---

## Local caching

Datafield metadata is cached automatically to `data/datasets/<dataset_id>_<region>_<universe>_d<delay>.db`. On subsequent runs, the pipeline loads from cache instead of making API calls, which saves significant time when testing many groups against the same dataset.

To force a fresh fetch (e.g. after a long gap or if the dataset has been updated), simply delete the relevant `.db` file from `data/datasets/` and rerun.

---

## Graceful interruption

Press `Ctrl+C` at any time during simulation. The pipeline catches the interrupt, saves all results collected so far to disk, and exits cleanly. No results are lost.

If 3 consecutive alpha submissions fail (syntax errors, API errors), the circuit breaker activates automatically, saves an error report to `reports/Error_Report_<timestamp>.md`, and stops the run to prevent wasting API quota.

























