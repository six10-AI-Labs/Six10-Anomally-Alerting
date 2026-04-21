# Six10 — Automated Anomaly Detection & Alerting System

A daily early-warning system that monitors key ecommerce metrics across all five Six10 brands on Amazon and sends a single HTML digest email when something looks wrong — before it becomes costly.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Project Structure](#project-structure)
3. [First-Time Setup](#first-time-setup)
4. [Running the Pipeline](#running-the-pipeline)
5. [Data Sources & Upload Guide](#data-sources--upload-guide)
6. [Configuration Reference](#configuration-reference)
7. [Threshold Tuning Guide](#threshold-tuning-guide)
8. [Alert Email Format](#alert-email-format)
9. [Known Limitations](#known-limitations)
10. [Troubleshooting](#troubleshooting)

---

## How It Works

Every day, after the Sellerise data is uploaded to Google Drive, a team member opens the Colab notebook and clicks **Run All**. The system:

1. Reads all historical data from Google Drive (all brands, all time)
2. Detects anomalies using two independent statistical baselines + hard business floors
3. Assigns a severity level (Critical / Warning / Watch) to each flagged metric
4. Builds a colour-coded HTML email sorted by severity and product tier
5. Sends the email to whoever runs the notebook

**An alert fires if either baseline triggers — not both.**

### The Two Baselines

**Baseline 1 — Rolling (14 days)**
Compares today's value against the average of the last 14 days for that ASIN. Catches sudden recent changes regardless of season.

**Baseline 2 — Year-over-Year (YoY)**
Compares today's value against a 7-day average centred on the same date last year. Catches seasonal underperformance — e.g. pool chemical sales being below where they were last spring. Only available for ASINs with 12+ months of data.

**On top of both baselines:** Hard absolute business floors override statistics for extreme cases (e.g. return rate > 5% is always Critical regardless of trend).

Each alert records what triggered it: `rolling / yoy / both / absolute_threshold`.

### Monitored Metrics

**Sellerise time-series (6 metrics — full rolling + YoY detection):**

| Metric | Bad direction | What it means |
|--------|--------------|---------------|
| conversion_rate | Drop | % of sessions that result in a purchase |
| return_rate | Spike | % of units returned |
| acos | Spike | Ad spend / ad revenue |
| tacos | Spike | Total ad spend / total revenue |
| sales | Drop | Units sold per day |
| margin | Drop | Profit margin % after all costs |

**Helium10 snapshots (4 metrics — rolling detection, activates after 7+ daily snapshots):**

| Metric | Bad direction | What it means |
|--------|--------------|---------------|
| keyword_avg_rank | Spike or drop | Average rank of tracked keywords (higher = worse) |
| review_rating | Spike or drop | Star rating — flags both drops (quality) and unusual spikes (fake reviews) |
| review_count | Spike or drop | Review count — flags drops and unusual spikes (incentivised reviews) |
| organic_top10_count | Drop | Number of keywords ranking in top 10 search results |

BSR statistical detection starts once 30+ days of daily Helium10 snapshots have accumulated (v2).

---

## Project Structure

```
anomaly_alerting/
│
├── config.py                        ← Single source of truth for ALL settings
├── main.py                          ← Entry point: run_pipeline()
├── backtest.py                      ← Threshold validation script (run locally)
│
├── ingestion/
│   └── load_data.py                 ← Reads all 4 data sources from Drive
│
├── preprocessing/
│   └── preprocess.py                ← Cleans, merges, assigns tiers
│
├── detection/
│   └── anomaly_detection.py         ← Dual-baseline detection + absolute overrides
│
├── alerting/
│   ├── alert_builder.py             ← Builds HTML email from detection output
│   └── email_sender.py              ← Sends email via Gmail API (service account)
│
├── data/                            ← Local test data only (not used in production)
│   ├── Sellerise/
│   ├── Returns/
│   ├── Inventory/
│   ├── helium10/
│   └── helium10_history/
│
└── Six10_Anomaly_Alerting.ipynb     ← Google Colab entry point
```

---

## First-Time Setup

### Step 1 — Copy the project to Google Drive

Upload the entire `anomaly_alerting/` folder to your Google Drive. Place it somewhere easy to find, for example:

```
My Drive / Six10 / anomaly_alerting /
```

### Step 2 — Set up service account for email and Drive access

The system uses a **Google service account** (no passwords, no 2-step verification needed):

1. **Create a service account** in Google Cloud Console:
   - Navigate to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable **Gmail API** and **Google Drive API**
   - Go to "Service Accounts" → Create a new service account
   - Download the JSON key file

2. **Share Drive folder with service account:**
   - Copy the service account email (e.g., `anomaly-alerts@project-id.iam.gserviceaccount.com`)
   - Share the Google Drive Data Feeds folder with this email (Viewer access)

3. **Place the JSON key file in the `anomaly_alerting/` folder** (same level as `main.py`)

4. **Update `.env` file:**

```bash
SERVICE_ACCOUNT_FILE=anomaly-alerting-b7f70323da39.json
SENDER_EMAIL=ai@six10ventures.com
RECIPIENT_EMAIL=shashank@six10ventures.com
```

⚠️ **The JSON key file and `.env` are in `.gitignore`** — they will never be pushed to GitHub. Keep them secure locally and in CI/CD as secrets.

### Step 3 — No config.py changes needed

All configuration is now handled via `.env` file and auto-detected based on your environment (Colab vs local).

> **Finding the mounted path:** After mounting Drive in Colab (Step 1 of the notebook), click the folder icon in the left sidebar. Navigate to your data folder, right-click it, and select **Copy path**. Paste that into config.py.

### Step 4 — Verify Google Drive folder structure

Each of the 4 Drive folders must follow this structure:

```
Sellerise/
  AquaDoc/
    sellerise_aquadoc_2026-03-23.xlsx
    sellerise_aquadoc_2026-03-24.xlsx
    ...
  Pureauty/
    ...
  (one subfolder per brand)

Returns/
  AquaDoc/
    returns_aquadoc_2026-03-23.csv
    ...
  Pureauty/
    ...

Inventory/
  AquaDoc/
    inventory_aquadoc_2026-03-23.csv
    ...

Helium10/
  AquaDoc/
    My Products2026-03-23.xlsx
    My Products2026-03-23 (1).xlsx    ← split file 2 (100 ASIN cap)
    My Products2026-03-24.xlsx
    ...
```

**Important:** File names must include the date. The system uses dates in filenames to know which file to load. Accepted formats: `2026-03-23` or `Mar 23, 2026`.

---

## Running the Pipeline

### In Google Colab (production — how the team runs it)

1. Open `Six10_Anomaly_Alerting.ipynb` in Google Colab
2. Click **Run All** (Runtime → Run all, or Ctrl+F9)
3. **Cell 1** — mounts your Google Drive. Approve when prompted.
4. **Cell 2** — adds the project folder to Python's path.
5. **Cell 3** — runs `run_pipeline()`:
   - Prompts you to enter your email address
   - Loads all data from Drive
   - Detects anomalies
   - Sends the alert email

The email arrives within about 2 minutes of running.

### Locally (for testing and development)

```bash
cd anomaly_alerting/
python main.py
```

For local runs, `DRIVE_FOLDERS` in config.py should point to the local `data/` folder paths, or use `backtest.py` which already points to local data automatically.

### Running the backtest (threshold validation)

```bash
cd anomaly_alerting/
python backtest.py
```

This runs detection across all historical local data and prints a full report — daily alert counts, breakdown by metric/tier/trigger. Use this to validate threshold changes before applying them.

---

## Data Sources & Upload Guide

### Daily upload checklist (by 10 AM)

| Source | Who uploads | Where | What file |
|--------|-------------|-------|-----------|
| Sellerise | Team member | `Sellerise / [Brand] /` | Product Summary by Day export (.xlsx) |
| Returns | Team member | `Returns / [Brand] /` | FBA Customer Returns Report (.csv) |
| Inventory | Team member | `Inventory / [Brand] /` | Restock/Inventory Report (.csv) |
| Helium10 | Team member | `Helium10 / AquaDoc /` | My Products export (.xlsx) — upload both split files |

### Data lag

- **Sellerise:** Data covers through T-2 (Amazon's 48-hour session/conversion delay). A file uploaded on March 25 will have data through March 23.
- **Inventory:** Current-day snapshot.
- **Helium10:** T-2 snapshot to match Sellerise.
- **Returns:** Historical cumulative export up to upload date.

The system automatically detects the latest available Helium10 date and loads all sources for that reference date.

### Historical data

Upload all historical files to the brand subfolders — not just the latest one. The system reads all files every run to compute rolling and YoY baselines. More history = more accurate baselines.

---

## Configuration Reference

All settings live in `config.py`. Nothing should be hardcoded anywhere else.

### Baseline settings

```python
ROLLING_WINDOW_DAYS = 14       # Days in the short-term rolling window
ROLLING_MIN_PERIODS = 7        # Min days of data before rolling baseline activates
YOY_WINDOW_DAYS = 7            # Days around same date last year to average
MIN_HISTORY_DAYS_FOR_YOY = 365 # ASIN needs 12+ months before YoY is used
```

### Z-score thresholds (rolling baseline)

```python
STD_DEV_THRESHOLDS = {
    "conversion_rate": {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "return_rate":     {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "acos":            {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "tacos":           {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "sales":           {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "margin":          {"watch": 2.0, "warning": 3.0, "critical": 4.5},  # raised — margin fluctuates daily with ad spend
    # Helium10 snapshot metrics
    "keyword_avg_rank":    {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "review_rating":       {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "review_count":        {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "organic_top10_count": {"watch": 1.5, "warning": 2.0, "critical": 3.0},
}
```

> **Why margin has higher thresholds than other metrics:** Margin changes day-to-day based on how ad spend is allocated, which creates natural daily noise. Lower bands (e.g. 1.5/2.0/3.0) would fire on normal variance. Use the YoY comparison and the absolute negative-margin floor to catch genuine margin problems instead.

### YoY deviation thresholds

```python
YOY_THRESHOLDS = {
    "conversion_rate": {"watch": 0.15, "warning": 0.25, "critical": 0.40},
    "return_rate":     {"watch": 0.20, "warning": 0.50, "critical": 1.00},
    "acos":            {"watch": 0.20, "warning": 0.30, "critical": 0.50},
    "tacos":           {"watch": 0.20, "warning": 0.30, "critical": 0.50},
    "sales":           {"watch": 0.15, "warning": 0.25, "critical": 0.40},
    "margin":          {"watch": 0.20, "warning": 0.35, "critical": 0.55},  # raised — see note below
}
```

> **Why margin YoY thresholds are higher:** A 15% relative YoY margin drop on a product with 15% margin is only a 2.25pp absolute difference — easily noise. The raised thresholds (20/35/55%) require a more meaningful compression before firing.

### YoY noise suppression floors

Two additional safeguards prevent meaningless YoY alerts:

```python
# Suppresses YoY alert if actual metric value is below this floor
# Prevents: "$0 vs $2.54 last year = 100% drop = CRITICAL" for off-season products
YOY_MIN_ABSOLUTE = {
    "sales": 50.0,    # Skip YoY sales alert if actual daily sales < $50
}

# Suppresses YoY alert if the historical baseline itself is near-zero
# Prevents: "TACoS 17.9% vs 0.1% last year = 17,800% higher = CRITICAL"
# when a product barely ran ads last year and has no meaningful baseline to compare against
YOY_MIN_BASELINE = {
    "acos":  0.05,    # Skip YoY ACoS alert if last year's ACoS was < 5%
    "tacos": 0.05,    # Skip YoY TACoS alert if last year's TACoS was < 5%
}
```

### Absolute business floors

```python
ABSOLUTE_THRESHOLDS = {
    "return_rate":        {"warning": 0.035, "critical": 0.05},  # warning raised from 0.02
    "conversion_drop_pct": {"warning": None, "critical": None},  # disabled — too noisy
    "margin":             {"critical_floor": 0.0, "hero_warning_floor": 0.10},
    "sales_zero":         {"critical_consecutive_days": 2, "hero_tiers": ["homerun", "triple"]},
    "acos_increase_pct":  {"warning": 0.30, "critical": 0.50},
    "bsr_increase_pct":   {"warning": 0.30, "critical": 0.50, "critical_min_days": 3},
}
```

### Noise filtering flags

```python
SUPPRESS_LESS_THAN_SINGLE = True            # Excludes less_than_single ASINs from email (enabled after backtest)
SUPPRESS_WATCH_FOR_LESS_THAN_SINGLE = False # True → suppress Watch for less_than_single only
SUPPRESS_WATCH_ALERTS = False               # True → exclude all Watch from email
```

### Baseball tier cutoffs

```python
TIER_THRESHOLDS = {
    "homerun": 2_500_000,   # > $2.5M trailing 12m revenue
    "triple":  1_500_000,
    "double":    750_000,
    "single":    250_000,
    # Below → less_than_single
}

# Hero ASINs — tighter absolute rules (margin <10% Warning, zero-sales Critical)
# Any ASIN with trailing 12m revenue >= $500K is hero, regardless of tier label
HERO_REVENUE_THRESHOLD = 500_000
```

---

## Threshold Tuning Guide

The manager owns threshold tuning after launch. Here is how to adjust:

### To change a z-score threshold

Open `config.py`, find `STD_DEV_THRESHOLDS`, and change the number for the relevant metric and severity. Higher number = harder to trigger = fewer alerts.

Example — make conversion_rate less sensitive:
```python
"conversion_rate": {"watch": 2.0, "warning": 3.0, "critical": 4.0},  # current: 1.5/2.5/3.5
```

### To change a hard floor

Find `ABSOLUTE_THRESHOLDS` and change the value. Example — raise return rate Warning from 2% to 3.5%:
```python
"return_rate": {"warning": 0.035, "critical": 0.05},  # was 0.02
```

### To suppress alerts for tail products

Set the noise filtering flag:
```python
SUPPRESS_WATCH_FOR_LESS_THAN_SINGLE = True  # Removes Watch alerts for <$250K ASINs
```

### To adjust YoY noise suppression floors

If small-value or newly-advertising products are creating noisy alerts, adjust these in config.py:

```python
# Raise to skip YoY sales alerts on higher-value products too (e.g. 100.0 = skip if < $100/day)
YOY_MIN_ABSOLUTE = {
    "sales": 50.0,
}

# Raise to skip YoY ad metric alerts when last year's baseline was weak (e.g. 0.10 = skip if < 10%)
YOY_MIN_BASELINE = {
    "acos":  0.05,
    "tacos": 0.05,
}
```

### To validate changes before applying

Run `backtest.py` after changing config values to see how the new thresholds would have performed on historical data. This shows you average alerts per day before and after without sending any emails.

---

## Alert Email Format

The email is HTML, colour-coded by severity, and designed to be scanned in 2–4 minutes.

**Structure:**
1. **Header** — Run date and data-through date
2. **Summary strip** — three coloured boxes: Critical count / Warning count / Watch count
3. **Severity legend** — what Critical / Warning / Watch each mean
4. **Top 10 plain-English explanations** — the most important alerts in plain language with possible causes
5. **Critical section** — full alert table sorted by tier (Homerun first)
6. **Warning section** — same structure
7. **Watch section** — same structure
8. **Footer** — run date, data lag note, total alert count

**Alert table columns:**

| Column | What it shows |
|--------|--------------|
| PRODUCT | ASIN title, ASIN ID, and BSR |
| METRIC | Which metric triggered (e.g. return_rate, margin) |
| ACTUAL | Today's value for that metric |
| EXPECTED | The baseline value being compared against. For rolling-triggered alerts: the 14-day rolling average. For YoY-triggered alerts: the same-week-last-year average. This is the number the alert is actually reacting to. |
| DEVIATION | `actual − expected` in the metric's own units. **Positive = metric is above expected. Negative = metric is below expected.** For example, `return_rate +6.0pp` means the return rate is 6 percentage points above its baseline (bad). `sales −$2,000` means sales are $2,000 below baseline (bad). |
| TRIGGERED BY | What fired the alert: `14-day avg` (rolling baseline), `vs last year` (YoY), `14-day + YoY` (both), or `Business rule` (absolute floor) |

> **pp = percentage points.** The absolute difference between two percentages. If return rate goes from 2.7% to 8.7%, that is +6.0pp — not "222% higher". Using pp avoids ambiguity when comparing percentages.

**Plain English example (return_rate alert):**
> Return rate for Bromine Starter Kit is 6.2% — 195% higher than the same week last year (2.1%). The 14-day average was 3.4%. Top return reason: *Not as described.* Possible causes: product quality issue, listing misrepresentation, in-transit damage.

**Note:** `less_than_single` ASINs are suppressed from the email by default (`SUPPRESS_LESS_THAN_SINGLE = True`). Their alerts are still saved to the Excel export every run — nothing is lost.

---

## Known Limitations

**Off-season products**
Products with strong seasonality (e.g. ice melt in April, pool openers in November) will show zero or near-zero sales, conversion, and margin during their off-season. The system will correctly detect these as large drops vs last year. These alerts are technically accurate but not actionable — the product is simply out of season. There is currently no per-product seasonality flag. When you see alerts for clearly off-season products, you can ignore them.

**Newly launched or recently relaunched ASINs**
Products with less than 12 months of history have no YoY baseline, so only rolling detection runs for them. Their rolling baselines are also less stable in the first few weeks. Expect noisier alerts for new ASINs until enough history accumulates.

**Products that weren't advertising last year**
If an ASIN had no meaningful ad spend in the same week last year (ACoS or TACoS baseline < 5%), the system skips the YoY comparison for those ad metrics. This is controlled by `YOY_MIN_BASELINE` in config.py. Without this guard, any current ad spend would look like an infinite-% spike.

**Margin volatility**
Margin is computed daily and fluctuates with ad spend allocation, FBA fee timing, and promotional pricing. It is naturally noisier than sales or conversion rate. The rolling thresholds for margin are intentionally set higher than other metrics to avoid alerting on routine daily variance. Use the YoY margin comparison and the negative-margin absolute floor for genuine margin problems.

**High return rates on some ASINs**
Several ASINs in the portfolio have structurally high return rates (above the 3.5% warning floor). If a product consistently runs above 3.5%, these alerts will fire every day. Either the floor should be raised for that specific product category, or the returns need investigating. The system cannot distinguish "always high" from "spiked high" using only the absolute floor — the statistical baseline comparison handles that distinction.

---

## Troubleshooting

**"config.py is missing required values"**
Ensure `.env` is configured with service account details and `DRIVE_FOLDERS` are set up before running (auto-detected based on Colab vs local environment).

**"Folder not found" warning during ingestion**
The mounted Drive path in `DRIVE_FOLDERS` is wrong. In Colab, click the folder icon in the left sidebar, navigate to the folder, right-click → Copy path, and paste into config.py.

**"No files found for [date]" during ingestion**
The file for reference date was not uploaded to the brand subfolder. Check that today's Sellerise/Inventory/Helium10 files are in the correct folder with the date in the filename.

**Email not received**
- Check that the service account JSON file exists in the `anomaly_alerting/` folder
- Verify the service account email has been shared with the Drive Data Feeds folder
- Verify `.env` file has correct `SENDER_EMAIL` and `RECIPIENT_EMAIL` values
- Check spam folder

**Too many alerts / too few alerts**
Run `backtest.py` to see the historical alert volume. Adjust thresholds in config.py following the Threshold Tuning Guide above. Re-run backtest to validate.
