# Six10 Anomaly Alerting — Maintenance & Automation Guide

This guide covers the system architecture, local setup, automation via GitHub Actions, and all recent improvements.

## 1. System Overview

The pipeline consists of four main layers:

1. **Ingestion**: Loads raw reports from Google Drive (Sellerise, Returns, Inventory, Helium10).
2. **Preprocessing**: Standardizes column names, handles unit conversions, and assigns business tiers (Homerun, Triple, etc.).
3. **Detection**: Calculates 14-day rolling averages and YoY baselines to identify statistical anomalies.
4. **Alerting**: Generates a high-quality HTML digest email and saves a full audit trail to Excel.

### Architecture Type
This is a **deterministic, rule-based statistical pipeline** — not AI/ML. It uses Z-scores and YoY comparisons with hardcoded thresholds in `config.py`. All detection logic is transparent and auditable.

---

## 2. Recent Improvements (v2)

### 2.1 Min-Sales Floor for ACoS/TACoS
- **What**: ACoS and TACoS alerts are now suppressed when daily sales < $50.
- **Why**: Prevents absurd alerts like ACoS=1146% on days with near-zero sales.
- **Config**: `ACOS_MIN_DAILY_SALES = 50.0` in `config.py`

### 2.2 Alert Persistence ("UNRESOLVED — DAY N")
- **What**: The system tracks how many consecutive days each ASIN+metric combination has been flagged.
- **Display**: Critical alerts that persist across multiple runs show a red `DAY N` badge in the email.
- **Storage**: `anomaly_alerting/data/outputs/alert_history.csv` — auto-managed, do not edit manually.
- **Config**: `ALERT_HISTORY_FILE` in `config.py`

### 2.3 Dollar Impact in Email Subject
- **What**: Subject line now shows estimated daily revenue at risk.
- **Example**: `[Six10 Alerts] ~$4,700/day at risk | 2026-04-15 | 15 Critical | 10 Warning`
- **Calculation**: Sums sales shortfall (vs YoY baseline) + margin loss × daily sales for all non-improvement alerts.
- **Config**: `SHOW_DOLLAR_IMPACT_IN_SUBJECT = True` in `config.py` (set to `False` to disable)

### 2.4 Seasonality Awareness
- **What**: YoY thresholds are automatically relaxed during known off-seasons.
- **How**: Product titles are matched against `SEASONAL_CATEGORIES` keywords. Each category has per-month weights (e.g., pool products in January = 0.4x, meaning YoY thresholds are effectively doubled).
- **Example**: A pool chemical product showing 30% below last year's sales in January won't trigger a Warning because the seasonal weight (0.4) reduces the effective deviation score.
- **Config**: `SEASONAL_CATEGORIES` dict in `config.py` — add new categories or adjust weights as needed.
- **Current categories**: `pool_spa`, `eye_vitamins`, `pet`

---

## 3. GitHub Actions Automation

### Setup Instructions
To enable automation, add these **GitHub Secrets** (`Settings > Secrets and variables > Actions`):

1. **`GOOGLE_SERVICE_ACCOUNT_JSON`**: Full content of your `anomaly-alerting-*.json` service account file.
2. **`GMAIL_TOKEN_JSON`**: Content of `gmail_token.json` (generated via `get_oauth_token.py`).
3. **`DOT_ENV_CONTENT`**: Content of your `.env` file including:
   - `SENDER_EMAIL`
   - `RECIPIENT_EMAIL`

### Schedule
- **Daily**: Runs at 9:00 AM EST (14:00 UTC) automatically.
- **Manual**: Trigger anytime from the "Actions" tab → "Run Anomaly Detection Pipeline" → "Run workflow".
- **Python**: Uses **3.12.10** (matches local environment).

---

## 4. Local Maintenance

- **Output files**: `anomaly_alerting/data/outputs/` (alert Excel + alert history CSV)
- **Helium10 snapshots**: `anomaly_alerting/data/helium10_history/`
- **Exclusions**: `.gitignore` prevents credentials, tokens, and business CSVs from being pushed.

---

## 5. Tuning Thresholds

If the system is too noisy or missing alerts, adjust these in `anomaly_alerting/config.py`:

| Setting | Default | Effect |
|---------|---------|--------|
| `STD_DEV_THRESHOLDS` | varies | Higher = fewer rolling alerts |
| `YOY_THRESHOLDS` | varies | Higher = fewer YoY alerts |
| `ROLLING_WINDOW_DAYS` | 14 | Smoothing period for recent baseline |
| `ACOS_MIN_DAILY_SALES` | $50 | Min sales before ACoS/TACoS alerts fire |
| `ACOS_SALES_INCREASE_TOLERANCE` | 5% | Suppress ACoS alerts when sales are also rising |
| `ALERT_CAPS` | 15/10/5/10 | Max alerts per severity in email |
| `SUPPRESS_LESS_THAN_SINGLE` | True | Exclude <$250K ASINs from email |
| `SEASONAL_CATEGORIES` | pool/eye/pet | Add categories or change month weights |
| `SHOW_DOLLAR_IMPACT_IN_SUBJECT` | True | Toggle $ impact in email subject |

---

## 6. Troubleshooting

- **OAuth Token Expired**: Run `python anomaly_alerting/alerting/get_oauth_token.py` locally, then update the `GMAIL_TOKEN_JSON` GitHub Secret.
- **Drive Access Denied**: Ensure the Service Account email is shared on the relevant Google Drive folders.
- **Alert History Reset**: Delete `anomaly_alerting/data/outputs/alert_history.csv` to reset consecutive day counters.
- **Seasonal False Positives**: Adjust `month_weights` in `SEASONAL_CATEGORIES` (lower weight = more suppression during that month).
