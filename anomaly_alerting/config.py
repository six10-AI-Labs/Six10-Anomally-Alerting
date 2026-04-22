# config.py
# Single source of truth for all configurable parameters.
# No values should be hardcoded anywhere else in the pipeline — import from here.
# All thresholds are starting points. Tune after launch based on real alert volume.

import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# SERVICE ACCOUNT & EMAIL CONFIG
# =============================================================================
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "anomaly-alerting-b7f70323da39.json")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "ai@six10ventures.com")
RECIPIENT_EMAIL_RAW = os.getenv("RECIPIENT_EMAIL", "")
# Default recipients - editable here if .env is not used
DEFAULT_RECIPIENTS = ["shashank@six10ventures.com"] 

if RECIPIENT_EMAIL_RAW:
    RECIPIENT_EMAILS = [email.strip() for email in RECIPIENT_EMAIL_RAW.split(",") if email.strip()]
else:
    RECIPIENT_EMAILS = DEFAULT_RECIPIENTS

# Single recipient for backward compatibility
RECIPIENT_EMAIL = RECIPIENT_EMAILS[0] if RECIPIENT_EMAILS else ""

# Interactive Bridge URLs (Google Apps Script)
# Replace with your deployed Web App URLs
ACTION_BRIDGE_URL = "https://script.google.com/macros/s/AKfycbwaJbby5t44726b9PJvOynvcuODAjTf2vkRZeEtRg3TYf9drq5vuvz0qMDjaP1TvV1hVw/exec"
RERUN_BRIDGE_URL = "https://script.google.com/macros/s/AKfycbwaJbby5t44726b9PJvOynvcuODAjTf2vkRZeEtRg3TYf9drq5vuvz0qMDjaP1TvV1hVw/exec"
DISMISSAL_SHEET_ID = "17Pzl86UaSrcpvNLkTlahll3xtmCDeBjW20GzFYfUvY8"

# Monday.com Integration (Placeholders)
MONDAY_API_KEY = "YOUR_MONDAY_API_KEY_HERE"
MONDAY_PERSON_MAPPING = {
    "Pranav": "BOARD_ID_FOR_PRANAV",
    "Shashank": "BOARD_ID_FOR_SHASHANK",
    # Add more people and their Board/Group IDs here
}

# =============================================================================
# QUICK TUNING GUIDE — read this first
# =============================================================================
#
# Getting too many alerts?
#   1. Raise STD_DEV_THRESHOLDS values (higher number = harder to trigger)
#   2. Raise ABSOLUTE_THRESHOLDS floors (e.g. return_rate warning: 0.035 → 0.05)
#   3. Set SUPPRESS_LESS_THAN_SINGLE = True  (removes <$250K ASINs from email)
#   4. Set SUPPRESS_WATCH_ALERTS = True      (removes Watch severity from email)
#
# Getting too few alerts (missing real problems)?
#   1. Lower STD_DEV_THRESHOLDS values (lower number = easier to trigger)
#   2. Lower ABSOLUTE_THRESHOLDS floors
#
# Want to tune a specific metric?
#   → Find it in STD_DEV_THRESHOLDS (rolling) and YOY_THRESHOLDS (year-over-year)
#   → Change the watch / warning / critical number for that metric only
#
# Want to validate changes before applying?
#   → Run: python backtest.py
#   → This shows alert counts per day across all historical data without sending any email
#
# After tuning, re-run backtest and aim for 5–15 Criticals/day.
# =============================================================================

# =============================================================================
# BASELINE CONFIGURATION
# =============================================================================
# The system uses TWO baselines simultaneously. An alert fires if EITHER triggers.
# Each alert clearly states which baseline flagged it so the team knows the context.

# --- Short-term rolling window (catches sudden recent changes) ---
# Plain English: "Compare today against the average of the last N days"
# 30 days is the new robust baseline (increased from 14 to reduce noise).
# Higher number = smoother baseline, less reactive to minor day-to-day variance.
ROLLING_WINDOW_DAYS = 30

# --- Flexible Overrides ---
# Use this to set specific timeframes for products that require it.
# Format: {"ASIN": {"rolling": 45, "yoy": 14}}
ASIN_SPECIFIC_WINDOWS = {
    # "B0B57LJSM6": {"rolling": 45, "yoy": 7} # Example: extra smoothing for Bromine Kit
}

# List of ASINs that are known to be highly volatile or seasonal.
# The AI Assistant (Claude) will get this context and be more conservative
# when sanity-checking alerts for these products.
VOLATILE_ASINS = [
    "B0B57LJSM6", # Bromine Kit
]

# Minimum days of data before rolling baseline is considered meaningful.
# Avoids false alerts in the first week of a new product's history.
ROLLING_MIN_PERIODS = 7

# --- Year-over-Year window (catches seasonal underperformance) ---
# Plain English: "Compare today against the same week last year"
# Uses a 7-day average (3 days before + same day + 3 days after) from last year
# to smooth out daily noise. Solves the seasonal ramp-up false alert problem.
YOY_WINDOW_DAYS = 7          # Number of days around the same date last year to average
MIN_HISTORY_DAYS_FOR_YOY = 365  # ASIN needs 12+ months of data to use YoY baseline
                                 # New SKUs fall back to rolling window only


# =============================================================================
# STD DEVIATION THRESHOLDS — SHORT-TERM ROLLING BASELINE
# =============================================================================
# Applied to z-score computed against the 14-day rolling baseline.
# Plain English guide:
#   watch    ≈ metric moved ~1.5x its typical daily variation — worth noting
#   warning  ≈ metric moved ~2x its typical daily variation — investigate soon
#   critical ≈ metric moved ~3x its typical daily variation — act today
#
# ACoS/TACoS have higher bands because ad metrics fluctuate more day-to-day.
# DO NOT lock these in — validate against historical data after launch.
STD_DEV_THRESHOLDS = {
    "conversion_rate":    {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "return_rate":        {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "acos":               {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "tacos":              {"watch": 1.5, "warning": 2.5, "critical": 3.5},
    "sales":              {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "margin":             {"watch": 2.0, "warning": 3.0, "critical": 4.5},
    # Helium10 snapshot metrics
    "keyword_avg_rank":    {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "review_rating":       {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "review_count":        {"watch": 1.5, "warning": 2.0, "critical": 3.0},
    "organic_top10_count": {"watch": 1.5, "warning": 2.0, "critical": 3.0},
}


# =============================================================================
# YOY DEVIATION THRESHOLDS — YEAR-OVER-YEAR BASELINE
# =============================================================================
# Applied to % deviation from the same week last year.
# Plain English guide (example for sales "down" direction):
#   watch    = sales are 15%+ below same week last year — monitor
#   warning  = sales are 25%+ below same week last year — investigate
#   critical = sales are 40%+ below same week last year — act today
#
# For "up" direction metrics (return_rate, acos, tacos):
#   thresholds represent % INCREASE above last year's same-week average.
#
# These are starting points — validate and tune after launch.
YOY_THRESHOLDS = {
    "conversion_rate": {"watch": 0.15, "warning": 0.25, "critical": 0.40},
    "return_rate":     {"watch": 0.20, "warning": 0.50, "critical": 1.00},
    "acos":            {"watch": 0.20, "warning": 0.30, "critical": 0.50},
    "tacos":           {"watch": 0.20, "warning": 0.30, "critical": 0.50},
    "sales":           {"watch": 0.15, "warning": 0.25, "critical": 0.40},
    "margin":          {"watch": 0.20, "warning": 0.35, "critical": 0.55},
}


# =============================================================================
# YOY MINIMUM ABSOLUTE THRESHOLDS
# =============================================================================
# Suppresses YoY alerts when the actual metric value is below this floor.
# Prevents meaningless percentage alarms from near-zero values —
# e.g. $2.54 → $0.00 would be a 100% YoY drop and trigger CRITICAL,
# even though the absolute difference is trivial (off-season product).
# Only applies to the YoY baseline comparison — rolling baseline is unaffected.
YOY_MIN_ABSOLUTE = {
    "sales": 50.0,    # Skip YoY sales alert if actual daily sales < $50
}

# Minimum YoY baseline value before the YoY comparison is considered meaningful.
# If last year's average was below this floor, the product barely had activity in
# that metric (e.g. newly launched, not advertising), so any current value looks
# like a huge spike purely due to the near-zero denominator — not a real alert.
YOY_MIN_BASELINE = {
    "acos":  0.05,    # Skip YoY ACoS alert if last year's ACoS baseline < 5%
    "tacos": 0.05,    # Skip YoY TACoS alert if last year's TACoS baseline < 5%
}

# Minimum absolute difference between actual and YoY baseline before alerting.
# Prevents tiny absolute moves from triggering alerts purely because the relative
# % change is large on a small base (e.g. return_rate 0.8% → 1.1% = +37% relative
# but only +0.3pp absolute — not meaningful).
# Values are in the same units as the metric (decimals for rates, dollars for sales).
YOY_MIN_ABS_DIFF = {
    "return_rate": 0.01,   # Must differ by at least 1pp (0.01) to alert
}

# Minimum daily sales ($) before ACoS/TACoS detection runs for that ASIN on that day.
# When sales are near-zero, ACoS becomes an unreliable ratio (e.g. $11 ad spend
# on $1 of sales = 1100% ACoS). Below this floor, ACoS AND TACoS detection is skipped.
# This prevents absurd spikes like ACoS=1146% from appearing in the email.
ACOS_MIN_DAILY_SALES = 50.0

# Minimum YoY sales baseline ($) before the zero-sales business rule fires.
# If last year's same-week daily sales average was below this floor, zero sales
# this week is expected (e.g. Ice Melt in spring — $2.54 YoY baseline).
# Prevents Critical alerts on off-season or dormant products.
ZERO_SALES_MIN_YOY_BASELINE = 50.0


# =============================================================================
# METRIC DIRECTION
# =============================================================================
# "up"   → a spike upward is bad (return rate, ACoS, TACoS, BSR)
# "down" → a drop downward is bad (conversion rate, sales, margin)
METRIC_DIRECTION = {
    "conversion_rate":    "down",
    "return_rate":        "up",
    "acos":               "up",
    "tacos":              "up",
    "sales":              "down",
    "margin":             "down",
    # Helium10 snapshot metrics
    "keyword_avg_rank":    "up",    # higher rank number = worse
    "review_rating":       "both",  # drop = bad quality signal; spike = possible fake reviews
    "review_count":        "both",  # drop = unusual; spike = possible incentivised reviews
    "organic_top10_count": "down",  # fewer top-10 keywords = worse
}


# =============================================================================
# ABSOLUTE THRESHOLDS (hard business floors — override statistical results)
# =============================================================================
# These fire regardless of what the statistical baseline says.
# They exist to catch cases where "bad" has become the new normal statistically.
# Only escalates severity — never reduces it.
#
# All values confirmed by manager.

ABSOLUTE_THRESHOLDS = {

    # --- Return Rate ---
    # Plain English: above 3.5% = concerning, above 5% = fire drill (Amazon suppression risk)
    # Warning floor raised from 2% → 3.5% (backtest: 2% floor caused 56% of all return_rate Criticals)
    "return_rate": {
        "warning":  0.035,  # 3.5% → at least Warning (raised from 0.02 — backtest rec #2)
        "critical": 0.05,   # 5%   → Critical
    },

    # --- Conversion Rate Drop (vs rolling mean) ---
    # DISABLED — backtest showed these hard floors were the single noisiest trigger (7,095 Criticals).
    # Rolling baseline already catches real conversion drops. Hard floors fired on normal day-to-day bounce.
    "conversion_drop_pct": {
        "warning":  None,   # Disabled (backtest rec #1)
        "critical": None,   # Disabled (backtest rec #1)
    },

    # --- Margin ---
    # Plain English: negative margin = losing money on every unit → always Critical
    # Below 10% on a hero ASIN (triple/homerun) → Warning (margin squeeze)
    "margin": {
        "critical_floor":       0.0,    # Any negative margin → Critical
        "hero_warning_floor":   0.10,   # <10% margin on triple/homerun → Warning
    },

    # --- Sales Velocity (zero units) ---
    # Plain English: if a hero ASIN sells zero units for 2+ days and has inventory, something broke
    # Only applies to homerun/triple tier ASINs that have stock available
    "sales_zero": {
        "critical_consecutive_days": 2,         # 2+ consecutive zero-unit days → Critical
        "hero_tiers": ["homerun", "triple"],     # Only applies to these tiers
    },

    # --- BSR Worsening (% increase from rolling mean = worse rank) ---
    # Plain English: 30% rank worsening = Warning, 50%+ for 3+ days = Critical
    # NOTE: BSR statistical detection is v2 (requires 30+ days of Helium10 snapshots).
    #       Accumulating snapshots from Day 1. Critical consecutive-day check is v2.
    "bsr_increase_pct": {
        "warning":              0.30,   # 30% rank worsening → Warning
        "critical":             0.50,   # 50% rank worsening → Critical (v2: require 3+ consecutive days)
        "critical_min_days":    3,      # Minimum consecutive days for BSR Critical (v2)
    },

    # --- ACoS Spike ---
    # Plain English: 30% above baseline = Warning, 50% above = Critical
    # IMPORTANT: Only flag when sales are NOT increasing alongside ACoS.
    # If ACoS spikes 30% but sales also jumped 40%, it may just be scaling spend — not a problem.
    # The problem scenario is ACoS rising while sales are flat or declining.
    "acos_increase_pct": {
        "warning":  0.30,   # 30% spike from rolling mean → Warning (if sales flat/declining)
        "critical": 0.50,   # 50% spike → Critical (if sales flat/declining)
    },

    # --- TACoS ---
    # No universal hard floor. Normal TACoS varies too much by product.
    # Statistical detection handles TACoS. No absolute overrides.

}


# =============================================================================
# ACOS FLAGGING RULE
# =============================================================================
# Per manager: only flag ACoS anomalies when sales are NOT increasing.
# If ACoS spikes but sales also increase meaningfully, it may be intentional scaling.
ACOS_FLAG_ONLY_WITHOUT_SALES_INCREASE = True
ACOS_SALES_INCREASE_TOLERANCE = 0.05   # Sales must be growing less than 5% to flag ACoS


# =============================================================================
# BASEBALL TIER CUTOFFS (trailing 12-month revenue in USD)
# =============================================================================
# Confirmed against Baseball Category.png.
TIER_THRESHOLDS = {
    "homerun": 2_500_000,   # >$2.5M  (>10% of $25M portfolio)
    "triple":  1_500_000,   # $1.5M–$2.5M  (6–10%)
    "double":    750_000,   # $750K–$1.5M  (3–6%)
    "single":    250_000,   # $250K–$750K  (1–3%)
    # Below → "less_than_single"  (<1%)
}

# Hero tiers — used in absolute threshold checks (e.g. margin floor, zero-sales check)
HERO_TIERS = ["homerun", "triple"]

# Hero revenue threshold — supersedes HERO_TIERS for all hero rules.
# An ASIN is "hero" if its trailing 12-month revenue >= this value, regardless of tier label.
HERO_REVENUE_THRESHOLD = 500_000


# =============================================================================
# TIER SORT ORDER (display priority in email — does NOT change severity label)
# =============================================================================
TIER_SORT_ORDER = [
    "homerun",
    "triple",
    "double",
    "single",
    "less_than_single",
]


# =============================================================================
# SEVERITY LABELS
# =============================================================================
SEVERITY_LEVELS = ["critical", "warning", "watch"]


# =============================================================================
# HELIUM10 SNAPSHOT STORAGE
# =============================================================================
# Per manager: store every daily Helium10 snapshot to build BSR/keyword time series.
# Once 30+ days accumulated, BSR and keyword rank detection can be added (v2).
# Set path to the folder where accumulated snapshots should be saved.


# =============================================================================
# HELIUM10 SNAPSHOT METRICS — DETECTION CONFIG
# =============================================================================
# Detection for BSR, keyword rank, reviews using daily Helium10 snapshots.
# Requires HELIUM10_MIN_SNAPSHOTS days of history before detection activates.
# Snapshots accumulate from go-live — expect 14+ days before first alerts fire.

HELIUM10_MIN_SNAPSHOTS = 7   # Minimum snapshot days before detection activates

# Metrics to detect from Helium10 history snapshots
HELIUM10_DETECTION_METRICS = ["keyword_avg_rank", "review_rating", "review_count", "organic_top10_count"]

# Hard floor for review rating — PENDING MANAGER CONFIRMATION
# Raises severity regardless of statistical baseline
REVIEW_RATING_FLOORS = {
    "warning": 3.5,   # rating < 3.5 → at least Warning (PENDING MANAGER CONFIRMATION)
    "critical": None, # not yet defined — awaiting manager input
}


# =============================================================================
# OUTPUT / EXPORT CONFIG
# =============================================================================
# Full detection output (all flagged alerts) is saved as Excel after each run.
# Alert history is tracked to detect consecutive-day alerts and show "UNRESOLVED — DAY N".

# Show estimated daily dollar impact in the email subject line.
# e.g. "[Six10 Alerts] ~$4,700/day at risk | 15 Critical | 10 Warning"
SHOW_DOLLAR_IMPACT_IN_SUBJECT = True
# Set to a folder path. File will be named: alerts_YYYY-MM-DD.xlsx
# Leave empty to save in the current working directory.


# =============================================================================
# ALERT FILTERING (pending manager decision — flip flags once decided)
# =============================================================================
# All default to False = no filtering (show everything).
# Flip after manager confirms noise reduction approach.
SUPPRESS_LESS_THAN_SINGLE = True             # True → exclude all less_than_single ASINs from email
SUPPRESS_WATCH_FOR_LESS_THAN_SINGLE = False  # True → suppress Watch-only for less_than_single
SUPPRESS_WATCH_ALERTS = False                # True → exclude Watch severity from email entirely

# Maximum number of alerts shown per severity in the email, enforced in tier order
# (homerun first, then triple, double, single). Keeps email scannable for senior management.
ALERT_CAPS = {
    "critical":    15,
    "warning":     10,
    "watch":        5,
    "improvement": 10,
}


# =============================================================================
# GMAIL API CONFIG (Service Account)
# =============================================================================
# Uses service account for authentication — no passwords, no OAuth prompts
GMAIL_CONFIG = {
    "service_account_file": SERVICE_ACCOUNT_FILE,
    "sender_email": SENDER_EMAIL,
    "recipient_emails": RECIPIENT_EMAILS,
}


# =============================================================================
# GOOGLE DRIVE FOLDER IDs
# =============================================================================
# Each source folder contains brand subfolders (AquaDoc, Pureauty, etc.)
# with data files inside. Files include the date in their filename.
#
# DRIVE_FOLDER_IDS: Google Drive folder IDs (from the share URLs).
# Used for documentation and reference.
#
# DRIVE_FOLDERS: Mounted filesystem paths used by load_data.py at runtime.
# In Google Colab after drive.mount('/content/drive'), set these to the
# mounted path for each folder, e.g.:
#   "/content/drive/MyDrive/Six10/Data Feeds/Sellerise"
# Leave empty until go-live — pipeline will fail fast if any path is missing.

# Google Drive Folder IDs (shared drive)
DRIVE_FOLDER_IDS = {
    "sellerise":  "1Gszw84dyfn96jFdzcHP0hrKwV6AiAeSt",
    "returns":    "1_Za_P_RKIJ_h3Lzh2JILrf_En0TVbkhu",
    "inventory":  "1SWrWcxvkQktnEx84L6frkfdNS4aiDGTt",
    "helium10":   "1MzMyiX_QcU53Oqbt3Sdb3rS8BGOzwUOY",
    "outputs":    "1RD6mB2YAsxi1bgW_3TKrTcB2g4h5znWJ",
}

# For Colab: Use these Drive paths (mounted)
DRIVE_FOLDERS_COLAB = {
    "sellerise":  "/content/drive/MyDrive/Data Feeds/SellerRise Sales Data",
    "returns":    "/content/drive/MyDrive/Data Feeds/Return Reports",
    "inventory":  "/content/drive/MyDrive/Data Feeds/Amazon Restock Report",
    "helium10":   "/content/drive/MyDrive/Data Feeds/Helium10 Data",
}

# Auto-detect environment and select appropriate folder paths
def _is_colab():
    """Check if running in Google Colab."""
    try:
        from google.colab import drive
        return True
    except ImportError:
        return False

# Use Colab paths if in Colab, otherwise use Drive API (folder IDs)
DRIVE_FOLDERS = DRIVE_FOLDERS_COLAB if _is_colab() else DRIVE_FOLDER_IDS

# Local storage paths (relative to project root)
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
HELIUM10_SNAPSHOT_STORE = os.path.join(_DATA_DIR, "helium10_history")
EXCEL_OUTPUT_DIR = os.path.join(_DATA_DIR, "outputs")
DISMISSED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dismissed")

# Alert history file — tracks which ASIN+metric combinations have been
# consecutively flagged across runs. Used to show "UNRESOLVED — DAY N".
ALERT_HISTORY_FILE = os.path.join(_DATA_DIR, "outputs", "alert_history.csv")

# =============================================================================
# AI ASSISTANT CONFIG
# =============================================================================
# Set to True to enable Claude-based sanity checking and summarization.
# Requires ANTHROPIC_API_KEY environment variable to be set.
USE_LLM_ENHANCEMENTS = True


# =============================================================================
# SEASONAL CATEGORY INDEX
# =============================================================================
# Maps ASIN prefixes or keywords in product titles to seasonal activity patterns.
# month_weights: 1.0 = normal, 0.5 = half expected activity, 1.5 = peak.
# Used to suppress false YoY alerts during known off-seasons.
# If an ASIN's title matches a category, its YoY threshold is relaxed by the
# inverse of the month_weight (e.g. weight=0.5 → threshold doubled).
SEASONAL_CATEGORIES = {
    "pool_spa": {
        "title_keywords": ["pool", "spa", "hot tub", "chlorine", "bromine",
                           "algaecide", "clarifier", "defoamer"],
        "month_weights": {
            1: 0.4, 2: 0.5, 3: 0.7, 4: 1.0, 5: 1.3, 6: 1.5,
            7: 1.5, 8: 1.3, 9: 0.8, 10: 0.5, 11: 0.4, 12: 0.3,
        },
    },
    "eye_vitamins": {
        "title_keywords": ["eye vitamin", "areds", "lutein", "visivite", "macular"],
        "month_weights": {m: 1.0 for m in range(1, 13)},  # No strong seasonality
    },
    "pet": {
        "title_keywords": ["dog", "probiotic", "pawmedica", "anal gland"],
        "month_weights": {m: 1.0 for m in range(1, 13)},  # No strong seasonality
    },
}
