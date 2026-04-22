# detection/anomaly_detection.py
# Layer 3 — dual-baseline anomaly detection (14-day rolling + Year-over-Year).
#
# BASELINE DESIGN (per manager):
#   Baseline 1 — Short-term rolling (14 days): catches sudden recent changes
#   Baseline 2 — Year-over-Year (same week last year, 7-day avg): catches seasonal underperformance
#   An alert fires if EITHER baseline triggers.
#   Each flagged row records which baseline triggered it.
#
# New SKUs (<12 months of data): rolling baseline only. YoY flagged as unavailable in output.
# BSR: context column only in v1 (no historical time series yet). Accumulating snapshots for v2.

import numpy as np
import pandas as pd

# Metrics to run detection on — all from Sellerise time-series
DETECTION_METRICS = ["conversion_rate", "return_rate", "acos", "tacos", "sales", "margin"]

# Severity ranking (higher = more severe)
SEVERITY_RANK = {"watch": 1, "warning": 2, "critical": 3}


def _escalate(current: str, new: str) -> str:
    """Return the more severe of two severity labels."""
    if not current or current == "None":
        return new
    return new if SEVERITY_RANK.get(new, 0) > SEVERITY_RANK.get(current, 0) else current


# =============================================================================
# BASELINE 1: Short-Term Rolling Baseline
# =============================================================================

def compute_rolling_baseline(df: pd.DataFrame, metric: str,
                              default_window: int, min_periods: int, 
                              asin_overrides: dict = None) -> pd.DataFrame:
    """
    Compute rolling mean and std deviation for a metric, grouped by ASIN.
    Supports per-ASIN window overrides from config.
    """
    df = df.copy()
    df = df.sort_values(["asin", "date"])

    def _get_rolling_stat(group, stat="mean"):
        asin = group.name
        # Check for ASIN-specific window in config overrides
        window = asin_overrides.get(asin, {}).get("rolling", default_window) if asin_overrides else default_window
        
        roller = group.rolling(window=window, min_periods=min_periods)
        return roller.mean() if stat == "mean" else roller.std()

    df[f"{metric}_roll_mean"] = df.groupby("asin")[metric].transform(lambda x: _get_rolling_stat(x, "mean"))
    df[f"{metric}_roll_std"]  = df.groupby("asin")[metric].transform(lambda x: _get_rolling_stat(x, "std"))
    
    return df


def compute_rolling_zscore(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Compute z-score against the short-term rolling baseline.

    z = (actual - rolling_mean) / rolling_std
    Handles std=0 and nulls gracefully (returns NaN).

    Adds: '{metric}_roll_zscore'
    """
    df = df.copy()
    safe_std = df[f"{metric}_roll_std"].replace(0, np.nan)
    df[f"{metric}_roll_zscore"] = (df[metric] - df[f"{metric}_roll_mean"]) / safe_std
    return df


def flag_rolling_anomalies(df: pd.DataFrame, metric: str,
                            thresholds: dict, direction: str) -> pd.DataFrame:
    """
    Assign severity using the rolling baseline z-score, direction-aware.

    "up"   → positive z is bad (actual > mean = bad, e.g. return rate spiking)
    "down" → negative z is bad (actual < mean = bad, e.g. conversion dropping)

    Adds: '{metric}_roll_severity'
    """
    df = df.copy()
    zscore = df[f"{metric}_roll_zscore"]
    bad_score = zscore.abs() if direction == "both" else (zscore if direction == "up" else -zscore)

    conditions = [
        bad_score >= thresholds["critical"],
        bad_score >= thresholds["warning"],
        bad_score >= thresholds["watch"],
    ]
    df[f"{metric}_roll_severity"] = np.select(conditions, ["critical", "warning", "watch"], default=None)
    df.loc[zscore.isna(), f"{metric}_roll_severity"] = None
    return df


# =============================================================================
# BASELINE 2: Year-over-Year Baseline
# =============================================================================

def compute_yoy_baseline(df: pd.DataFrame, metric: str,
                          default_yoy_window: int, min_history_days: int,
                          asin_overrides: dict = None) -> pd.DataFrame:
    """
    Compute Year-over-Year baseline for a metric, per ASIN.
    Supports per-ASIN window overrides from config.
    """
    df = df.copy()
    df = df.sort_values(["asin", "date"])

    # Mark ASINs that have enough history for YoY
    history_days = df.groupby("asin")["date"].transform(lambda x: (x.max() - x.min()).days)
    df["yoy_available"] = history_days >= min_history_days

    # Build lookup: asin → date → metric value
    lookup = df.set_index(["asin", "date"])[metric]

    yoy_means = []
    for _, row in df.iterrows():
        if not row["yoy_available"]:
            yoy_means.append(np.nan)
            continue

        asin = row["asin"]
        date = row["date"]
        
        # Get ASIN-specific YoY window or default
        yoy_window = asin_overrides.get(asin, {}).get("yoy", default_yoy_window) if asin_overrides else default_yoy_window
        half_window = yoy_window // 2
        
        yoy_center = date - pd.DateOffset(years=1)

        # N-day window around same date last year
        dates_to_check = [yoy_center + pd.Timedelta(days=d) for d in range(-half_window, half_window + 1)]
        vals = [lookup.get((asin, d), np.nan) for d in dates_to_check]
        vals = [v for v in vals if not np.isnan(v)]

        yoy_means.append(np.mean(vals) if vals else np.nan)

    df[f"{metric}_yoy_mean"] = yoy_means
    return df


def compute_yoy_deviation(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Compute % deviation from the YoY baseline.

    For "up" direction: positive deviation = actual is higher than last year = bad
    For "down" direction: negative deviation = actual is lower than last year = bad

    Adds: '{metric}_yoy_deviation' (signed % change from last year's value)
    """
    df = df.copy()
    safe_yoy = df[f"{metric}_yoy_mean"].replace(0, np.nan)
    df[f"{metric}_yoy_deviation"] = (df[metric] - safe_yoy) / safe_yoy.abs()
    return df


def flag_yoy_anomalies(df: pd.DataFrame, metric: str,
                        thresholds: dict, direction: str, config_obj=None) -> pd.DataFrame:
    """
    Assign severity using the YoY % deviation, direction-aware.

    Adds: '{metric}_yoy_severity'
    """
    df = df.copy()
    deviation = df[f"{metric}_yoy_deviation"]
    bad_score = deviation.abs() if direction == "both" else (deviation if direction == "up" else -deviation)

    # Adjust bad_score based on seasonal categories (if applicable)
    if config_obj is not None and hasattr(config_obj, "SEASONAL_CATEGORIES") and "title" in df.columns:
        import re
        month = df["date"].dt.month
        for cat_name, cat_data in config_obj.SEASONAL_CATEGORIES.items():
            keywords = cat_data.get("title_keywords", [])
            weights = cat_data.get("month_weights", {})
            if not keywords or not weights:
                continue
            
            pattern = "|".join([re.escape(k.lower()) for k in keywords])
            mask = df["title"].fillna("").str.lower().str.contains(pattern)
            
            if mask.any():
                weight_series = month.map(weights).fillna(1.0)
                # If weight is 0.5 (off-season), bad_score becomes 0.5x, making it harder to trigger alerts.
                bad_score.loc[mask] = bad_score.loc[mask] * weight_series.loc[mask]

    conditions = [
        bad_score >= thresholds["critical"],
        bad_score >= thresholds["warning"],
        bad_score >= thresholds["watch"],
    ]
    df[f"{metric}_yoy_severity"] = np.select(conditions, ["critical", "warning", "watch"], default=None)
    df.loc[deviation.isna(), f"{metric}_yoy_severity"] = None
    return df


# =============================================================================
# COMBINE DUAL BASELINES
# =============================================================================

def combine_baselines(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Combine rolling and YoY severity into a single final severity per row.

    Rules:
    - If both trigger: take the more severe, mark triggered_by = "both"
    - If only rolling triggers: triggered_by = "rolling"
    - If only YoY triggers: triggered_by = "yoy"
    - If neither triggers: severity = None

    Adds: '{metric}_severity', '{metric}_triggered_by'
    """
    df = df.copy()
    roll_sev = df[f"{metric}_roll_severity"]
    yoy_sev  = df[f"{metric}_yoy_severity"]

    severities = []
    triggered_by = []

    for r, y in zip(roll_sev, yoy_sev):
        # Treat NaN/None as missing; only real severity labels are valid.
        # Using plain truthiness is unsafe because bool(np.nan) is True.
        r_valid = pd.notna(r) and r != "None"
        y_valid = pd.notna(y) and y != "None"

        if r_valid and y_valid:
            severities.append(_escalate(r, y))
            triggered_by.append("both")
        elif r_valid:
            severities.append(r)
            triggered_by.append("rolling")
        elif y_valid:
            severities.append(y)
            triggered_by.append("yoy")
        else:
            severities.append(None)
            triggered_by.append(None)

    df[f"{metric}_severity"]     = severities
    df[f"{metric}_triggered_by"] = triggered_by
    return df


# =============================================================================
# ABSOLUTE THRESHOLD OVERRIDES
# =============================================================================

def apply_absolute_overrides(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Escalate severity using hard absolute business floors.

    Overrides (all confirmed by manager):
    1. return_rate: > 2% → Warning, > 5% → Critical
    2. conversion_rate drop: > 20% from rolling mean → Warning, > 25% → Critical
    3. margin: < 0 → always Critical; < 10% on hero ASIN → Warning
    4. sales: 0 units for 2+ consecutive days on hero ASIN with stock → Critical
    5. acos: 30% spike → Warning, 50% → Critical — ONLY when sales not increasing

    Never reduces severity. Only escalates.
    Also sets triggered_by = "absolute_threshold" for any row escalated here
    that didn't already have a triggered_by value from statistical detection.
    """
    df = df.copy()
    at = config.ABSOLUTE_THRESHOLDS

    def escalate_with_trigger(df, sev_col, trigger_col, mask, level):
        """Escalate severity and mark triggered_by = 'absolute_threshold' if not already set."""
        df.loc[mask, sev_col] = df.loc[mask, sev_col].apply(lambda s: _escalate(s, level))
        if trigger_col in df.columns:
            df.loc[mask & df[trigger_col].isna(), trigger_col] = "absolute_threshold"
        return df

    # --- 1. Return Rate ---
    if "return_rate_severity" in df.columns:
        rt = at.get("return_rate", {})
        for floor, level in [(rt.get("critical"), "critical"), (rt.get("warning"), "warning")]:
            if floor is not None:
                mask = df["return_rate"].notna() & (df["return_rate"] > floor)
                df = escalate_with_trigger(df, "return_rate_severity", "return_rate_triggered_by", mask, level)

    # --- 2. Conversion Rate Drop ---
    if "conversion_rate_severity" in df.columns and "conversion_rate_roll_mean" in df.columns:
        cr = at.get("conversion_drop_pct", {})
        valid_mean = df["conversion_rate_roll_mean"] >= 1.0
        drop_pct = (
            (df["conversion_rate_roll_mean"] - df["conversion_rate"]) /
            df["conversion_rate_roll_mean"].replace(0, np.nan)
        )
        for floor, level in [(cr.get("critical"), "critical"), (cr.get("warning"), "warning")]:
            if floor is not None:
                mask = valid_mean & (drop_pct > floor)
                df = escalate_with_trigger(df, "conversion_rate_severity", "conversion_rate_triggered_by", mask, level)

    # --- 3. Margin ---
    if "margin_severity" in df.columns:
        mg = at.get("margin", {})
        critical_floor = mg.get("critical_floor")
        hero_warning   = mg.get("hero_warning_floor")

        if critical_floor is not None:
            mask = df["margin"].notna() & (df["margin"] < critical_floor)
            df = escalate_with_trigger(df, "margin_severity", "margin_triggered_by", mask, "critical")
        if hero_warning is not None:
            hero_mask = df.get("is_hero", pd.Series(False, index=df.index)).fillna(False).astype(bool)
            mask = hero_mask & df["margin"].notna() & (df["margin"] < hero_warning)
            df = escalate_with_trigger(df, "margin_severity", "margin_triggered_by", mask, "warning")

    # --- 4. Sales Zero Consecutive Days (hero ASINs only, in stock) ---
    if "sales_severity" in df.columns and "units" in df.columns:
        sz = at.get("sales_zero", {})
        min_days   = sz.get("critical_consecutive_days", 2)

        df = df.sort_values(["asin", "date"])
        df["_units_zero"] = (df["units"].fillna(0) == 0).astype(int)
        df["_consec_zero"] = (
            df.groupby("asin")["_units_zero"]
            .transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        )
        has_stock = df.get("available_units", pd.Series(1, index=df.index)).fillna(1) > 0
        is_hero   = df.get("is_hero", pd.Series(False, index=df.index)).fillna(False).astype(bool)
        mask = is_hero & has_stock & (df["_consec_zero"] >= min_days)

        # Don't fire on off-season / low-activity products.
        # If last year's same-week daily sales were below the minimum threshold,
        # zero sales this week is expected (e.g. Ice Melt in spring: $2.54 YoY baseline).
        min_yoy = getattr(config, "ZERO_SALES_MIN_YOY_BASELINE", 50.0)
        if "sales_yoy_mean" in df.columns:
            meaningful_baseline = df["sales_yoy_mean"].isna() | (df["sales_yoy_mean"] >= min_yoy)
            mask = mask & meaningful_baseline

        df = escalate_with_trigger(df, "sales_severity", "sales_triggered_by", mask, "critical")
        df = df.drop(columns=["_units_zero", "_consec_zero"])

    # --- 5. ACoS Spike (only when sales not increasing) ---
    if "acos_severity" in df.columns and config.ACOS_FLAG_ONLY_WITHOUT_SALES_INCREASE:
        ac = at.get("acos_increase_pct", {})
        tolerance = config.ACOS_SALES_INCREASE_TOLERANCE

        if "sales_roll_mean" in df.columns:
            sales_change = (df["sales"] - df["sales_roll_mean"]) / df["sales_roll_mean"].replace(0, np.nan)
            sales_not_increasing = sales_change.fillna(0) <= tolerance
        else:
            sales_not_increasing = pd.Series(True, index=df.index)

        if "acos_roll_mean" in df.columns:
            spike_pct = (df["acos"] - df["acos_roll_mean"]) / df["acos_roll_mean"].replace(0, np.nan)
            for floor, level in [(ac.get("critical"), "critical"), (ac.get("warning"), "warning")]:
                if floor is not None:
                    mask = sales_not_increasing & (spike_pct > floor)
                    df = escalate_with_trigger(df, "acos_severity", "acos_triggered_by", mask, level)

            # Clear ACoS flags where sales ARE increasing significantly
            sales_increasing = sales_change.fillna(0) > tolerance
            df.loc[sales_increasing, "acos_severity"]     = None
            df.loc[sales_increasing, "acos_triggered_by"] = None

    return df


# =============================================================================
# FULL DETECTION PIPELINE
# =============================================================================

def run_detection(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Orchestrate the full dual-baseline anomaly detection pipeline.

    For each of the 6 metrics:
        1. Compute 14-day rolling baseline + z-score + rolling severity
        2. Compute YoY baseline (if 12+ months of data) + % deviation + YoY severity
        3. Combine: take more severe, record triggered_by
    Then apply absolute threshold overrides.
    Finally build long-format output with one row per ASIN + date + metric flagged.

    Output columns:
        asin | date | tier | title | metric | actual_value | expected_value |
        z_score | yoy_deviation | severity | triggered_by | yoy_available | category_bsr
    """
    print("\n" + "=" * 50)
    print("DETECTION LAYER — Dual-baseline anomaly detection")
    print("=" * 50)
    print(f"  Rolling window : {config.ROLLING_WINDOW_DAYS} days")
    print(f"  YoY window     : {config.YOY_WINDOW_DAYS} days (same week last year)")

    working = df.copy()

    for metric in DETECTION_METRICS:
        if metric not in working.columns:
            print(f"  [SKIP] {metric} — column not found")
            continue

        print(f"  Processing: {metric}")
        thresholds     = config.STD_DEV_THRESHOLDS.get(metric, {})
        yoy_thresholds = config.YOY_THRESHOLDS.get(metric, {})
        direction      = config.METRIC_DIRECTION.get(metric, "up")

        # Baseline 1: rolling
        working = compute_rolling_baseline(
            working, metric, config.ROLLING_WINDOW_DAYS, config.ROLLING_MIN_PERIODS, 
            asin_overrides=getattr(config, "ASIN_SPECIFIC_WINDOWS", {})
        )
        working = compute_rolling_zscore(working, metric)
        working = flag_rolling_anomalies(working, metric, thresholds, direction)

        # Baseline 2: YoY
        working = compute_yoy_baseline(
            working, metric, config.YOY_WINDOW_DAYS, config.MIN_HISTORY_DAYS_FOR_YOY,
            asin_overrides=getattr(config, "ASIN_SPECIFIC_WINDOWS", {})
        )
        working = compute_yoy_deviation(working, metric)
        working = flag_yoy_anomalies(working, metric, yoy_thresholds, direction, config_obj=config)

        # Suppress YoY alerts where actual value is below the minimum absolute floor
        # (prevents 100% YoY criticals from near-zero values like $0 vs $2.54)
        yoy_min = getattr(config, "YOY_MIN_ABSOLUTE", {}).get(metric)
        if yoy_min is not None:
            below_floor = working[metric].notna() & (working[metric] < yoy_min)
            working.loc[below_floor, f"{metric}_yoy_severity"] = None

        # Suppress YoY alerts where the historical baseline itself is near-zero
        # (prevents fake spikes when a product barely ran ads last year,
        # making any current spend look like an astronomical % increase)
        yoy_min_baseline = getattr(config, "YOY_MIN_BASELINE", {}).get(metric)
        if yoy_min_baseline is not None:
            yoy_mean_col = f"{metric}_yoy_mean"
            if yoy_mean_col in working.columns:
                weak_baseline = working[yoy_mean_col].notna() & (working[yoy_mean_col] < yoy_min_baseline)
                working.loc[weak_baseline, f"{metric}_yoy_severity"] = None

        # Suppress YoY alerts where the absolute difference is too small to be meaningful
        # (e.g. return_rate 0.8% → 1.1% = +37% relative but only +0.3pp absolute)
        yoy_min_abs_diff = getattr(config, "YOY_MIN_ABS_DIFF", {}).get(metric)
        if yoy_min_abs_diff is not None:
            yoy_mean_col = f"{metric}_yoy_mean"
            if yoy_mean_col in working.columns:
                abs_diff = (working[metric] - working[yoy_mean_col]).abs()
                too_small = abs_diff.notna() & (abs_diff < yoy_min_abs_diff)
                working.loc[too_small, f"{metric}_yoy_severity"] = None

        # Suppress ACoS and TACoS detection when daily sales are too low to make them meaningful
        if metric in ("acos", "tacos") and "sales" in working.columns:
            min_sales = getattr(config, "ACOS_MIN_DAILY_SALES", None)
            if min_sales is not None:
                low_sales = working["sales"].notna() & (working["sales"] < min_sales)
                working.loc[low_sales, f"{metric}_roll_severity"] = None
                working.loc[low_sales, f"{metric}_yoy_severity"]  = None

        # Combine
        working = combine_baselines(working, metric)

    # Absolute overrides
    print("  Applying absolute threshold overrides...")
    working = apply_absolute_overrides(working, config)

    # Build long-format output
    print("  Building long-format output...")
    records = []
    for metric in DETECTION_METRICS:
        sev_col = f"{metric}_severity"
        if sev_col not in working.columns:
            continue

        flagged = working[working[sev_col].notna() & (working[sev_col] != "None")].copy()
        if flagged.empty:
            continue

        context_cols = ["asin", "date", "tier", "title", "category_bsr", "yoy_available", "top_return_reason"]
        available = [c for c in context_cols if c in flagged.columns]

        out = flagged[available].copy()
        out["metric"]         = metric
        out["actual_value"]   = flagged[metric]
        out["expected_value"] = flagged.get(f"{metric}_roll_mean", np.nan)
        out["yoy_baseline"]   = flagged.get(f"{metric}_yoy_mean", np.nan)
        out["z_score"]        = flagged.get(f"{metric}_roll_zscore", np.nan)
        out["yoy_deviation"]  = flagged.get(f"{metric}_yoy_deviation", np.nan)
        out["severity"]       = flagged[sev_col]
        out["triggered_by"]   = flagged.get(f"{metric}_triggered_by", "rolling")
        # Carry sales rolling mean alongside margin rows for daily dollar impact calculation.
        # sales_roll_mean is available here because sales is processed before margin in DETECTION_METRICS.
        if metric == "margin" and "sales_roll_mean" in flagged.columns:
            out["sales_roll_mean"] = flagged["sales_roll_mean"]

        records.append(out)

    # Build improvement records — metrics significantly BETTER than expected
    # Only includes rows that don't already have a bad-direction alert.
    # Uses the warning-level threshold as the minimum to avoid noise.
    print("  Building positive signals (improvements)...")
    improvement_records = []
    for metric in DETECTION_METRICS:
        sev_col = f"{metric}_severity"
        zscore_col = f"{metric}_roll_zscore"
        yoy_dev_col = f"{metric}_yoy_deviation"
        roll_mean_col = f"{metric}_roll_mean"
        yoy_mean_col = f"{metric}_yoy_mean"

        if sev_col not in working.columns:
            continue

        direction = config.METRIC_DIRECTION.get(metric, "up")
        roll_warn = config.STD_DEV_THRESHOLDS.get(metric, {}).get("warning", 2.0)
        yoy_warn  = config.YOY_THRESHOLDS.get(metric, {}).get("warning", 0.25)

        no_bad_alert = working[sev_col].isna() | (working[sev_col] == "None")

        # Rolling improvement: actual meaningfully better than 14-day average
        roll_imp = pd.Series(False, index=working.index)
        if zscore_col in working.columns:
            z = working[zscore_col]
            if direction == "up":
                roll_imp = z.notna() & (-z >= roll_warn)    # metric went down (good for up-is-bad)
            elif direction == "down":
                roll_imp = z.notna() & (z >= roll_warn)     # metric went up (good for down-is-bad)

        # YoY improvement: actual meaningfully better than same week last year
        yoy_imp = pd.Series(False, index=working.index)
        if yoy_dev_col in working.columns:
            dev = working[yoy_dev_col]
            if direction == "up":
                yoy_imp = dev.notna() & (-dev >= yoy_warn)
            elif direction == "down":
                yoy_imp = dev.notna() & (dev >= yoy_warn)

        # Only include rows with no bad alert and at least one improvement signal
        imp_mask = no_bad_alert & (roll_imp | yoy_imp)
        flagged = working[imp_mask].copy()
        if flagged.empty:
            continue

        # Determine what triggered the improvement
        def _imp_trigger(r, y):
            if r and y: return "both"
            if r: return "rolling"
            if y: return "yoy"
            return "rolling"

        context_cols = ["asin", "date", "tier", "title", "category_bsr", "yoy_available", "top_return_reason"]
        available = [c for c in context_cols if c in flagged.columns]

        out = flagged[available].copy()
        out["metric"]         = metric
        out["actual_value"]   = flagged[metric]
        out["expected_value"] = flagged[roll_mean_col] if roll_mean_col in flagged.columns else np.nan
        out["yoy_baseline"]   = flagged[yoy_mean_col]  if yoy_mean_col  in flagged.columns else np.nan
        out["z_score"]        = flagged[zscore_col]    if zscore_col    in flagged.columns else np.nan
        out["yoy_deviation"]  = flagged[yoy_dev_col]   if yoy_dev_col   in flagged.columns else np.nan
        out["severity"]       = "improvement"
        out["triggered_by"]   = [
            _imp_trigger(r, y)
            for r, y in zip(roll_imp[imp_mask], yoy_imp[imp_mask])
        ]
        improvement_records.append(out)

    if not records and not improvement_records:
        print("  No anomalies detected.")
        return pd.DataFrame()

    all_records = records + improvement_records
    result = pd.concat(all_records, ignore_index=True)

    print("\n" + "=" * 50)
    print("DETECTION COMPLETE")
    print(f"  Total flagged rows : {len(result):,}")
    print(f"  Unique ASINs       : {result['asin'].nunique()}")
    print(f"  Severity breakdown :")
    print(result["severity"].value_counts().to_string())
    print(f"  Triggered by       :")
    print(result["triggered_by"].value_counts().to_string())
    print("=" * 50)

    return result


# =============================================================================
# HELIUM10 SNAPSHOT METRICS DETECTION
# =============================================================================

def run_helium10_detection(helium10_history: pd.DataFrame, master_df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Run rolling baseline detection on Helium10 snapshot metrics.

    Detects anomalies in keyword rank, review rating, review count, and
    organic top-10 count using the accumulated daily Helium10 snapshots.

    Requires config.HELIUM10_MIN_SNAPSHOTS days of history to activate.
    YoY baseline not yet available — rolling only until 12+ months of snapshots.

    Args:
        helium10_history: Dataframe from load_helium10_history() with snapshot_date column.
        master_df: Full master dataframe (used to join tier, title, is_hero per ASIN).
        config: Config module.

    Returns:
        Long-format detection output (same schema as run_detection()) or empty DataFrame.
    """
    if helium10_history is None or helium10_history.empty:
        print("  [Helium10 Detection] No snapshot history available — skipping.")
        return pd.DataFrame()

    metrics = getattr(config, "HELIUM10_DETECTION_METRICS",
                      ["keyword_avg_rank", "review_rating", "review_count", "organic_top10_count"])
    min_snaps = getattr(config, "HELIUM10_MIN_SNAPSHOTS", 7)

    # Rename snapshot_date → date for compatibility with rolling baseline functions
    h = helium10_history.copy()
    if "snapshot_date" in h.columns:
        h = h.rename(columns={"snapshot_date": "date"})
    h["date"] = pd.to_datetime(h["date"])

    # Check if enough snapshots exist
    n_dates = h["date"].nunique()
    if n_dates < min_snaps:
        print(f"  [Helium10 Detection] Only {n_dates} snapshots — need {min_snaps} to activate. Skipping.")
        return pd.DataFrame()

    print(f"\n  [Helium10 Detection] Running on {n_dates} snapshots | {h['asin'].nunique()} ASINs")

    # Build ASIN → tier, title, is_hero lookup from master_df
    asin_meta = (
        master_df[["asin"] + [c for c in ["tier", "title", "is_hero", "category_bsr"] if c in master_df.columns]]
        .drop_duplicates("asin")
    )

    records = []
    for metric in metrics:
        if metric not in h.columns:
            print(f"  [Helium10 Detection] {metric} — column not found, skipping")
            continue

        thresholds = config.STD_DEV_THRESHOLDS.get(metric, {"watch": 1.5, "warning": 2.0, "critical": 3.0})
        direction  = config.METRIC_DIRECTION.get(metric, "down")

        working = h[["asin", "date", metric]].dropna(subset=[metric]).copy()

        # Rolling baseline
        working = compute_rolling_baseline(
            working, metric, config.ROLLING_WINDOW_DAYS, config.ROLLING_MIN_PERIODS,
            asin_overrides=getattr(config, "ASIN_SPECIFIC_WINDOWS", {})
        )
        working = compute_rolling_zscore(working, metric)
        working = flag_rolling_anomalies(working, metric, thresholds, direction)

        # YoY not available for snapshot metrics yet
        working[f"{metric}_yoy_severity"] = None
        working = combine_baselines(working, metric)
        working["yoy_available"] = False

        # Apply review rating hard floor if applicable
        if metric == "review_rating":
            floors = getattr(config, "REVIEW_RATING_FLOORS", {})
            for level in ["critical", "warning"]:
                floor = floors.get(level)
                if floor is not None:
                    mask = working["review_rating"].notna() & (working["review_rating"] < floor)
                    sev_col = "review_rating_severity"
                    working.loc[mask, sev_col] = working.loc[mask, sev_col].apply(lambda s: _escalate(s, level))
                    trig_col = "review_rating_triggered_by"
                    if trig_col not in working.columns:
                        working[trig_col] = None
                    working.loc[mask & working[trig_col].isna(), trig_col] = "absolute_threshold"

        sev_col = f"{metric}_severity"
        flagged = working[working[sev_col].notna() & (working[sev_col] != "None")].copy()
        if flagged.empty:
            continue

        out = flagged[["asin", "date", "yoy_available"]].copy()
        out["metric"]         = metric
        out["actual_value"]   = flagged[metric]
        out["expected_value"] = flagged.get(f"{metric}_roll_mean", np.nan)
        out["yoy_baseline"]   = np.nan
        out["z_score"]        = flagged.get(f"{metric}_roll_zscore", np.nan)
        out["yoy_deviation"]  = np.nan
        out["severity"]       = flagged[sev_col]
        out["triggered_by"]   = flagged.get(f"{metric}_triggered_by", "rolling")

        records.append(out)

    if not records:
        print("  [Helium10 Detection] No Helium10 anomalies detected.")
        return pd.DataFrame()

    result = pd.concat(records, ignore_index=True)

    # Join tier, title, is_hero from master_df
    result = result.merge(asin_meta, on="asin", how="left")
    if "tier" not in result.columns:
        result["tier"] = "less_than_single"

    print(f"  [Helium10 Detection] {len(result):,} flagged rows across {result['asin'].nunique()} ASINs")
    return result


# =============================================================================
# GET FLAGGED ROWS
# =============================================================================

def get_flagged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter detection output to rows where severity is assigned.

    Args:
        df: Long-format dataframe from run_detection().

    Returns:
        Filtered dataframe with only flagged rows.
    """
    if df.empty:
        return df
    return df[df["severity"].notna() & (df["severity"] != "None")].reset_index(drop=True)


# =============================================================================
# ALERT HISTORY TRACKING
# =============================================================================

def update_alert_history(flagged_df: pd.DataFrame, history_file: str) -> pd.DataFrame:
    """
    Reads alert history file, updates with today's alerts, adds 'consecutive_days' 
    column, and saves back to disk.

    Args:
        flagged_df: Flagged dataframe for the current run.
        history_file: Path to the alert_history.csv file.

    Returns:
        Dataframe with 'consecutive_days' column embedded.
    """
    if flagged_df.empty:
        return flagged_df

    df = flagged_df.copy()
    df["consecutive_days"] = 1
    
    if not history_file:
        return df
        
    import os
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    
    if os.path.exists(history_file):
        try:
            history = pd.read_csv(history_file)
            hist_map = history.set_index(["asin", "metric"])["consecutive_days"].to_dict()
            df["consecutive_days"] = df.apply(
                lambda row: hist_map.get((row["asin"], row["metric"]), 0) + 1, axis=1
            )
        except Exception as e:
            print(f"  [History] Error reading alert history: {e}")
            
    # Save the current state as the new history
    try:
        new_history = df[["asin", "metric", "consecutive_days"]].copy()
        new_history.to_csv(history_file, index=False)
    except Exception as e:
        print(f"  [History] Error saving alert history: {e}")
        
    return df
