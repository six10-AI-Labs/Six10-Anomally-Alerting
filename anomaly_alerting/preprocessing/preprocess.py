# preprocessing/preprocess.py
# Layer 2 — standardize, clean, merge all data sources, assign tiers.
# No anomaly detection or date filtering happens here.

import os
import pandas as pd
from datetime import date
from typing import Dict


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first matching column name from the dataframe, case-insensitively."""
    lowered = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
        candidate_lower = candidate.lower()
        if candidate_lower in lowered:
            return lowered[candidate_lower]
    return None


# =============================================================================
# STEP 1: Standardize Sellerise
# =============================================================================

def standardize_sellerise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the raw Sellerise dataframe.

    - Normalizes ASIN (uppercase, stripped)
    - Converts Date from DD-MM-YYYY string to datetime
    - Renames key metric columns to clean internal names
    - Casts metric columns to float

    Args:
        df: Raw Sellerise dataframe from ingestion layer.

    Returns:
        Cleaned Sellerise dataframe with standardized columns and types.
    """
    if df is None or df.empty:
        print("  [Sellerise] No data to standardize — skipping.")
        return pd.DataFrame(columns=["asin", "date", "sales", "margin", "acos", "tacos", "return_rate", "conversion_rate"])
    df = df.copy()

    # Normalize ASIN
    asin_col = _find_column(df, ["ASIN", "asin"])
    if asin_col is None:
        raise ValueError("Sellerise data is missing ASIN column")
    df["asin"] = df[asin_col].astype(str).str.strip().str.upper()

    # Convert date string MM-DD-YYYY → datetime
    # Note: Sellerise exports dates as MM-DD-YYYY (e.g. 01-13-2025 = Jan 13)
    date_col = _find_column(df, ["Date", "date"])
    if date_col is None:
        raise ValueError("Sellerise data is missing Date column")
    df["date"] = pd.to_datetime(df[date_col], format="%m-%d-%Y", errors="coerce")

    # Rename key columns to clean internal names.
    # `asin` and `date` are already normalized explicitly above,
    # so avoid remapping the original ASIN/Date columns to prevent duplicates.
    rename_map = {
        "Refund rate %": "return_rate",
        "Conversion":    "conversion_rate",
        "Sales":         "sales",
        "Margin":        "margin",
        "ACoS":          "acos",
        "TACoS":         "tacos",
        "Net profit":    "net_profit",
        "Sessions":      "sessions",
        "Orders":        "orders",
        "Units":         "units",
        "Refunds qty":   "refund_qty",
        "Refunds $":     "refund_amount",
        "Ad. cost":      "ad_cost",
        "Title":         "title",
    }
    df = df.rename(columns=rename_map)
    if "ASIN" in df.columns:
        df = df.drop(columns=["ASIN"])
    if "Date" in df.columns:
        df = df.drop(columns=["Date"])

    # Cast key metric columns to float
    float_cols = [
        "return_rate", "conversion_rate", "sales", "margin",
        "acos", "tacos", "net_profit", "sessions", "orders",
        "units", "refund_qty", "refund_amount", "ad_cost",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where ASIN or date is null (unparseable rows)
    df = df.dropna(subset=["asin", "date"])

    print(f"  [Sellerise] Standardized: {len(df):,} rows | {df['asin'].nunique()} unique ASINs")
    return df


# =============================================================================
# STEP 2: Standardize Returns
# =============================================================================

def standardize_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize and aggregate the raw Returns dataframe to ASIN + date level.

    - Normalizes ASIN
    - Parses return-date ISO timestamp → date only
    - Aggregates: return_count (sum of quantity), top_return_reason (mode)

    Args:
        df: Raw returns dataframe from ingestion layer.

    Returns:
        Aggregated dataframe with one row per ASIN + date.
        Columns: asin, date, return_count, top_return_reason
    """
    if df is None or df.empty:
        print("  [Returns] No data to standardize — skipping.")
        return pd.DataFrame(columns=["asin", "date", "return_count", "top_return_reason"])
    df = df.copy()

    # Normalize ASIN
    df["asin"] = df["asin"].astype(str).str.strip().str.upper()

    # Parse ISO timestamp → date only
    df["date"] = pd.to_datetime(df["return-date"], errors="coerce").dt.date
    df["date"] = pd.to_datetime(df["date"])

    # Drop rows with unparseable dates or ASINs
    df = df.dropna(subset=["asin", "date"])

    # Aggregate to ASIN + date level
    def top_reason(series):
        return series.mode().iloc[0] if not series.mode().empty else None

    aggregated = df.groupby(["asin", "date"], as_index=False).agg(
        return_count=("quantity", "sum"),
        top_return_reason=("reason", top_reason),
    )

    print(f"  [Returns] Standardized: {len(aggregated):,} rows | {aggregated['asin'].nunique()} unique ASINs")
    return aggregated


# =============================================================================
# STEP 3: Standardize Inventory
# =============================================================================

def standardize_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the raw Inventory snapshot dataframe.

    - Normalizes ASIN
    - Keeps only relevant columns for alerting context
    - Ensures one row per ASIN (deduplicates)

    Args:
        df: Raw inventory dataframe from ingestion layer.

    Returns:
        Cleaned inventory dataframe with one row per ASIN.
        Columns: asin, total_units, available_units, days_of_supply, inventory_alert, recommended_replenishment_qty
    """
    if df is None or df.empty:
        print("  [Inventory] No data to standardize — skipping.")
        return pd.DataFrame(columns=["asin", "total_units", "available_units", "days_of_supply", "inventory_alert", "recommended_replenishment_qty"])
    df = df.copy()

    # Normalize ASIN
    asin_col = _find_column(df, ["ASIN", "asin", "SKU", "sku", "asin/sku"])
    if asin_col is None:
        raise ValueError("Inventory data is missing ASIN/SKU column")
    df["asin"] = df[asin_col].astype(str).str.strip().str.upper()

    # Keep only relevant columns
    keep = {
        "Total Units":                                             "total_units",
        "Available":                                               "available_units",
        "Days of Supply at Amazon Fulfillment Network":            "days_of_supply",
        "Alert":                                                   "inventory_alert",
        "Recommended replenishment qty":                           "recommended_replenishment_qty",
    }
    existing = {}
    for source_col, target_col in keep.items():
        found = _find_column(df, [source_col])
        if found is not None:
            existing[found] = target_col
    existing["asin"] = "asin"
    df = df[list(existing.keys())].rename(columns=existing)

    # One row per ASIN
    df = df.drop_duplicates(subset=["asin"], keep="last")

    print(f"  [Inventory] Standardized: {len(df):,} rows | {df['asin'].nunique()} unique ASINs")
    return df


# =============================================================================
# STEP 4: Standardize Helium10
# =============================================================================

def standardize_helium10(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the raw Helium10 snapshot dataframe.

    - Normalizes ASIN
    - Keeps relevant columns: BSR, reviews, keyword metrics
    - Deduplicates by ASIN (handles multi-file concatenation)

    Args:
        df: Raw Helium10 dataframe from ingestion layer.

    Returns:
        Cleaned Helium10 dataframe with one row per ASIN.
        Columns: asin, category_bsr, subcategory_bsr, keyword_avg_rank,
                 keyword_avg_rank_trend, organic_top10_count,
                 organic_top10_search_volume, review_rating, review_count
    """
    if df is None or df.empty:
        print("  [Helium10] No data to standardize — skipping.")
        return pd.DataFrame(columns=["asin", "category_bsr", "subcategory_bsr", "review_rating", "review_count"])
    df = df.copy()

    # Normalize ASIN
    asin_col = _find_column(df, ["ASIN", "asin"])
    if asin_col is None:
        raise ValueError("Helium10 data is missing ASIN column")
    df["asin"] = df[asin_col].astype(str).str.strip().str.upper()

    # Keep only relevant columns
    keep = {
        "Category BSR":                      "category_bsr",
        "Subcategory BSR":                   "subcategory_bsr",
        "Keywords Average Rank":             "keyword_avg_rank",
        "Keywords Average Rank Trend":       "keyword_avg_rank_trend",
        "Organic Top 10":                    "organic_top10_count",
        "Organic Top 10 Search Volume":      "organic_top10_search_volume",
        "Reviews Rating":                    "review_rating",
        "Review Count":                      "review_count",
    }
    existing = {"asin": "asin"}
    for source_col, target_col in keep.items():
        found = _find_column(df, [source_col])
        if found is not None:
            existing[found] = target_col
    df = df[list(existing.keys())].rename(columns=existing)

    # Deduplicate by ASIN (keep last in case of overlap across files)
    df = df.drop_duplicates(subset=["asin"], keep="last")

    print(f"  [Helium10] Standardized: {len(df):,} rows | {df['asin'].nunique()} unique ASINs")
    return df


# =============================================================================
# STEP 5: Merge All Sources
# =============================================================================

def merge_all_sources(sellerise: pd.DataFrame, returns: pd.DataFrame,
                      inventory: pd.DataFrame, helium10: pd.DataFrame) -> pd.DataFrame:
    """
    Merge all four standardized sources into a single master dataframe.

    - Sellerise is the base (no rows dropped)
    - Returns joined on ASIN + date (adds return_count, top_return_reason)
    - Inventory joined on ASIN only (adds current inventory snapshot columns)
    - Helium10 joined on ASIN only (adds BSR, reviews, keyword columns)

    Missing joins result in NaN — handled gracefully, no rows dropped.

    Args:
        sellerise: Standardized Sellerise dataframe.
        returns: Standardized returns dataframe (ASIN + date aggregated).
        inventory: Standardized inventory snapshot dataframe.
        helium10: Standardized Helium10 snapshot dataframe.

    Returns:
        Master dataframe with all sources merged. Row count = Sellerise row count.
    """
    print(f"  [Merge] Starting with Sellerise base: {len(sellerise):,} rows")

    # Left join returns on ASIN + date
    df = sellerise.merge(returns, on=["asin", "date"], how="left")
    print(f"  [Merge] After returns join: {len(df):,} rows")

    # Left join inventory on ASIN only (snapshot — no date)
    df = df.merge(inventory, on="asin", how="left")
    print(f"  [Merge] After inventory join: {len(df):,} rows")

    # Left join helium10 on ASIN only (snapshot — no date)
    df = df.merge(helium10, on="asin", how="left")
    print(f"  [Merge] After helium10 join: {len(df):,} rows")

    return df


# =============================================================================
# STEP 6: Assign Baseball Tiers
# =============================================================================

def assign_tiers(df: pd.DataFrame, tier_thresholds: dict,
                 hero_revenue_threshold: float = 500_000) -> pd.DataFrame:
    """
    Compute trailing 12-month revenue per ASIN and assign a baseball tier label.
    Also adds an 'is_hero' column based on revenue >= hero_revenue_threshold.

    Uses the most recent 365 days of data available in the dataframe.
    Adds 'tier', 'trailing_12m_revenue', and 'is_hero' columns to the master dataframe.

    Args:
        df: Master merged dataframe with 'asin', 'date', and 'sales' columns.
        tier_thresholds: Dict of tier labels to revenue cutoffs (from config.TIER_THRESHOLDS).
        hero_revenue_threshold: Revenue cutoff for hero ASIN classification (from config.HERO_REVENUE_THRESHOLD).

    Returns:
        Dataframe with new 'tier', 'trailing_12m_revenue', and 'is_hero' columns.
    """
    df = df.copy()

    # Compute trailing 12-month cutoff from latest date in data
    latest_date = df["date"].max()
    cutoff_date = latest_date - pd.DateOffset(months=12)

    trailing = (
        df[df["date"] >= cutoff_date]
        .groupby("asin")["sales"]
        .sum()
        .reset_index()
        .rename(columns={"sales": "trailing_12m_revenue"})
    )

    def get_tier(revenue):
        if revenue >= tier_thresholds.get("homerun", float("inf")):
            return "homerun"
        elif revenue >= tier_thresholds.get("triple", float("inf")):
            return "triple"
        elif revenue >= tier_thresholds.get("double", float("inf")):
            return "double"
        elif revenue >= tier_thresholds.get("single", float("inf")):
            return "single"
        else:
            return "less_than_single"

    trailing["tier"] = trailing["trailing_12m_revenue"].apply(get_tier)
    trailing["is_hero"] = trailing["trailing_12m_revenue"] >= hero_revenue_threshold

    # Join tier, trailing_12m_revenue, and is_hero back to master dataframe
    df = df.merge(trailing[["asin", "tier", "trailing_12m_revenue", "is_hero"]], on="asin", how="left")
    df["tier"] = df["tier"].fillna("less_than_single")
    df["is_hero"] = df["is_hero"].fillna(False).astype(bool)

    tier_counts = df.drop_duplicates("asin")["tier"].value_counts().to_dict()
    hero_count = df.drop_duplicates("asin")["is_hero"].sum()
    print(f"  [Tiers] Assigned: {tier_counts} | Hero ASINs (>=${hero_revenue_threshold:,.0f}): {hero_count}")
    return df


# =============================================================================
# STEP 7: Deduplicate
# =============================================================================

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate ASIN + date rows from the master dataframe.

    Duplicates can occur when overlapping date ranges are uploaded across
    multiple Sellerise files. Keeps the last occurrence.

    Args:
        df: Master merged dataframe potentially containing duplicates.

    Returns:
        Deduplicated dataframe.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["asin", "date"], keep="last")
    removed = before - len(df)

    if removed > 0:
        print(f"  [Deduplicate] Removed {removed:,} duplicate rows. Remaining: {len(df):,}")
    else:
        print(f"  [Deduplicate] No duplicates found. Rows: {len(df):,}")

    return df


# =============================================================================
# HELIUM10 SNAPSHOT STORAGE
# =============================================================================

def save_helium10_snapshot(df: pd.DataFrame, store_path: str, run_date: str = None) -> None:
    """
    Save today's standardized Helium10 snapshot to the accumulation folder.

    Adds a 'snapshot_date' column so future detection knows which day each
    row belongs to. Saves as a date-stamped CSV file.

    Skips silently if store_path is empty (not yet configured).

    Called every time the pipeline runs — this is how we build a BSR and
    keyword ranking time series over time. Once 30+ days are accumulated,
    rolling baseline detection on BSR/keywords becomes possible (v2).

    Args:
        df: Standardized Helium10 dataframe from standardize_helium10().
        store_path: Path to the folder where snapshots are accumulated
                    (config.HELIUM10_SNAPSHOT_STORE).
        run_date: Date string YYYY-MM-DD. Defaults to today if not provided.
    """
    if not store_path:
        print("  [Helium10 Snapshot] HELIUM10_SNAPSHOT_STORE not configured — skipping save.")
        return

    os.makedirs(store_path, exist_ok=True)

    snapshot_date = run_date or date.today().strftime("%Y-%m-%d")
    df = df.copy()
    df["snapshot_date"] = snapshot_date

    file_path = os.path.join(store_path, f"helium10_snapshot_{snapshot_date}.csv")

    if os.path.exists(file_path):
        print(f"  [Helium10 Snapshot] Snapshot for {snapshot_date} already exists — skipping.")
        return

    df.to_csv(file_path, index=False)
    print(f"  [Helium10 Snapshot] Saved: {file_path} — {len(df):,} rows")


def load_helium10_history(store_path: str) -> pd.DataFrame:
    """
    Load all accumulated Helium10 snapshots into a single time-series dataframe.

    Each snapshot file contains one day's BSR, keyword rank, and review data
    for all ASINs. Concatenating them builds a historical time series that
    enables rolling and YoY detection on BSR/keywords (v2 detection).

    Returns an empty dataframe if store_path is not configured or no files exist yet.

    Args:
        store_path: Path to the accumulation folder (config.HELIUM10_SNAPSHOT_STORE).

    Returns:
        Concatenated dataframe with 'snapshot_date' column, sorted by asin + snapshot_date.
        Empty dataframe if no snapshots exist yet.
    """
    if not store_path or not os.path.exists(store_path):
        print("  [Helium10 History] No snapshot store found — BSR history not available yet.")
        return pd.DataFrame()

    files = sorted([
        os.path.join(store_path, f)
        for f in os.listdir(store_path)
        if f.startswith("helium10_snapshot_") and f.endswith(".csv")
    ])

    if not files:
        print("  [Helium10 History] No snapshots accumulated yet.")
        return pd.DataFrame()

    dfs = [pd.read_csv(f) for f in files]
    history = pd.concat(dfs, ignore_index=True)
    history["snapshot_date"] = pd.to_datetime(history["snapshot_date"])
    history = history.sort_values(["asin", "snapshot_date"]).reset_index(drop=True)

    print(f"  [Helium10 History] Loaded {len(files)} snapshots | "
          f"{len(history):,} rows | "
          f"Date range: {history['snapshot_date'].min().date()} to {history['snapshot_date'].max().date()}")
    return history


# =============================================================================
# STEP 8: Full Preprocessing Pipeline
# =============================================================================

def run_preprocessing(data_dict: Dict[str, pd.DataFrame], config,
                       run_date: str = None) -> pd.DataFrame:
    """
    Run the full preprocessing pipeline in order.

    Steps:
        1. Standardize each source
        2. Save Helium10 snapshot to accumulation store (builds BSR time series over time)
        3. Merge all into master dataframe
        4. Deduplicate
        5. Assign baseball tiers

    The Helium10 snapshot is saved on every run. Once 30+ daily snapshots
    accumulate, rolling baseline detection on BSR/keywords becomes possible.
    Once 365+ days accumulate, YoY detection on BSR/keywords is available (v2).

    Args:
        data_dict: Dict with keys 'sellerise', 'returns', 'inventory', 'helium10'
                   as returned by ingestion.load_all_sources().
        config: The config module — used for TIER_THRESHOLDS and HELIUM10_SNAPSHOT_STORE.
        run_date: Optional date string YYYY-MM-DD for the snapshot filename.
                  Defaults to today's date.

    Returns:
        Final clean master dataframe ready for anomaly detection.
    """
    print("\n" + "=" * 50)
    print("PREPROCESSING LAYER — Standardizing and merging")
    print("=" * 50)

    sellerise  = standardize_sellerise(data_dict["sellerise"])
    returns    = standardize_returns(data_dict["returns"])
    inventory  = standardize_inventory(data_dict["inventory"])
    helium10   = standardize_helium10(data_dict["helium10"])

    # Save today's Helium10 snapshot — accumulates BSR/keyword history over time
    print()
    save_helium10_snapshot(helium10, config.HELIUM10_SNAPSHOT_STORE, run_date)

    print()
    master = merge_all_sources(sellerise, returns, inventory, helium10)

    print()
    master = deduplicate(master)

    print()
    master = assign_tiers(master, config.TIER_THRESHOLDS, config.HERO_REVENUE_THRESHOLD)

    print("\n" + "=" * 50)
    print("PREPROCESSING COMPLETE")
    print(f"  Final shape : {master.shape}")
    print(f"  Date range  : {master['date'].min().date()} to {master['date'].max().date()}")
    print(f"  Unique ASINs: {master['asin'].nunique()}")
    print("=" * 50)

    return master
