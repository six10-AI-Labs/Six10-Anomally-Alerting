# backtest.py
# Threshold calibration check — runs detection across ALL historical dates.
#
# Goal: determine whether 99 Criticals/day is typical or an outlier,
# and identify which metrics/tiers/baselines are driving alert volume.
#
# Run from: anomaly_alerting/ directory
#   > python backtest.py

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from ingestion.load_data import load_all_sources
from preprocessing.preprocess import (
    standardize_sellerise, standardize_returns,
    standardize_inventory, standardize_helium10,
    merge_all_sources, assign_tiers, deduplicate,
)
from detection.anomaly_detection import run_detection, get_flagged_rows

# Local test data paths — bypasses Drive and SMTP validation
LOCAL_FOLDERS = {
    "sellerise":  os.path.join(os.path.dirname(__file__), "data", "Sellerise"),
    "returns":    os.path.join(os.path.dirname(__file__), "data", "Returns"),
    "inventory":  os.path.join(os.path.dirname(__file__), "data", "Inventory"),
    "helium10":   os.path.join(os.path.dirname(__file__), "data", "helium10"),
}

# How many days back to include in the summary analysis
LOOKBACK_DAYS = 180

SEV_COLS = ["critical", "warning", "watch"]
DIVIDER  = "-" * 62


def _daily_table(df: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Return a date × severity pivot of daily alert counts."""
    cutoff = df["date"].max() - pd.Timedelta(days=lookback_days)
    window = df[df["date"] >= cutoff]
    pivot = (
        window.groupby(["date", "severity"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=SEV_COLS, fill_value=0)
    )
    pivot["total"] = pivot.sum(axis=1)
    return pivot


def _print_daily_table(pivot: pd.DataFrame) -> None:
    header = f"{'Date':<12} {'Critical':>10} {'Warning':>10} {'Watch':>10} {'Total':>10}"
    print(header)
    print(DIVIDER)
    for dt, row in pivot.iterrows():
        print(
            f"{str(dt.date()):<12}"
            f"{row.get('critical', 0):>10}"
            f"{row.get('warning',  0):>10}"
            f"{row.get('watch',    0):>10}"
            f"{row['total']:>10}"
        )
    print(DIVIDER)
    avg_c = pivot.get("critical", pd.Series(dtype=float)).mean()
    avg_w = pivot.get("warning",  pd.Series(dtype=float)).mean()
    avg_k = pivot.get("watch",    pd.Series(dtype=float)).mean()
    avg_t = pivot["total"].mean()
    max_c = pivot.get("critical", pd.Series(dtype=float)).max()
    max_t = pivot["total"].max()
    print(f"{'Average':<12} {avg_c:>10.1f} {avg_w:>10.1f} {avg_k:>10.1f} {avg_t:>10.1f}")
    print(f"{'Max':<12} {max_c:>10.0f} {'':>10} {'':>10} {max_t:>10.0f}")


def run_backtest() -> None:
    print("\n" + "=" * 62)
    print("  BACKTEST — Threshold Calibration Check")
    print("=" * 62)

    # ------------------------------------------------------------------
    # 1. Load & preprocess (same as production pipeline)
    # ------------------------------------------------------------------
    raw = load_all_sources(LOCAL_FOLDERS)

    print("\nPREPROCESSING")
    sellerise_df = standardize_sellerise(raw["sellerise"])
    returns_df   = standardize_returns(raw["returns"])
    inventory_df = standardize_inventory(raw["inventory"])
    helium10_df  = standardize_helium10(raw["helium10"])

    master_df = merge_all_sources(sellerise_df, returns_df, inventory_df, helium10_df)
    master_df = deduplicate(master_df)
    master_df = assign_tiers(master_df, config.TIER_THRESHOLDS, config.HERO_REVENUE_THRESHOLD)

    # ------------------------------------------------------------------
    # 2. Run detection across ALL dates (no date filter)
    # ------------------------------------------------------------------
    results_df = run_detection(master_df, config)
    flagged_df = get_flagged_rows(results_df)

    if flagged_df.empty:
        print("\nNo alerts detected across full history. Check data or thresholds.")
        return

    date_range = f"{flagged_df['date'].min().date()} to {flagged_df['date'].max().date()}"
    n_days     = flagged_df["date"].nunique()
    print(f"\n  Full history: {date_range}  ({n_days} trading days with data)")

    # Restrict analysis to last LOOKBACK_DAYS
    cutoff       = flagged_df["date"].max() - pd.Timedelta(days=LOOKBACK_DAYS)
    analysis_df  = flagged_df[flagged_df["date"] >= cutoff].copy()
    n_days_window = analysis_df["date"].nunique()

    # ------------------------------------------------------------------
    # 3. Daily alert counts
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  DAILY ALERT COUNTS  (last {LOOKBACK_DAYS} days — {n_days_window} days with data)")
    print(f"{'='*62}")
    pivot = _daily_table(flagged_df, LOOKBACK_DAYS)
    _print_daily_table(pivot)

    avg_daily_critical = pivot.get("critical", pd.Series(dtype=float)).mean()
    if avg_daily_critical > 20:
        print(f"\n  [!!] Average {avg_daily_critical:.0f} Criticals/day -- thresholds likely too loose.")
    elif avg_daily_critical > 10:
        print(f"\n  [~]  Average {avg_daily_critical:.0f} Criticals/day -- borderline, review metric breakdown.")
    else:
        print(f"\n  [OK] Average {avg_daily_critical:.0f} Criticals/day -- within reasonable range.")

    # ------------------------------------------------------------------
    # 4. Breakdown by metric
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  ALERTS BY METRIC  (last {LOOKBACK_DAYS} days)")
    print(f"{'='*62}")
    metric_pivot = (
        analysis_df.groupby(["metric", "severity"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=SEV_COLS, fill_value=0)
    )
    metric_pivot["total"] = metric_pivot.sum(axis=1)
    metric_pivot = metric_pivot.sort_values("total", ascending=False)
    print(metric_pivot.to_string())

    # ------------------------------------------------------------------
    # 5. Breakdown by triggered_by
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  TRIGGERED BY  (last {LOOKBACK_DAYS} days)")
    print(f"{'='*62}")
    trigger_pivot = (
        analysis_df.groupby(["triggered_by", "severity"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=SEV_COLS, fill_value=0)
    )
    trigger_pivot["total"] = trigger_pivot.sum(axis=1)
    print(trigger_pivot.to_string())

    # ------------------------------------------------------------------
    # 6. Critical alerts by tier
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  CRITICAL ALERTS BY TIER  (last {LOOKBACK_DAYS} days)")
    print(f"{'='*62}")
    if "tier" in analysis_df.columns:
        crit_df = analysis_df[analysis_df["severity"] == "critical"]
        tier_counts = crit_df.groupby("tier").size().reindex(
            ["homerun", "triple", "double", "single", "less_than_single"], fill_value=0
        )
        total_crit = tier_counts.sum()
        for tier, count in tier_counts.items():
            pct = (count / total_crit * 100) if total_crit > 0 else 0
            print(f"  {tier:<22} {count:>6}  ({pct:.0f}%)")

    # ------------------------------------------------------------------
    # 7. Top noisy ASINs (most Critical alerts)
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  TOP 10 NOISIEST ASINs — Critical alerts  (last {LOOKBACK_DAYS} days)")
    print(f"{'='*62}")
    crit_df = analysis_df[analysis_df["severity"] == "critical"]
    if not crit_df.empty:
        top_asins = (
            crit_df.groupby(["asin", "tier"])
            .size()
            .reset_index(name="critical_count")
            .sort_values("critical_count", ascending=False)
            .head(10)
        )
        for _, row in top_asins.iterrows():
            print(f"  {row['asin']}  [{row['tier']}]  —  {row['critical_count']} Critical alerts")

    # ------------------------------------------------------------------
    # 8. Return rate absolute threshold check
    # ------------------------------------------------------------------
    print(f"\n{'='*62}")
    print(f"  RETURN RATE ABSOLUTE THRESHOLD CHECK  (last {LOOKBACK_DAYS} days)")
    print(f"{'='*62}")
    rr_abs = analysis_df[
        (analysis_df["metric"] == "return_rate") &
        (analysis_df["triggered_by"] == "absolute_threshold")
    ]
    rr_stat = analysis_df[
        (analysis_df["metric"] == "return_rate") &
        (analysis_df["triggered_by"] != "absolute_threshold")
    ]
    total_rr = len(analysis_df[analysis_df["metric"] == "return_rate"])
    print(f"  Total return_rate alerts : {total_rr}")
    print(f"  Triggered by absolute    : {len(rr_abs)}  ({len(rr_abs)/total_rr*100:.0f}%)" if total_rr else "  No return_rate alerts.")
    print(f"  Triggered by stats only  : {len(rr_stat)}  ({len(rr_stat)/total_rr*100:.0f}%)" if total_rr else "")
    if not rr_abs.empty:
        print(f"\n  Return rate alerts from absolute floor by severity:")
        print(rr_abs["severity"].value_counts().to_string())

    print(f"\n{'='*62}")
    print("  BACKTEST COMPLETE")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    run_backtest()
