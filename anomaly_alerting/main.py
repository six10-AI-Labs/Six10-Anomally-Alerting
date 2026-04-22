# main.py
# Entry point — orchestrates the full anomaly detection and alerting pipeline.
# In Google Colab: click "Run All" to execute end-to-end.

import os
import argparse
from datetime import date, datetime

import pandas as pd
import config
from ingestion.load_data import load_all_sources, get_dismissed_asins
from preprocessing.preprocess import (
    standardize_sellerise, standardize_returns,
    standardize_inventory, standardize_helium10,
    merge_all_sources, assign_tiers, deduplicate,
    save_helium10_snapshot,
)
from detection.anomaly_detection import run_detection, get_flagged_rows
from alerting.alert_builder import build_alert_payload, filter_alerts
from alerting.email_sender import send_email, log_send_result


def get_run_date() -> str:
    """
    Return today's run date as a string in YYYY-MM-DD format.

    Note: Data covers through T-2 due to Amazon's 48-hour session/conversion
    data population lag. The run date reflects when the pipeline was executed,
    not the latest data date.

    Returns:
        Today's date string (YYYY-MM-DD).
    """
    return date.today().strftime("%Y-%m-%d")


def validate_config() -> None:
    """
    Basic sanity check on config.py before the pipeline runs.

    Verifies:
      - Service account file exists (for local runs)
      - Email configuration is set
      - Drive folder IDs are configured
      - Recipient email is set

    Raises:
        ValueError: If any required config field is missing or invalid.
    """
    missing = []
    import os

    # Check if running in Colab
    is_colab = config._is_colab()

    # For local runs: verify service account file exists
    if not is_colab:
        service_account_file = config.SERVICE_ACCOUNT_FILE
        if not service_account_file or not os.path.exists(service_account_file):
            missing.append(f"SERVICE_ACCOUNT_FILE: '{service_account_file}' (file not found)")

    # Check email configuration
    if not config.GMAIL_CONFIG.get("sender_email"):
        missing.append("GMAIL_CONFIG['sender_email'] (set in .env or config.py)")
    if not config.GMAIL_CONFIG.get("recipient_emails"):
        missing.append("GMAIL_CONFIG['recipient_emails'] (set in .env or config.py)")

    # Check Drive folders
    for key, val in config.DRIVE_FOLDERS.items():
        if not val:
            missing.append(f"DRIVE_FOLDERS['{key}']")

    if missing:
        raise ValueError(
            "config.py is missing or invalid required values — fix these before running:\n" +
            "\n".join(f"  - {m}" for m in missing)
        )


def export_alerts_to_excel(flagged_df: pd.DataFrame, run_date: str) -> str:
    """
    Save the full detection output (all flagged alerts, before email filtering)
    to a dated Excel file. Returns the file path.
    """
    output_dir = config.EXCEL_OUTPUT_DIR if config.EXCEL_OUTPUT_DIR else "."
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"alerts_{run_date}.xlsx")

    export_df = flagged_df.copy()
    # Round float columns for readability
    for col in ["actual_value", "expected_value", "yoy_baseline", "z_score", "yoy_deviation"]:
        if col in export_df.columns:
            export_df[col] = export_df[col].round(4)

    export_df.to_excel(filepath, index=False)
    print(f"  [Export] Full alert output saved: {filepath} ({len(export_df):,} rows)")
    return filepath


def run_pipeline(target_date: str = None) -> None:
    """
    Execute the full anomaly detection and alerting pipeline end-to-end.

    Steps:
        1. Validate config.py — fail fast if credentials or paths are missing.
        2. Prompt the runner for their recipient email address.
        3. Load raw data from all four Google Drive source folders.
        4. Standardize and clean each data source.
        5. Merge all sources into a single master dataframe.
        6. Deduplicate and assign baseball tiers to each ASIN.
        7. Save today's Helium10 snapshot for BSR time-series accumulation.
        8. Run anomaly detection across all monitored metrics.
        9. Filter flagged rows by tier/severity config flags.
        10. Build the email subject and body from the filtered alerts.
        11. Send the daily digest email to the recipient.
        12. Log the send result to the console.
    """
    run_date = get_run_date()
    print(f"\n{'='*60}")
    print(f"  SIX10 ANOMALY DETECTION PIPELINE — {run_date}")
    print(f"{'='*60}\n")

    # Step 1: Validate config
    validate_config()

    # Step 2: Set recipients from config
    recipients = config.GMAIL_CONFIG["recipient_emails"]

    # Step 3: Load raw data
    raw = load_all_sources(config.DRIVE_FOLDERS, reference_date_override=target_date)
    reference_date = raw.pop("reference_date")  # Helium10-anchored T-2 date
    source_status  = raw.pop("source_status", {})

    # Step 4: Standardize each source
    print("\nPREPROCESSING")
    sellerise_df = standardize_sellerise(raw["sellerise"])
    returns_df   = standardize_returns(raw["returns"])
    inventory_df = standardize_inventory(raw["inventory"])
    helium10_df  = standardize_helium10(raw["helium10"])

    # Step 5–6: Merge, deduplicate, assign tiers
    master_df = merge_all_sources(sellerise_df, returns_df, inventory_df, helium10_df)
    master_df = deduplicate(master_df)
    master_df = assign_tiers(master_df, config.TIER_THRESHOLDS, config.HERO_REVENUE_THRESHOLD)

    # Step 7: Accumulate Helium10 snapshot (BSR time series — v2 detection)
    if config.HELIUM10_SNAPSHOT_STORE:
        save_helium10_snapshot(helium10_df, config.HELIUM10_SNAPSHOT_STORE, run_date)

    # Step 8: Run detection
    print("\nDETECTION")
    results_df = run_detection(master_df, config)

    # Step 7.5: Run Helium10 snapshot metric detection
    from preprocessing.preprocess import load_helium10_history
    from detection.anomaly_detection import run_helium10_detection
    if config.HELIUM10_SNAPSHOT_STORE:
        h10_history = load_helium10_history(config.HELIUM10_SNAPSHOT_STORE)
        h10_results = run_helium10_detection(h10_history, master_df, config)
        if not h10_results.empty:
            results_df = pd.concat([results_df, h10_results], ignore_index=True)
            print(f"  Added {len(h10_results):,} Helium10 metric alerts to detection output.")

    flagged_df = get_flagged_rows(results_df)

    # Step 8.1: Update alert history for unresolved tracking across runs
    if getattr(config, "ALERT_HISTORY_FILE", None):
        from detection.anomaly_detection import update_alert_history
        flagged_df = update_alert_history(flagged_df, config.ALERT_HISTORY_FILE)

    # Filter to the latest date that actually has alerts.
    # We do NOT hard-anchor to Helium10 reference_date, because if a user uploads
    # Helium10 data for April 15 but Sellerise data only up to April 8, a hard
    # anchor would result in 0 alerts for April 15 (since Sellerise metrics are NaN).
    data_date = None
    if not flagged_df.empty:
        anchor = flagged_df["date"].max()
        data_date = anchor.strftime("%Y-%m-%d")
        flagged_df = flagged_df[flagged_df["date"] == anchor].reset_index(drop=True)
        print(f"  Data date (Latest available with alerts): {anchor.date()} ({len(flagged_df)} raw alerts)")

    # Step 8.5: Export full detection output to Excel (before email filtering)
    if not flagged_df.empty:
        export_alerts_to_excel(flagged_df, run_date)

    # Step 9: Apply noise filters (tier/severity suppression + Google Sheet mutes)
    dismissed_data = get_dismissed_asins(config.DISMISSAL_SHEET_ID)
    flagged_df = filter_alerts(flagged_df, dismissed_asins=dismissed_data)
    
    # Identify Persistent ASINs (Added to board > 30 days ago) for special notes
    persistent_asins = {}
    if isinstance(dismissed_data, dict):
        temporary = dismissed_data.get("temporary", {})
        try:
            run_dt = datetime.strptime(run_date, "%Y-%m-%d")
        except:
            run_dt = datetime.now()
            
        for asin, added_str in temporary.items():
            try:
                added_dt = datetime.strptime(added_str, "%Y-%m-%d")
                if (run_dt - added_dt).days > 30:
                    persistent_asins[asin] = {"date": added_str}
            except:
                pass

    n_alerts = len(flagged_df) if not flagged_df.empty else 0
    print(f"  {n_alerts} alerts after filtering.")

    # Step 9.5: LLM Context Validation & Summarization
    llm_summary = None
    if getattr(config, "USE_LLM_ENHANCEMENTS", False) and os.getenv("ANTHROPIC_API_KEY"):
        try:
            from alerting.llm_assistant import run_llm_analysis
            print("\nLLM ENHANCEMENT")
            flagged_df, llm_summary = run_llm_analysis(flagged_df, source_status, run_date, data_date)
        except Exception as e:
            print(f"  [CRITICAL ERROR] LLM Enhancement failed: {e}")
            print("  Continuing with standard report...")
            llm_summary = None

    # Step 10: Build email
    print("\nALERTING")
    payload = build_alert_payload(flagged_df, run_date, data_date=data_date, source_status=source_status, llm_summary=llm_summary, persistent_asins=persistent_asins)

    # Step 11: Send
    success = send_email(
        recipients=recipients,
        subject=payload["subject"],
        body=payload["body"],
        content_type=payload.get("content_type", "html"),
    )

    # Step 12: Log result
    log_send_result(success, recipients, run_date)

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE — {run_date}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Six10 Anomaly Detection Pipeline")
    parser.add_argument("--date", type=str, help="Target date for the run (YYYY-MM-DD). If omitted, uses latest data.")
    args = parser.parse_args()
    
    run_pipeline(target_date=args.date)
