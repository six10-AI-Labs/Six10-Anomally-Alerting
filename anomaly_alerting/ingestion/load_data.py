# ingestion/load_data.py
# Layer 1 — reads all raw files from each data source folder into dataframes.
# No preprocessing, column changes, or date filtering happens here.
#
# Folder structure (production Google Drive):
#   Each source folder contains brand subfolders (AquaDoc, Pureauty, etc.)
#   Files inside brand subfolders include the date in their filename.
#
# Date anchoring logic (run on day T):
#   1. Scan Helium10 folder → find latest date available → this is reference_date (T-2)
#   2. Helium10  → load files matching reference_date (2 files for AquaDoc)
#   3. Returns   → load files matching reference_date (fallback: all files)
#   4. Inventory → load files matching reference_date (fallback: most recent)
#   5. Sellerise → load ALL files across all brand subfolders (rolling baseline needs full history)

import os
import re
import tempfile
from datetime import date, datetime
from typing import Optional

import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
import config


def _get_drive_service():
    """Get authenticated Google Drive API service using service account."""
    # Get service account file from config
    from config import SERVICE_ACCOUNT_FILE
    
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)


SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}

# Sellerise percentage columns (raw names before renaming) that must be in
# decimal form (0.088 = 8.8%) for the pipeline to work correctly.
# Sellerise Excel exports are inconsistent: some files format these as Excel
# "Percentage" cells (pandas reads 0.088), others as plain numbers (pandas
# reads 8.8). We normalise per file so the rolling baseline is never computed
# on a mix of scales.
_SELLERISE_PCT_COLS = ["Refund rate %", "Conversion", "Margin", "ACoS", "TACoS"]


def _normalize_sellerise_pct_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure Sellerise percentage columns are in decimal form (0.088 = 8.8%).

    Sellerise exports can deliver these columns either as plain numbers
    (8.8 for 8.8%) or as Excel Percentage-formatted values (0.088 for 8.8%).
    pandas reads the latter as decimals and the former as-is, so the same
    metric ends up on different scales across files.

    Detection: if the maximum absolute value in the column exceeds 1, the
    values are in percentage-number form and need dividing by 100.
    Using max (not median) because several columns — Refund rate %, Margin,
    ACoS, TACoS — have median=0 in typical exports (most rows are zero),
    which caused the old median check to silently skip normalization and
    leave values like 33.58 (ACoS) un-divided, producing wildly wrong
    displayed values (e.g. "3358%").

    Applied per file, before concatenation, so the rolling baseline always
    operates on a consistent scale.
    """
    normalized = []
    for col in _SELLERISE_PCT_COLS:
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        # Use abs().any() > 1 instead of median > 1.
        # Median fails when most rows are zero (e.g. Refund rate %, Margin, ACoS, TACoS
        # all have median=0 in typical exports, so the median check never fires and those
        # columns stay in percentage-number form, causing values like 2168% in the email).
        # If ANY absolute value exceeds 1 the column must be in percentage-number form
        # (a true decimal percentage is always in [−1, 1] for these metrics).
        max_abs = numeric.dropna().abs().max()
        if pd.notna(max_abs) and max_abs > 1:
            df[col] = numeric / 100
            normalized.append(f"{col}(max_abs={max_abs:.2f} /100)")
    if normalized:
        print(f"    [normalize] % form detected, divided by 100: {', '.join(normalized)}")
    return df


# =============================================================================
# STEP 1: File discovery helpers
# =============================================================================

def get_all_files(folder_path: str) -> list:
    """
    Return all .csv and .xlsx file paths in the given folder and one level
    of brand subfolders (production Drive structure).

    Handles both flat folders (local test data) and brand-subfolder layouts.
    Files are returned sorted by path so ordering is deterministic.

    For Google Drive folder IDs (starting with '1'), uses Drive API to list files.

    Args:
        folder_path: Path to the source folder to scan, or Drive folder ID.

    Returns:
        Sorted list of full file paths (local) or (file_id, filename) tuples (Drive).
    """
    # Check if it's a Drive folder ID
    if folder_path.startswith('1') and len(folder_path) > 10:
        return _get_drive_files(folder_path)
    
    files = []

    if not os.path.exists(folder_path):
        print(f"  [WARNING] Folder not found: {folder_path}")
        return []

    for entry in sorted(os.listdir(folder_path)):
        if entry.startswith("."):
            continue
        full_path = os.path.join(folder_path, entry)
        ext = os.path.splitext(entry)[1].lower()

        if os.path.isfile(full_path) and ext in SUPPORTED_EXTENSIONS:
            # File sits directly in source folder (flat / local layout)
            files.append(full_path)

        elif os.path.isdir(full_path):
            # Brand subfolder — scan one level deep
            for sub_entry in sorted(os.listdir(full_path)):
                if sub_entry.startswith("."):
                    continue
                sub_path = os.path.join(full_path, sub_entry)
                sub_ext = os.path.splitext(sub_entry)[1].lower()
                if os.path.isfile(sub_path) and sub_ext in SUPPORTED_EXTENSIONS:
                    files.append(sub_path)

    return files


def _get_drive_files(folder_id: str) -> list:
    """Get all CSV/XLSX files from Drive folder and subfolders."""
    service = _get_drive_service()
    files = []
    
    # Get files directly in folder
    query = f"'{folder_id}' in parents and (mimeType='text/csv' or mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files.extend([(item['id'], item['name']) for item in results.get('files', [])])
    
    # Get subfolders
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    subfolders = results.get('files', [])
    
    # Get files from subfolders
    for subfolder in subfolders:
        sub_id = subfolder['id']
        query = f"'{sub_id}' in parents and (mimeType='text/csv' or mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files.extend([(item['id'], item['name']) for item in results.get('files', [])])
    
    # Sort by name
    files.sort(key=lambda x: x[1])
    return files


def extract_date_from_filename(filename: str) -> Optional[date]:
    """
    Extract a date from a filename using common patterns found in the data exports.

    Patterns tried (in order):
      1. YYYY-MM-DD  e.g. "My Products2026-03-23.xlsx"
      2. Mon DD, YYYY  e.g. "Restock Inventory Mar 23, 2026.csv"

    Args:
        filename: Basename of the file (not the full path).

    Returns:
        Parsed date object, or None if no date found.
    """
    # Pattern 1: YYYY-MM-DD
    m = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass

    # Pattern 2: Mon DD, YYYY  (e.g. "Mar 23, 2026")
    m = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%b %d, %Y").date()
        except ValueError:
            pass

    return None


def get_files_for_date(folder_path: str, target_date: date) -> list:
    """
    Return files from the folder (and brand subfolders) whose filename contains
    target_date.

    Args:
        folder_path: Path to the source folder.
        target_date: The date to match against filenames.

    Returns:
        List of full file paths matching the target date.
    """
    all_files = get_all_files(folder_path)
    return [
        f for f in all_files
        if extract_date_from_filename(f[1] if isinstance(f, tuple) else os.path.basename(f)) == target_date
    ]


def get_latest_date_in_folder(folder_path: str) -> Optional[date]:
    """
    Find the most recent date across all files in the folder and brand subfolders.

    Used to determine reference_date from the Helium10 folder.

    Args:
        folder_path: Path to the folder to scan.

    Returns:
        Most recent date found in any filename, or None if no dates found.
    """
    all_files = get_all_files(folder_path)
    dates = []
    for f in all_files:
        if isinstance(f, tuple):
            filename = f[1]  # (file_id, filename)
        else:
            filename = os.path.basename(f)
        d = extract_date_from_filename(filename)
        if d is not None:
            dates.append(d)
    return max(dates) if dates else None


# =============================================================================
# STEP 2: Generic loader — reads a list of files into a single dataframe
# =============================================================================

def load_files_to_dataframe(file_list: list) -> pd.DataFrame:
    """
    Load a list of .csv and/or .xlsx files and concatenate them into one dataframe.

    For CSV files, attempts UTF-8 encoding first, then falls back to latin1
    (required for Seller Central exports which use Windows-1252 encoding).

    Handles both local file paths and Drive file ID tuples.

    Args:
        file_list: List of full file paths or (file_id, filename) tuples.

    Returns:
        Single concatenated dataframe. Returns empty dataframe if no files loaded.
    """
    dataframes = []

    for file_item in file_list:
        if isinstance(file_item, tuple):
            # Drive file: (file_id, filename)
            file_id, filename = file_item
            temp_path = _download_drive_file(file_id, filename)
            if not temp_path:
                continue
            file_path = temp_path
            file_name = filename
        else:
            # Local file
            file_path = file_item
            file_name = os.path.basename(file_path)

        ext = os.path.splitext(file_name)[1].lower()

        try:
            if ext == ".xlsx":
                df = pd.read_excel(file_path)
                print(f"  Loaded: {file_name} — {len(df):,} rows")
                dataframes.append(df)

            elif ext == ".csv":
                try:
                    df = pd.read_csv(file_path)
                    print(f"  Loaded: {file_name} — {len(df):,} rows")
                    dataframes.append(df)
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding="latin1")
                    print(f"  Loaded: {file_name} — {len(df):,} rows (latin1 encoding)")
                    dataframes.append(df)

        except Exception as e:
            print(f"  [ERROR] Could not load {file_name}: {e}")
            continue

        # Clean up temp file if it was downloaded
        if isinstance(file_item, tuple) and 'temp_path' in locals():
            os.unlink(temp_path)

    if not dataframes:
        print("  [WARNING] No files were successfully loaded. Returning empty dataframe.")
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


def _download_drive_file(file_id: str, filename: str) -> Optional[str]:
    """Download a Drive file to a temporary location."""
    service = _get_drive_service()
    try:
        request = service.files().get_media(fileId=file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
            temp_file.write(request.execute())
            return temp_file.name
    except Exception as e:
        print(f"  [ERROR] Could not download {filename}: {e}")
        return None


# =============================================================================
# STEP 3: Source-specific loaders
# =============================================================================

def load_sellerise_data(folder_path: str) -> pd.DataFrame:
    """
    Load ALL Sellerise 'Product Summary by Day' files from the folder and
    any brand subfolders.

    Sellerise is the core time-series dataset. All historical files are loaded
    every run so the rolling baseline has the full history it needs.
    No date filtering is applied — the detection layer handles date logic.

    Each file is normalised individually (percentage columns → decimal) before
    concatenation to handle inconsistent Excel cell formatting across exports.

    Args:
        folder_path: Path to the Sellerise source folder.

    Returns:
        Raw concatenated dataframe of all Sellerise records, with percentage
        columns normalised to decimal form.
    """
    print("\n[Sellerise] Loading all files (full history)...")
    files = get_all_files(folder_path)

    if not files:
        print("  [WARNING] No Sellerise files found.")
        return pd.DataFrame()

    # Load and normalise each file individually so that % columns are always
    # in decimal form regardless of how the Excel cells were formatted.
    dataframes = []
    for file_item in files:
        if isinstance(file_item, tuple):
            # Drive file: (file_id, filename)
            file_id, filename = file_item
            temp_path = _download_drive_file(file_id, filename)
            if not temp_path:
                continue
            file_path = temp_path
            file_name = filename
        else:
            # Local file
            file_path = file_item
            file_name = os.path.basename(file_path)
        
        try:
            df_file = pd.read_excel(file_path)
            df_file = _normalize_sellerise_pct_columns(df_file)
            print(f"  Loaded: {file_name} — {len(df_file):,} rows")
            dataframes.append(df_file)
        except Exception as e:
            print(f"  [ERROR] Could not load {file_name}: {e}")
        finally:
            # Clean up temp file
            if isinstance(file_item, tuple) and 'temp_path' in locals():
                os.unlink(temp_path)

    if not dataframes:
        print("  [WARNING] No Sellerise files were successfully loaded.")
        return pd.DataFrame()

    df = pd.concat(dataframes, ignore_index=True)
    print(f"  Total: {len(files)} file(s) — {len(df):,} rows combined")
    return df


def load_returns_data(folder_path: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Load FBA Customer Returns Report for the reference date.

    In production: one cumulative returns file per day per brand is uploaded.
    We load the files matching reference_date. If no date-matched files are
    found (e.g. local historical dump with no date in filename), falls back
    to loading all files in the folder.

    Args:
        folder_path: Path to the Returns source folder.
        reference_date: Target date to match filenames against (T-2).

    Returns:
        Raw concatenated dataframe of returns records.
    """
    print(f"\n[Returns] Loading files for {reference_date}...")
    files = []

    if reference_date:
        files = get_files_for_date(folder_path, reference_date)
        if not files:
            print(f"  No files found for {reference_date} — falling back to all files.")
            files = get_all_files(folder_path)
    else:
        files = get_all_files(folder_path)

    if not files:
        print("  [WARNING] No Returns files found.")
        return pd.DataFrame()

    df = load_files_to_dataframe(files)
    print(f"  Total: {len(files)} file(s) — {len(df):,} rows combined")
    return df


def load_inventory_data(folder_path: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Load the Restock/Inventory snapshot for the reference date.

    One point-in-time snapshot per day is uploaded. We load only the file
    matching reference_date. If no exact match is found, falls back to the
    most recently dated file available.

    Args:
        folder_path: Path to the Inventory source folder.
        reference_date: Target date to match filenames against (T-2).

    Returns:
        Raw dataframe of the inventory snapshot for reference_date.
    """
    print(f"\n[Inventory] Loading snapshot for {reference_date}...")
    files = []

    if reference_date:
        files = get_files_for_date(folder_path, reference_date)
        if not files:
            # Fallback: most recent available file
            all_files = get_all_files(folder_path)
            dated = [
                (f, extract_date_from_filename(f[1] if isinstance(f, tuple) else os.path.basename(f)))
                for f in all_files
            ]
            dated = [(f, d) for f, d in dated if d is not None]
            if dated:
                dated.sort(key=lambda x: x[1], reverse=True)
                files = [dated[0][0]]
                selected = files[0]
                selected_name = selected[1] if isinstance(selected, tuple) else os.path.basename(selected)
                print(f"  No file for {reference_date} — using most recent: {selected_name}")
            else:
                files = all_files
    else:
        files = get_all_files(folder_path)

    if not files:
        print("  [WARNING] No Inventory files found.")
        return pd.DataFrame()

    df = load_files_to_dataframe(files)
    print(f"  Total: {len(files)} file(s) — {len(df):,} rows combined")
    return df


def load_helium10_data(folder_path: str, reference_date: Optional[date] = None) -> pd.DataFrame:
    """
    Load Helium10 'My Products' snapshot files for the reference date.

    Helium10 caps exports at 100 products per file, so 2 files per date are
    expected for a ~200 ASIN catalogue. All files for reference_date are loaded
    and concatenated.

    If reference_date is not provided, loads the most recently dated files
    available across all brand subfolders.

    Args:
        folder_path: Path to the Helium10 source folder.
        reference_date: Target date to match filenames against (T-2).

    Returns:
        Raw concatenated dataframe of Helium10 records for reference_date.
    """
    target = reference_date or get_latest_date_in_folder(folder_path)
    print(f"\n[Helium10] Loading snapshot for {target}...")

    if target is None:
        print("  [WARNING] Could not determine target date — loading all files.")
        files = get_all_files(folder_path)
    else:
        files = get_files_for_date(folder_path, target)
        if not files:
            print(f"  [WARNING] No Helium10 files found for {target}.")
            return pd.DataFrame()

    if not files:
        print("  [WARNING] No Helium10 files found.")
        return pd.DataFrame()

    df = load_files_to_dataframe(files)
    print(f"  Total: {len(files)} file(s) — {len(df):,} rows combined")
    return df


# =============================================================================
# STEP 4: Master loader — loads all 4 sources in one call
# =============================================================================

def load_all_sources(drive_folders: dict, reference_date_override=None) -> dict:
    """
    Load all four data sources and return them as a dictionary of dataframes.

    Date anchoring:
      - Scans the Helium10 folder first to find the latest available date
        (reference_date = T-2). All point-in-time sources (Helium10, Returns,
        Inventory) are loaded for that date. Sellerise loads full history.

    Args:
        drive_folders: Dict with keys 'sellerise', 'returns', 'inventory',
                       'helium10' mapping to their folder paths.
        reference_date_override: Optional date object to force the pipeline to 
                                 target a specific historical day.

    Returns:
        Dict with keys 'sellerise', 'returns', 'inventory', 'helium10',
        'reference_date', and 'source_status'.
    """
    print("=" * 60)
    print("INGESTION LAYER — Loading all data sources")
    print("=" * 60)

    # LOOPHOLE FIX: Ensure output directories exist
    os.makedirs(config.EXCEL_OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.HELIUM10_SNAPSHOT_STORE, exist_ok=True)
    os.makedirs(config.DISMISSED_FOLDER, exist_ok=True)

    # Determine reference_date: Use override if provided, else latest Helium10 snapshot
    helium10_folder = drive_folders.get("helium10", "")
    if reference_date_override:
        # If it's a string, convert to date object
        if isinstance(reference_date_override, str):
            from datetime import datetime
            reference_date = datetime.strptime(reference_date_override, "%Y-%m-%d").date()
        else:
            reference_date = reference_date_override
    else:
        reference_date = get_latest_date_in_folder(helium10_folder) if helium10_folder else None

    if reference_date:
        print(f"\n  Reference date (from Helium10): {reference_date}")
    else:
        print("\n  [WARNING] Could not determine reference date from Helium10 folder.")

    data = {
        "sellerise":  load_sellerise_data(drive_folders.get("sellerise", "")),
        "returns":    load_returns_data(drive_folders.get("returns", ""),   reference_date),
        "inventory":  load_inventory_data(drive_folders.get("inventory", ""), reference_date),
        "helium10":   load_helium10_data(drive_folders.get("helium10", ""),  reference_date),
        "reference_date": reference_date,
    }

    # Generate source status for lag reporting
    source_status = {}
    expected_date_str = reference_date.strftime("%Y-%m-%d") if reference_date else "Unknown"
    
    for source in ["sellerise", "returns", "inventory", "helium10"]:
        df = data[source]
        is_missing = df.empty
        
        # Special check for Sellerise which is full history
        if source == "sellerise" and not df.empty:
            # Sellerise doesn't have a 'date' column yet (it's created in preprocessing)
            # but it usually has 'Date' or 'Date (Local Time)'
            date_col = next((c for c in df.columns if 'date' in c.lower()), None)
            if date_col:
                try:
                    # Use a copy to avoid SettingWithCopyWarning
                    temp_dates = pd.to_datetime(df[date_col], errors='coerce').dt.date
                    latest_sellerise = temp_dates.max()
                    if reference_date and latest_sellerise < reference_date:
                        is_missing = True
                except:
                    pass

        status = "OK" if not is_missing else "LAGGING"
        source_status[source] = {
            "status": status,
            "latest_date": expected_date_str if not is_missing else "Missing/Earlier",
            "folder": drive_folders.get(source, "Not Configured")
        }

    data["source_status"] = source_status

    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    for source, df in data.items():
        if isinstance(df, pd.DataFrame):
            print(f"  {source:<12}: {len(df):>7,} rows  |  {len(df.columns)} columns")
    print("=" * 60)

    return data


def get_dismissed_asins(sheet_id: str) -> list:
    """
    Read the list of dismissed ASINs directly from the Google Sheet dashboard.
    Returns a list of ASINs that have 'Dismissed' status.
    """
    if not sheet_id:
        return []

    print(f"\n[Dismissals] Loading list from Google Sheet: {sheet_id}")
    try:
        from googleapiclient.discovery import build
        service = build('sheets', 'v4', credentials=service_account.Credentials.from_service_account_file(
            config.SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
        ))
        
        # Read the first sheet (assumes ASIN is Col A, Status is Col B, DateAdded is Col C)
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range='A:C'
        ).execute()
        
        values = result.get('values', [])
        
        # Scenario: Empty Sheet -> Initialize Headers
        if not values or len(values) == 0:
            print("  Sheet is empty. Initializing headers...")
            header_row = [["ASIN", "Status", "DateAdded", "Notes"]]
            service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range='A1',
                valueInputOption='RAW',
                body={'values': header_row}
            ).execute()
            return {"eternal": [], "temporary": {}}

        # Find ASINs with 'Dismissed' status or 'Added to Board'
        eternal_dismissed = []
        temporary_silence = {} # asin -> date_added_str

        for row in values[1:]: # Skip headers
            if len(row) >= 2:
                asin = row[0].strip()
                status = row[1].strip()
                date_added = row[2].strip() if len(row) >= 3 else ""
                
                if not asin: continue
                
                if status == "Dismissed":
                    eternal_dismissed.append(asin)
                elif status == "Added to Board":
                    temporary_silence[asin] = date_added
        
        print(f"  Dismissal Sync: {len(eternal_dismissed)} Eternal, {len(temporary_silence)} Temporary.")
        return {
            "eternal": eternal_dismissed,
            "temporary": temporary_silence
        }

    except Exception as e:
        print(f"  [ERROR] Failed to read/initialize dismissal Google Sheet: {e}")
        return {"eternal": [], "temporary": {}}
