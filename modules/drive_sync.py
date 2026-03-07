"""
drive_sync.py - Google Drive Integration for DALA Analytics

Automatically watches folders, detects new/modified Excel files,
and runs the full generation pipeline (KPIs, alerts, DB save).

Auth hierarchy (same as sheets.py):
  1. google_token.json  — OAuth2 user token (preferred)
  2. google_credentials.json — Service account (fallback)

Folders watched:
  2025: 1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD
  2026: 1dLbLm-O66ySffXUHlmAsHNIiayKHkEol
"""

import os
import io
import re
import json
import time
import calendar as _calendar
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# Google API
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2 import service_account

import pandas as pd

# DALA modules
from .data_store import DataStore
from .ingestion import load_and_clean, filter_by_date, split_by_brand

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

DRIVE_FOLDERS = [
    {
        'name': '2025 Sales Reports',
        'id': '1I6b9ytn6XR0QHtr9tBRzXXe1xhMKW7dD',
        'year': 2025,
    },
    {
        'name': '2026 Sales Reports',
        'id': '1dLbLm-O66ySffXUHlmAsHNIiayKHkEol',
        'year': 2026,
    },
]

# ── Drive folder helpers ───────────────────────────────────────────────────────

_MONTH_NAMES = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5,     'june': 6,     'july': 7,  'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


def _extract_month_from_folder_name(folder_name: str) -> Optional[int]:
    """Return month number (1-12) if a month name appears in the folder name, else None."""
    name_lower = folder_name.lower()
    for mname, mnum in _MONTH_NAMES.items():
        if mname in name_lower:
            return mnum
    return None


def _extract_brand_from_filename(filename: str) -> str:
    """
    Extract brand name from a file like 'B-Boom August Sales Report Week 1.xlsx'.
    Strips everything from the first whole-word month name onwards,
    and also strips trailing ' Sales Report...' suffixes.
    """
    name = os.path.splitext(filename)[0]
    # Remove " Sales Report ..." suffix
    name = re.sub(r'\s+Sales\s+Report.*$', '', name, flags=re.IGNORECASE)
    # Remove month name (whole word) and everything after it
    months_pat = (
        r'\b(January|February|March|April|May|June|July|'
        r'August|September|October|November|December)\b.*$'
    )
    name = re.sub(months_pat, '', name, flags=re.IGNORECASE)
    return name.strip()


# ── Auth helper ───────────────────────────────────────────────────────────────

def _get_drive_service():
    """
    Build an authenticated Google Drive API service.
    Tries OAuth2 (google_token.json) first, then service account.
    Raises RuntimeError if no credentials found.
    """
    token_path  = os.path.join(BASE_DIR, 'google_token.json')
    
    # Support GOOGLE_APPLICATION_CREDENTIALS env var (Railway)
    sa_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', os.path.join(BASE_DIR, 'google_credentials.json'))

    # ── 1. OAuth2 user token ─────────────────────────────────────────────────
    if os.path.isfile(token_path):
        from google.oauth2.credentials import Credentials as OAuthCreds
        with open(token_path, encoding='utf-8') as f:
            token_data = json.load(f)
        creds = OAuthCreds.from_authorized_user_info(token_data, SCOPES)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(token_path, 'w', encoding='utf-8') as f:
                    f.write(creds.to_json())
        if creds.valid:
            return build('drive', 'v3', credentials=creds, cache_discovery=False)

    # ── 2. Service account fallback ──────────────────────────────────────────
    # Also support GOOGLE_CREDENTIALS_JSON env var for Railway
    if not os.path.isfile(sa_path):
        creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
        if creds_json:
            # Ensure directory exists
            os.makedirs(os.path.dirname(sa_path), exist_ok=True)
            with open(sa_path, 'w', encoding='utf-8') as f:
                f.write(creds_json)
            print(f"[DriveSync] Credentials written to {sa_path}")

    if os.path.isfile(sa_path):
        creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        return build('drive', 'v3', credentials=creds, cache_discovery=False)

    raise RuntimeError(
        "No Google credentials found for Drive access.\n"
        "Run `python setup_oauth.py` to set up OAuth2, or place a service account "
        "JSON at google_credentials.json in the project root."
    )


def drive_available() -> bool:
    """Returns True if Drive credentials are present (doesn't test connection)."""
    token_path = os.path.join(BASE_DIR, 'google_token.json')
    sa_path    = os.path.join(BASE_DIR, 'google_credentials.json')
    return os.path.isfile(token_path) or os.path.isfile(sa_path) or \
           bool(os.environ.get('GOOGLE_CREDENTIALS_JSON'))


# ── Google Drive API wrapper ──────────────────────────────────────────────────

class DriveService:
    """Thin wrapper around the Google Drive v3 API."""

    def __init__(self):
        self.service = _get_drive_service()

    def list_excel_files(self, folder_id: str) -> List[Dict]:
        """List all Excel files (.xls / .xlsx) in a folder."""
        # Match both old .xls and new .xlsx MIME types
        q = (
            f"'{folder_id}' in parents and trashed=false and ("
            "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
            "mimeType='application/vnd.ms-excel'"
            ")"
        )
        files, page_token = [], None
        while True:
            resp = self.service.files().list(
                q=q,
                spaces='drive',
                fields='nextPageToken, files(id, name, modifiedTime, size)',
                pageToken=page_token,
                orderBy='name',
            ).execute()
            files.extend(resp.get('files', []))
            page_token = resp.get('nextPageToken')
            if not page_token:
                break
        return files

    def list_subfolders(self, folder_id: str) -> List[Dict]:
        """List immediate subfolders of a folder."""
        q = (
            f"'{folder_id}' in parents and trashed=false and "
            "mimeType='application/vnd.google-apps.folder'"
        )
        resp = self.service.files().list(
            q=q, spaces='drive',
            fields='files(id, name)',
            pageSize=200,
        ).execute()
        return resp.get('files', [])

    def list_excel_recursive(self, folder_id: str) -> List[Dict]:
        """Recursively find all Excel files under folder_id (any depth)."""
        results = list(self.list_excel_files(folder_id))
        for sf in self.list_subfolders(folder_id):
            results.extend(self.list_excel_recursive(sf['id']))
        return results

    def download_file(self, file_id: str) -> io.BytesIO:
        """Download a Drive file to an in-memory buffer."""
        request = self.service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return buf


# ── Sync State (SQLite-backed via DataStore) ───────────────────────────────────

class SyncState:
    """
    Tracks which Drive files have been imported.
    State is persisted in the DataStore SQLite DB (drive_sync_files table),
    with a JSON file fallback for legacy compatibility.
    """

    def __init__(self, ds: DataStore = None):
        self.ds = ds or DataStore()
        self._ensure_table()
        # Also keep in-memory cache for the current session
        self._cache: Dict[str, Dict] = self._load_from_db()

    def _ensure_table(self):
        with self.ds._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS drive_sync_files (
                    file_id      TEXT PRIMARY KEY,
                    folder_id    TEXT,
                    file_name    TEXT NOT NULL,
                    modified_time TEXT NOT NULL,
                    status       TEXT DEFAULT 'pending',
                    report_id    INTEGER,
                    error_msg    TEXT,
                    imported_at  TEXT,
                    updated_at   TEXT NOT NULL
                )
            """)

    def _load_from_db(self) -> Dict[str, Dict]:
        with self.ds._connect() as conn:
            rows = conn.execute("SELECT * FROM drive_sync_files").fetchall()
        return {r['file_id']: dict(r) for r in rows}

    def is_imported(self, file_id: str, modified_time: str) -> bool:
        row = self._cache.get(file_id)
        if not row:
            return False
        return row['modified_time'] == modified_time and row['status'] == 'imported'

    def mark_imported(self, file_id: str, folder_id: str, file_name: str,
                      modified_time: str, report_id: int = None):
        now = datetime.now().isoformat(timespec='seconds')
        with self.ds._connect() as conn:
            conn.execute("""
                INSERT INTO drive_sync_files
                    (file_id, folder_id, file_name, modified_time, status, report_id, imported_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET
                    modified_time=excluded.modified_time,
                    status='imported', report_id=excluded.report_id,
                    imported_at=excluded.imported_at, updated_at=excluded.updated_at
            """, (file_id, folder_id, file_name, modified_time, 'imported', report_id, now, now))
        self._cache[file_id] = {
            'file_id': file_id, 'folder_id': folder_id, 'file_name': file_name,
            'modified_time': modified_time, 'status': 'imported',
            'report_id': report_id, 'imported_at': now, 'updated_at': now,
        }

    def mark_error(self, file_id: str, folder_id: str, file_name: str,
                   modified_time: str, error_msg: str):
        now = datetime.now().isoformat(timespec='seconds')
        with self.ds._connect() as conn:
            conn.execute("""
                INSERT INTO drive_sync_files
                    (file_id, folder_id, file_name, modified_time, status, error_msg, updated_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET
                    status='error', error_msg=excluded.error_msg, updated_at=excluded.updated_at
            """, (file_id, folder_id, file_name, modified_time, 'error', error_msg, now))
        self._cache[file_id] = {
            'file_id': file_id, 'folder_id': folder_id, 'file_name': file_name,
            'modified_time': modified_time, 'status': 'error',
            'error_msg': error_msg, 'updated_at': now,
        }

    def get_all_files(self) -> List[Dict]:
        with self.ds._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM drive_sync_files ORDER BY updated_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> Dict:
        with self.ds._connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COALESCE(SUM(CASE WHEN status='imported' THEN 1 ELSE 0 END), 0) as imported,
                    COALESCE(SUM(CASE WHEN status='error'    THEN 1 ELSE 0 END), 0) as errors,
                    MAX(imported_at) as last_import
                FROM drive_sync_files
            """).fetchone()
        return dict(row) if row else {'total': 0, 'imported': 0, 'errors': 0, 'last_import': None}


# ── Smart Date Extractor ──────────────────────────────────────────────────────

class DateExtractor:
    """Extracts date ranges from filenames or Excel content."""

    MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }

    @classmethod
    def from_filename(cls, filename: str, default_year: int = 2026) -> Tuple[str, str]:
        import re
        fn = filename.lower()
        for abbr, num in cls.MONTH_MAP.items():
            if abbr in fn:
                yr_m = re.search(r'20(\d{2})', fn)
                year = 2000 + int(yr_m.group(1)) if yr_m else default_year
                start = datetime(year, num, 1)
                end = (datetime(year + 1, 1, 1) if num == 12
                       else datetime(year, num + 1, 1)) - timedelta(days=1)
                return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        # Default: folder year + current month
        today = datetime.now()
        start = datetime(default_year, today.month, 1)
        end = (datetime(default_year + 1, 1, 1) if today.month == 12
               else datetime(default_year, today.month + 1, 1)) - timedelta(days=1)
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

    @classmethod
    def from_excel_content(cls, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        if 'Date' in df.columns:
            dates = pd.to_datetime(df['Date'], errors='coerce').dropna()
            if len(dates) > 0:
                return dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d')
        return None, None


# ── Full Generation Pipeline (same as app.py _run_generation) ────────────────

def _run_pipeline(file_buffer: io.BytesIO, filename: str,
                  start_date: str, end_date: str, ds: DataStore) -> Dict:
    """
    Run the full DALA generation pipeline on a file buffer.
    Returns a summary dict with keys: report_id, brands, rows, start_date, end_date.
    Raises on hard failure.
    """
    from .ingestion import load_and_clean, filter_by_date, split_by_brand
    from .kpi import calculate_kpis, calculate_perf_score
    from .alerts import check_and_save_alerts, run_portfolio_alerts

    # 1. Load and filter
    df = load_and_clean(file_buffer)
    df_filtered = filter_by_date(df, start_date, end_date)
    if df_filtered.empty:
        raise ValueError(f"No data in date range {start_date} to {end_date}")

    brand_data = split_by_brand(df_filtered)
    brands = list(brand_data.keys())
    if not brands:
        raise ValueError("No brand data found after split")

    # 2. KPIs + perf scores
    all_kpis = {b: calculate_kpis(brand_data[b]) for b in brands}
    total_rev = sum(k['total_revenue'] for k in all_kpis.values())
    avg_rev   = total_rev / max(len(brands), 1)
    for b in brands:
        all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], avg_rev)

    # 3. Upsert report row
    all_stores: set = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    total_qty = sum(k['total_qty'] for k in all_kpis.values())
    existing  = ds.get_report_by_date_range(start_date, end_date)
    if existing:
        report_id = existing['id']
        ds.clear_report_data(report_id)
        ds.update_report(report_id, xls_filename=filename,
                         total_revenue=total_rev, total_qty=total_qty,
                         total_stores=len(all_stores), brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date, end_date=end_date,
            xls_filename=filename,
            total_revenue=total_rev, total_qty=total_qty,
            total_stores=len(all_stores), brand_count=len(brands),
        )
    ds.sync_catalog_from_history()
    ds.register_catalog_candidates(df_filtered, source_filename=filename, source_report_id=report_id)

    # 4. Save brand KPIs + daily sales + alerts
    for b in brands:
        k = all_kpis[b]
        share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
        ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
        ds.save_brand_detail_json(report_id, b, k)
        if not k['daily_sales'].empty:
            ds.save_daily_sales(report_id, b, k['daily_sales'])
        history = ds.get_brand_history(b, limit=3)
        check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)

    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)

    return {
        'report_id':  report_id,
        'brands':     len(brands),
        'rows':       len(df_filtered),
        'start_date': start_date,
        'end_date':   end_date,
    }


def _run_pipeline_from_df(df: pd.DataFrame, label: str,
                          start_date: str, end_date: str, ds: DataStore) -> Dict:
    """
    Run the full DALA generation pipeline on a pre-combined DataFrame.
    Identical to _run_pipeline() but accepts a DataFrame instead of a file buffer.
    """
    from .ingestion import filter_by_date, split_by_brand
    from .kpi import calculate_kpis, calculate_perf_score
    from .alerts import check_and_save_alerts, run_portfolio_alerts

    df_filtered = filter_by_date(df, start_date, end_date)
    if df_filtered.empty:
        raise ValueError(f"No data in date range {start_date} to {end_date}")

    brand_data = split_by_brand(df_filtered)
    brands     = list(brand_data.keys())
    if not brands:
        raise ValueError("No brand data found after split")

    all_kpis  = {b: calculate_kpis(brand_data[b]) for b in brands}
    total_rev = sum(k['total_revenue'] for k in all_kpis.values())
    avg_rev   = total_rev / max(len(brands), 1)
    for b in brands:
        all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], avg_rev)

    all_stores: set = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    total_qty = sum(k['total_qty'] for k in all_kpis.values())
    existing  = ds.get_report_by_date_range(start_date, end_date)
    if existing:
        report_id = existing['id']
        ds.clear_report_data(report_id)
        ds.update_report(report_id, xls_filename=label,
                         total_revenue=total_rev, total_qty=total_qty,
                         total_stores=len(all_stores), brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date, end_date=end_date,
            xls_filename=label,
            total_revenue=total_rev, total_qty=total_qty,
            total_stores=len(all_stores), brand_count=len(brands),
        )
    ds.sync_catalog_from_history()
    ds.register_catalog_candidates(df_filtered, source_filename=label, source_report_id=report_id)

    for b in brands:
        k = all_kpis[b]
        share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
        ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
        ds.save_brand_detail_json(report_id, b, k)
        if not k['daily_sales'].empty:
            ds.save_daily_sales(report_id, b, k['daily_sales'])
        history = ds.get_brand_history(b, limit=3)
        check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)

    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
    return {
        'report_id':  report_id,
        'brands':     len(brands),
        'rows':       len(df_filtered),
        'start_date': start_date,
        'end_date':   end_date,
    }


# ── Main Sync Orchestrator ────────────────────────────────────────────────────

class DriveSyncOrchestrator:
    """Orchestrates Drive listing, downloading, and pipeline execution."""

    def __init__(self):
        self.ds    = DataStore()
        self.state = SyncState(self.ds)
        self.drive = DriveService()

    # ── Public API ────────────────────────────────────────────────────────────

    def get_month_groups(self) -> List[Dict]:
        """
        Scan root Drive folders and return a list of month groups.
        Each group = one first-level subfolder that contains a month name,
        with all Excel files found recursively inside it.
        Folders without a month name (Audit, Templates, etc.) are skipped.
        """
        groups = []
        for root_folder in DRIVE_FOLDERS:
            year = root_folder['year']
            try:
                subfolders = self.drive.list_subfolders(root_folder['id'])
            except Exception as e:
                print(f"Could not list subfolders in {root_folder['name']}: {e}")
                continue
            for sf in sorted(subfolders, key=lambda x: x['name']):
                month_num = _extract_month_from_folder_name(sf['name'])
                if month_num is None:
                    continue
                files = self.drive.list_excel_recursive(sf['id'])
                if not files:
                    continue
                groups.append({
                    'root_folder_id':    root_folder['id'],
                    'root_folder_name':  root_folder['name'],
                    'year':              year,
                    'month':             month_num,
                    'month_folder_name': sf['name'],
                    'month_folder_id':   sf['id'],
                    'files':             files,
                })
        return groups

    def check_new_files(self) -> List[Dict]:
        """
        Check all month groups for new or changed files and import any that have changed.
        """
        groups  = self.get_month_groups()
        results = []
        for group in groups:
            has_new = any(
                not self.state.is_imported(f['id'], f.get('modifiedTime', ''))
                for f in group['files']
            )
            if has_new:
                results.append(self._import_month_group(group))
            else:
                results.append({'group': group['month_folder_name'], 'status': 'skipped'})
        return results

    def full_historical_sync(self, progress_cb=None, groups=None) -> List[Dict]:
        """
        Import ALL month groups from all folders regardless of sync state.
        groups: optional pre-computed list from get_month_groups() to avoid a second API call.
        progress_cb: optional callable(current, total, label).
        """
        if groups is None:
            groups = self.get_month_groups()
        total   = len(groups)
        results = []
        for i, group in enumerate(groups):
            if progress_cb:
                progress_cb(i, total, group['month_folder_name'])
            results.append(self._import_month_group(group))
        if progress_cb:
            progress_cb(total, total, 'Done')
        return results

    def list_all_files(self) -> List[Dict]:
        """
        List all month groups from Drive with their import status.
        Used by the dashboard — returns one entry per month group (not per file).
        """
        synced    = {r['file_id']: r for r in self.state.get_all_files()}
        all_items = []
        for root_folder in DRIVE_FOLDERS:
            try:
                subfolders = self.drive.list_subfolders(root_folder['id'])
                for sf in sorted(subfolders, key=lambda x: x['name']):
                    month_num = _extract_month_from_folder_name(sf['name'])
                    if month_num is None:
                        continue
                    files = self.drive.list_excel_recursive(sf['id'])
                    if not files:
                        continue
                    statuses = [synced.get(f['id'], {}).get('status', 'pending') for f in files]
                    if all(s == 'imported' for s in statuses):
                        status = 'imported'
                    elif any(s == 'error' for s in statuses):
                        status = 'error'
                    else:
                        status = 'pending'
                    report_id = next(
                        (synced[f['id']].get('report_id') for f in files
                         if f['id'] in synced and synced[f['id']].get('status') == 'imported'),
                        None
                    )
                    all_items.append({
                        'id':           sf['id'],
                        'name':         sf['name'],
                        'folder':       root_folder['name'],
                        'folder_id':    root_folder['id'],
                        'file_count':   len(files),
                        'status':       status,
                        'report_id':    report_id,
                        'list_error':   None,
                    })
            except Exception as e:
                err_msg = str(e)
                print(f"Could not list {root_folder['name']}: {err_msg}")
                all_items.append({
                    'id': None, 'name': None,
                    'folder':    root_folder['name'],
                    'folder_id': root_folder['id'],
                    'list_error': err_msg,
                })
        return sorted([f for f in all_items if f.get('name')], key=lambda x: x['name'])

    def get_sync_summary(self) -> Dict:
        stats = self.state.get_stats()
        return {
            'total_files_tracked': stats['total'],
            'total_imports':       stats['imported'],
            'total_errors':        stats['errors'],
            'last_import':         stats['last_import'],
            'folders':             [{'name': f['name'], 'folder_id': f['id']} for f in DRIVE_FOLDERS],
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _import_month_group(self, group: Dict) -> Dict:
        """
        Download all brand files in a month group, combine into one DataFrame,
        and run the full DALA pipeline for that calendar month.
        """
        from .ingestion import load_brand_file, load_and_clean

        year       = group['year']
        month      = group['month']
        start_date = f"{year}-{month:02d}-01"
        last_day   = _calendar.monthrange(year, month)[1]
        end_date   = f"{year}-{month:02d}-{last_day}"
        label      = group['month_folder_name']

        print(f"  Importing month group: {label} ({start_date} → {end_date})")
        combined_dfs, brand_dfs, file_errors = [], [], []
        for f in group['files']:
            try:
                buf = self.drive.download_file(f['id'])
                # Try standard combined format first (has Brand Partner column)
                try:
                    df = load_and_clean(buf)
                    if not df.empty:
                        combined_dfs.append(df)
                    continue
                except (ValueError, Exception):
                    pass
                # Fall back to per-brand wide format
                buf.seek(0)
                brand_name = _extract_brand_from_filename(f['name']) or f['name']
                df = load_brand_file(buf, brand_name)
                if not df.empty:
                    brand_dfs.append(df)
            except Exception as e:
                file_errors.append(f"{f['name']}: {e}")

        # Prefer combined files; use brand files only if no combined file found
        dfs = combined_dfs if combined_dfs else brand_dfs

        if not dfs:
            err = f"No data loaded from {len(group['files'])} files. Sample errors: {'; '.join(file_errors[:3])}"
            for f in group['files']:
                self.state.mark_error(f['id'], group['root_folder_id'], f['name'],
                                      f.get('modifiedTime', ''), err)
            return {'group': label, 'status': 'error', 'error': err}

        combined = pd.concat(dfs, ignore_index=True)
        try:
            summary   = _run_pipeline_from_df(combined, label, start_date, end_date, self.ds)
            report_id = summary['report_id']
            for f in group['files']:
                self.state.mark_imported(f['id'], group['root_folder_id'], f['name'],
                                         f.get('modifiedTime', ''), report_id)
            return {
                'group':      label,
                'status':     'success',
                'date_range': f"{start_date} to {end_date}",
                'brands':     summary['brands'],
                'rows':       summary['rows'],
                'report_id':  report_id,
            }
        except Exception as e:
            error_msg = str(e)
            for f in group['files']:
                self.state.mark_error(f['id'], group['root_folder_id'], f['name'],
                                      f.get('modifiedTime', ''), error_msg)
            return {'group': label, 'status': 'error', 'error': error_msg}

    def _import_file(self, file: Dict, folder_id: str, default_year: int) -> Dict:
        """Download and run the full generation pipeline for one Drive file."""
        file_id   = file['id']
        file_name = file['name']
        print(f"  Importing: {file_name}")

        try:
            # Download
            buf = self.drive.download_file(file_id)

            # Detect date range from content first, then filename
            df_preview = load_and_clean(io.BytesIO(buf.read()))
            buf.seek(0)
            start_date, end_date = DateExtractor.from_excel_content(df_preview)
            if not start_date:
                start_date, end_date = DateExtractor.from_filename(file_name, default_year)

            # Run full pipeline
            summary = _run_pipeline(buf, file_name, start_date, end_date, self.ds)

            self.state.mark_imported(
                file_id, folder_id, file_name,
                file.get('modifiedTime', ''), summary['report_id']
            )
            return {
                'file':       file_name,
                'status':     'success',
                'date_range': f"{start_date} to {end_date}",
                'brands':     summary['brands'],
                'rows':       summary['rows'],
                'report_id':  summary['report_id'],
            }

        except Exception as e:
            error_msg = str(e)
            print(f"  Error importing {file_name}: {error_msg}")
            self.state.mark_error(
                file_id, folder_id, file_name,
                file.get('modifiedTime', ''), error_msg
            )
            return {'file': file_name, 'status': 'error', 'error': error_msg}


if __name__ == '__main__':
    orch = DriveSyncOrchestrator()
    print("Starting full historical sync...")
    results = orch.full_historical_sync()
    ok  = sum(1 for r in results if r.get('status') == 'success')
    err = sum(1 for r in results if r.get('status') == 'error')
    print(f"Done. Imported: {ok}  Errors: {err}")
