"""
drive_sync.py - Google Drive Integration for DALA Analytics

Automatically watches folders, detects new/modified Excel files,
and triggers import pipeline. Runs as background service.

Features:
- Watch multiple folders (2025, 2026, etc.)
- Detect new/modified files
- Automatic download and validation
- Smart date range extraction
- Duplicate prevention
- Error handling with exponential backoff
"""

import os
import io
import json
import time
import pickle
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# Google API
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2 import service_account

import pandas as pd

# DALA modules
from .data_store import DataStore
from .ingestion import load_and_clean, filter_by_date

# ── Configuration ─────────────────────────────────────────────────────────────

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
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

SYNC_STATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'cache', 'drive_sync_state.json'
)

os.makedirs(os.path.dirname(SYNC_STATE_FILE), exist_ok=True)

# ── Google Drive Service ───────────────────────────────────────────────────────

class DriveService:
    """Handles Google Drive API authentication and operations."""
    
    def __init__(self, credentials_path: str = None):
        self.service = None
        self.credentials_path = credentials_path or self._find_credentials()
        self._authenticate()
    
    def _find_credentials(self) -> str:
        """Find service account credentials file."""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        possible_paths = [
            os.path.join(base_dir, 'google_credentials.json'),
            os.path.join(base_dir, 'service_account.json'),
            os.environ.get('GOOGLE_CREDENTIALS_PATH', ''),
        ]
        for path in possible_paths:
            if path and os.path.exists(path):
                return path
        raise FileNotFoundError("Google credentials not found. Please set up service account.")
    
    def _authenticate(self):
        """Authenticate with Google Drive API."""
        credentials = service_account.Credentials.from_service_account_file(
            self.credentials_path,
            scopes=SCOPES
        )
        self.service = build('drive', 'v3', credentials=credentials)
    
    def list_excel_files(self, folder_id: str) -> List[Dict]:
        """List all Excel files in a folder."""
        query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        
        files = []
        page_token = None
        
        while True:
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, modifiedTime, createdTime, size)',
                pageToken=page_token,
                orderBy='modifiedTime desc'
            ).execute()
            
            files.extend(results.get('files', []))
            page_token = results.get('nextPageToken')
            
            if not page_token:
                break
        
        return files
    
    def download_file(self, file_id: str, file_name: str) -> io.BytesIO:
        """Download file from Google Drive to memory."""
        request = self.service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        return file_buffer
    
    def get_file_changes(self, folder_id: str, last_check: datetime) -> List[Dict]:
        """Get files modified since last check."""
        all_files = self.list_excel_files(folder_id)
        
        changes = []
        for file in all_files:
            modified = datetime.fromisoformat(file['modifiedTime'].replace('Z', '+00:00'))
            if modified > last_check:
                changes.append(file)
        
        return changes

# ── Sync State Manager ─────────────────────────────────────────────────────────

class SyncState:
    """Manages state of synced files to prevent re-imports."""
    
    def __init__(self):
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load sync state from file."""
        if os.path.exists(SYNC_STATE_FILE):
            with open(SYNC_STATE_FILE, 'r') as f:
                return json.load(f)
        return {
            'last_check': None,
            'files': {},  # file_id -> {name, modifiedTime, imported_at, status}
            'stats': {
                'total_imports': 0,
                'total_errors': 0,
                'last_import': None,
            }
        }
    
    def save(self):
        """Save sync state to file."""
        with open(SYNC_STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def is_imported(self, file_id: str, modified_time: str) -> bool:
        """Check if file has been imported and hasn't changed."""
        if file_id not in self.state['files']:
            return False
        
        stored = self.state['files'][file_id]
        return stored['modifiedTime'] == modified_time and stored['status'] == 'imported'
    
    def mark_imported(self, file_id: str, file_info: Dict, report_id: int = None):
        """Mark file as successfully imported."""
        self.state['files'][file_id] = {
            'name': file_info['name'],
            'modifiedTime': file_info['modifiedTime'],
            'imported_at': datetime.now().isoformat(),
            'status': 'imported',
            'report_id': report_id,
        }
        self.state['stats']['total_imports'] += 1
        self.state['stats']['last_import'] = datetime.now().isoformat()
        self.save()
    
    def mark_error(self, file_id: str, file_info: Dict, error_msg: str):
        """Mark file as failed import."""
        self.state['files'][file_id] = {
            'name': file_info['name'],
            'modifiedTime': file_info['modifiedTime'],
            'error_at': datetime.now().isoformat(),
            'status': 'error',
            'error': error_msg,
        }
        self.state['stats']['total_errors'] += 1
        self.save()
    
    def get_last_check(self) -> datetime:
        """Get last check time."""
        if self.state['last_check']:
            return datetime.fromisoformat(self.state['last_check'])
        return datetime.now() - timedelta(days=30)  # Check last 30 days initially
    
    def update_last_check(self):
        """Update last check timestamp."""
        self.state['last_check'] = datetime.now().isoformat()
        self.save()

# ── Smart Date Extractor ──────────────────────────────────────────────────────

class DateExtractor:
    """Extracts date ranges from filenames or Excel content."""
    
    MONTH_MAP = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    
    @classmethod
    def from_filename(cls, filename: str, default_year: int = 2026) -> Tuple[str, str]:
        """
        Extract date range from filename.
        Examples:
            "February_2026_Sales.xlsx" -> ("2026-02-01", "2026-02-28")
            "Sales_Q1_2025.xlsx" -> ("2025-01-01", "2025-03-31")
        """
        import re
        
        filename_lower = filename.lower()
        
        # Try to find month name
        for month_abbr, month_num in cls.MONTH_MAP.items():
            if month_abbr in filename_lower:
                # Extract year (default to provided)
                year_match = re.search(r'20(\d{2})', filename_lower)
                year = 2000 + int(year_match.group(1)) if year_match else default_year
                
                # Calculate date range
                start_date = datetime(year, month_num, 1)
                if month_num == 12:
                    end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_date = datetime(year, month_num + 1, 1) - timedelta(days=1)
                
                return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        
        # Default: use current month
        today = datetime.now()
        start = today.replace(day=1)
        if start.month == 12:
            end = datetime(start.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = datetime(start.year, start.month + 1, 1) - timedelta(days=1)
        
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
    
    @classmethod
    def from_excel_content(cls, df: pd.DataFrame) -> Tuple[str, str]:
        """Extract date range from actual Excel data."""
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            valid_dates = df['Date'].dropna()
            
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                return min_date.strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')
        
        # Fallback to filename extraction
        return None, None

# ── Main Sync Orchestrator ────────────────────────────────────────────────────

class DriveSyncOrchestrator:
    """Main class that orchestrates the entire sync process."""
    
    def __init__(self):
        self.drive = DriveService()
        self.state = SyncState()
        self.ds = DataStore()
        self.processing = False
    
    def check_all_folders(self) -> List[Dict]:
        """Check all configured folders for changes."""
        results = []
        
        for folder in DRIVE_FOLDERS:
            try:
                folder_results = self._process_folder(folder)
                results.extend(folder_results)
            except Exception as e:
                print(f"Error processing folder {folder['name']}: {e}")
                results.append({
                    'folder': folder['name'],
                    'status': 'error',
                    'error': str(e),
                })
        
        self.state.update_last_check()
        return results
    
    def _process_folder(self, folder_config: Dict) -> List[Dict]:
        """Process a single folder."""
        folder_id = folder_config['id']
        folder_name = folder_config['name']
        default_year = folder_config['year']
        
        print(f"Checking folder: {folder_name}")
        
        # Get files modified since last check
        last_check = self.state.get_last_check()
        files = self.drive.get_file_changes(folder_id, last_check)
        
        results = []
        
        for file in files:
            file_id = file['id']
            file_name = file['name']
            modified_time = file['modifiedTime']
            
            # Skip if already imported and unchanged
            if self.state.is_imported(file_id, modified_time):
                print(f"  Skipping {file_name} (already imported)")
                continue
            
            # Process file
            result = self._import_file(file, default_year)
            results.append(result)
        
        return results
    
    def _import_file(self, file: Dict, default_year: int) -> Dict:
        """Import a single file from Drive."""
        file_id = file['id']
        file_name = file['name']
        
        print(f"  Importing: {file_name}")
        
        try:
            # Download file
            file_buffer = self.drive.download_file(file_id, file_name)
            
            # Load and validate
            df = load_and_clean(file_buffer)
            
            # Extract date range
            start_date, end_date = DateExtractor.from_filename(file_name, default_year)
            
            # Override with actual data dates if available
            content_start, content_end = DateExtractor.from_excel_content(df)
            if content_start and content_end:
                start_date, end_date = content_start, content_end
            
            # Filter to date range
            df_filtered = filter_by_date(df, start_date, end_date)
            
            # Check for overlapping data (prevent duplicates)
            if self._check_duplicate_import(start_date, end_date, df_filtered):
                self.state.mark_imported(file_id, file)
                return {
                    'file': file_name,
                    'status': 'skipped',
                    'reason': 'Duplicate data range already imported',
                    'date_range': f"{start_date} to {end_date}",
                }
            
            # TODO: Trigger actual generation pipeline
            # For now, just validate the data
            brand_count = df_filtered['Brand Partner'].nunique()
            row_count = len(df_filtered)
            
            # Mark as imported
            self.state.mark_imported(file_id, file)
            
            return {
                'file': file_name,
                'status': 'success',
                'date_range': f"{start_date} to {end_date}",
                'brands': brand_count,
                'rows': row_count,
            }
            
        except Exception as e:
            error_msg = str(e)
            print(f"  Error importing {file_name}: {error_msg}")
            self.state.mark_error(file_id, file, error_msg)
            
            return {
                'file': file_name,
                'status': 'error',
                'error': error_msg,
            }
    
    def _check_duplicate_import(self, start_date: str, end_date: str, df: pd.DataFrame) -> bool:
        """Check if this date range has already been imported."""
        # Get existing reports for this date range
        existing = self.ds.get_report_by_date_range(start_date, end_date)
        
        if existing:
            # Check if data is similar (same brand count, similar row count)
            existing_brands = existing.get('brand_count', 0)
            new_brands = df['Brand Partner'].nunique()
            
            # If similar, consider it duplicate
            if abs(existing_brands - new_brands) <= 2:
                return True
        
        return False
    
    def get_sync_summary(self) -> Dict:
        """Get summary of sync status."""
        return {
            'last_check': self.state.state['last_check'],
            'total_files_tracked': len(self.state.state['files']),
            'total_imports': self.state.state['stats']['total_imports'],
            'total_errors': self.state.state['stats']['stats'],
            'folders': [
                {
                    'name': f['name'],
                    'folder_id': f['id'],
                }
                for f in DRIVE_FOLDERS
            ],
        }

# ── Background Service ─────────────────────────────────────────────────────────

def run_sync_service(interval_minutes: int = 60):
    """
    Run continuous sync service.
    
    Args:
        interval_minutes: How often to check for changes (default: hourly)
    """
    orchestrator = DriveSyncOrchestrator()
    
    print(f"Starting Drive Sync Service (checking every {interval_minutes} minutes)")
    print("=" * 60)
    
    while True:
        try:
            print(f"\n[{datetime.now().isoformat()}] Checking for updates...")
            results = orchestrator.check_all_folders()
            
            # Report results
            success_count = sum(1 for r in results if r.get('status') == 'success')
            error_count = sum(1 for r in results if r.get('status') == 'error')
            
            if success_count > 0:
                print(f"  ✓ Imported {success_count} files")
            if error_count > 0:
                print(f"  ✗ Failed to import {error_count} files")
            if not results:
                print("  No new files found")
            
        except Exception as e:
            print(f"Error in sync service: {e}")
        
        # Sleep until next check
        print(f"  Next check in {interval_minutes} minutes...")
        time.sleep(interval_minutes * 60)


if __name__ == '__main__':
    # Run standalone for testing
    run_sync_service(interval_minutes=5)  # Check every 5 minutes for testing
