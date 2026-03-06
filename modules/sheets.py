"""
sheets.py — Google Sheets integration for DALA brand partner reports.

Auth hierarchy (first found wins):
  1. google_token.json   — OAuth2 user token (preferred — files owned by YOUR
                           Google account, no storage quota issues).
                           Run `python setup_oauth.py` once to create this.
  2. google_credentials.json — Service account (auth works but can't create
                           new files due to zero Drive quota on service accounts).

For each brand partner, this module:
  1. Authenticates with Google
  2. Creates (or overwrites) a Google Sheet named after the brand + date range
  3. Pushes the brand's full filtered data slice as a clean flat table
  4. Applies DALA-branded formatting (navy header, frozen row, alternating rows)
  5. Sets the sheet to view-only for anyone with the link
  6. Returns the shareable URL for embedding in the PDF
"""

import os
import json
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDS_PATH      = os.path.join(BASE_DIR, 'google_credentials.json')     # service account
TOKEN_PATH      = os.path.join(BASE_DIR, 'google_token.json')           # OAuth2 token
OAUTH_CREDS_PATH = os.path.join(BASE_DIR, 'google_oauth_credentials.json')  # OAuth2 client
FOLDER_ID_PATH  = os.path.join(BASE_DIR, 'google_folder_id.txt')

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# Column order pushed to Sheets (all Vch Types included so partner sees full picture)
SHEET_COLUMNS = [
    'Date', 'Brand Partner', 'SKUs', 'Particulars',
    'Vch Type', 'Vch No.', 'Quantity', 'Sales_Value',
]

# Google Sheets API colour objects (DALA navy palette)
_NAVY_BG  = {'red': 0.106, 'green': 0.169, 'blue': 0.369}
_WHITE_FG = {'red': 1.0,   'green': 1.0,   'blue': 1.0}
_LIGHT_BG = {'red': 0.957, 'green': 0.961, 'blue': 0.980}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_folder_id():
    """Read the Drive folder ID from google_folder_id.txt, if present."""
    if os.path.isfile(FOLDER_ID_PATH):
        fid = open(FOLDER_ID_PATH, encoding='utf-8').read().strip()
        return fid if fid and fid != 'PASTE_YOUR_FOLDER_ID_HERE' else None
    return None


# ── Authentication ─────────────────────────────────────────────────────────────

def _get_client(creds_path=None):
    """
    Authenticate with Google. Auth hierarchy:
      1. OAuth2 user token (google_token.json) — preferred, uses real user's Drive.
      2. Service account (google_credentials.json) — fallback (auth works but
         cannot create new Drive files due to zero service-account storage quota).

    Returns an authorised gspread client.
    """
    from google.auth.transport.requests import Request

    # ── 1. OAuth2 user token ─────────────────────────────────────────────────
    if os.path.isfile(TOKEN_PATH):
        from google.oauth2.credentials import Credentials as OAuthCreds

        with open(TOKEN_PATH, encoding='utf-8') as f:
            token_data = json.load(f)

        creds = OAuthCreds.from_authorized_user_info(token_data, SCOPES)

        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Persist refreshed token
                with open(TOKEN_PATH, 'w', encoding='utf-8') as f:
                    f.write(creds.to_json())
            else:
                raise RuntimeError(
                    "OAuth2 token is invalid and cannot be refreshed.\n"
                    "Delete google_token.json and run `python setup_oauth.py` again."
                )

        return gspread.authorize(creds)

    # ── 2. Service account fallback ──────────────────────────────────────────
    path = creds_path or CREDS_PATH
    if not os.path.isfile(path):
        raise FileNotFoundError(
            "No Google credentials found.\n"
            f"  Option A: Run `python setup_oauth.py` to set up OAuth2 (recommended).\n"
            f"  Option B: Place service account JSON at: {path}"
        )

    creds  = Credentials.from_service_account_file(path, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


# ── Sheet name builder ─────────────────────────────────────────────────────────

def _sheet_title(brand_name, start_date, end_date):
    """
    Build a safe Google Sheet title.
    e.g. 'EtiFarms_Feb02_Feb28_2026'
    """
    start_fmt = pd.Timestamp(start_date).strftime('%b%d')
    end_fmt   = pd.Timestamp(end_date).strftime('%b%d_%Y')
    safe      = (brand_name
                 .replace(' ', '')
                 .replace("'", '')
                 .replace('.', '')
                 .replace('/', '-'))
    return f"{safe}_{start_fmt}_{end_fmt}"


# ── Data preparation ───────────────────────────────────────────────────────────

def _prepare_dataframe(brand_df):
    """
    Clean and format the brand DataFrame for Sheets output.
    - Date → 'DD/MM/YYYY' string (Sheets-friendly)
    - Numeric columns rounded to 2dp
    - NaN → empty string
    - Column order enforced
    """
    df = brand_df.copy()

    cols = [c for c in SHEET_COLUMNS if c in df.columns]
    df   = df[cols]

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%d/%m/%Y')

    for col in ['Quantity', 'Sales_Value']:
        if col in df.columns:
            df[col] = df[col].round(2)

    df = df.fillna('')
    return df


# ── Formatting ─────────────────────────────────────────────────────────────────

def _format_sheet(worksheet, n_cols, n_rows):
    """
    Apply DALA-branded formatting:
    - Row 1: navy background, white bold text, frozen
    - Alternate data rows: light blue tint
    - Auto-resize all columns
    """
    requests = [
        # Navy header
        {
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 0, 'endRowIndex': 1,
                    'startColumnIndex': 0, 'endColumnIndex': n_cols,
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': _NAVY_BG,
                        'textFormat': {
                            'foregroundColor': _WHITE_FG,
                            'bold': True,
                            'fontSize': 10,
                        },
                        'horizontalAlignment': 'CENTER',
                        'verticalAlignment': 'MIDDLE',
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,'
                          'horizontalAlignment,verticalAlignment)',
            }
        },
        # Freeze header row
        {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': worksheet.id,
                    'gridProperties': {'frozenRowCount': 1},
                },
                'fields': 'gridProperties.frozenRowCount',
            }
        },
        # Auto-resize columns
        {
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': worksheet.id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': n_cols,
                }
            }
        },
    ]

    # Alternate row shading
    if n_rows > 1:
        requests.append({
            'addBanding': {
                'bandedRange': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,
                        'endRowIndex': n_rows + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': n_cols,
                    },
                    'rowProperties': {
                        'headerColor': _NAVY_BG,
                        'firstBandColor':  {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                        'secondBandColor': _LIGHT_BG,
                    }
                }
            }
        })

    if requests:
        worksheet.spreadsheet.batch_update({'requests': requests})


# ── Public API ─────────────────────────────────────────────────────────────────

def push_brand_to_sheets(brand_name, brand_df, start_date, end_date,
                          creds_path=None):
    """
    Push one brand partner's data slice to Google Sheets.

    Args:
        brand_name: display name of the brand partner.
        brand_df:   full brand DataFrame (all Vch Types).
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'
        creds_path: optional override for service account JSON path.

    Returns:
        str: shareable Google Sheets URL.
    """
    client    = _get_client(creds_path)
    title     = _sheet_title(brand_name, start_date, end_date)
    df        = _prepare_dataframe(brand_df)
    headers   = df.columns.tolist()
    rows      = df.values.tolist()
    all_vals  = [headers] + rows
    folder_id = _get_folder_id()

    # ── Create or overwrite spreadsheet ───────────────────────────────────────
    try:
        spreadsheet = client.open(title)
        worksheet   = spreadsheet.sheet1
        worksheet.clear()
        time.sleep(0.5)
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(title, folder_id=folder_id) if folder_id \
                      else client.create(title)
        worksheet   = spreadsheet.sheet1
        time.sleep(0.5)

    # ── Push data ──────────────────────────────────────────────────────────────
    worksheet.update(all_vals, value_input_option='USER_ENTERED')

    # ── Format ────────────────────────────────────────────────────────────────
    try:
        _format_sheet(worksheet, n_cols=len(headers), n_rows=len(rows))
    except Exception:
        pass  # Formatting failure must never block the report

    # ── Share as anyone-with-link viewer ──────────────────────────────────────
    try:
        spreadsheet.share(
            email_address=None,
            perm_type='anyone',
            role='reader',
            notify=False,
        )
    except Exception:
        pass  # Share failure must never block the report

    return spreadsheet.url


def sheets_available(creds_path=None):
    """
    Returns True if Google Sheets is configured and ready to use.
    Checks for OAuth2 token first, then service account credentials.
    """
    return os.path.isfile(TOKEN_PATH) or \
           os.path.isfile(creds_path or CREDS_PATH)


def sheets_auth_method():
    """
    Returns 'oauth2', 'service_account', or None.
    Used by the UI to show which auth method is active.
    """
    if os.path.isfile(TOKEN_PATH):
        return 'oauth2'
    if os.path.isfile(CREDS_PATH):
        return 'service_account'
    return None
