"""
ingestion.py — Data loading, cleaning, and brand splitting.

Handles Tally .xls exports (xlsx internally), plain .xlsx, and large .csv files.
For CSV files with millions of rows, chunked reading keeps memory usage flat —
each chunk is cleaned and appended so even files > 1 GB are processed without
loading the whole dataset into RAM at once.

Applies the standard column rename schema and filters to the selected date range.
Designed so the file-upload source can later be swapped for a Tally API call
without touching any downstream module.
"""

import os
import io
from functools import lru_cache
import pandas as pd

from .brand_names import canonicalize_brand_name

_CATALOG_STORE = None

# ── CSV chunk size (rows per chunk for large file streaming) ─────────────────
_CSV_CHUNKSIZE = 50_000

# ── Column rename map: Raw Tally → Standard schema ──────────────────────────
COLUMN_RENAME_MAP = {
    'Brand Partners': 'Brand Partner',
    'Value':          'Sales_Value',
}

# ── Vch Type constants ───────────────────────────────────────────────────────
VCH_SALES               = 'Sales'
VCH_AVAILABLE_INVENTORY = 'Available Inventory'
VCH_INVENTORY_PICKUP    = 'Inventory Pickup by Dala'
VCH_INVENTORY_SUPPLIED  = 'Inventory Supplied by Brands'
VCH_JOURNAL             = 'Journal'

_STORE_HINTS = (
    'supermarket', 'hypermart', 'hypermarket', 'market', 'mart', 'mall',
    'shop', 'stores', 'store', 'plaza', 'freedom way', 'road', 'rd',
    'ikeja', 'lekki', 'gbagada', 'magodo', 'maryland', 'orchid', 'ogombo',
    'chisco', 'eleganza', 'victory park', 'ajah', 'ikota', 'sangotedo',
)
_SKU_HINTS = (
    '(12x)', '(24x)', '(6x)', '500ml', '330ml', '1ltr', '1.5ltr', '2ltr',
    'kg', 'gram', 'g ', 'yoghurt', 'yogurt', 'oil', 'soap', 'tea', 'juice',
    'drink', 'water', 'chips', 'crisps', 'carrot', 'coconut', 'vanilla',
    'strawberry', 'greek', 'protein', 'unsweet', 'sweetend', 'sweetened',
)


def looks_like_store_label(value) -> bool:
    text = str(value or '').strip().lower()
    if not text:
        return False
    score = 0
    if ',' in text:
        score += 1
    if any(hint in text for hint in _STORE_HINTS):
        score += 2
    if any(hint in text for hint in _SKU_HINTS):
        score -= 2
    if any(ch.isdigit() for ch in text):
        score -= 0.5
    return score >= 2


def looks_like_sku_label(value) -> bool:
    text = str(value or '').strip().lower()
    if not text:
        return False
    score = 0
    if any(hint in text for hint in _SKU_HINTS):
        score += 2
    if any(ch.isdigit() for ch in text):
        score += 1
    if 'x)' in text or 'ml' in text or 'ltr' in text:
        score += 1
    if ',' in text:
        score -= 1
    if any(hint in text for hint in _STORE_HINTS):
        score -= 2
    return score >= 2


def _sample_texts(series, limit=40):
    if series is None:
        return []
    values = []
    for value in series.dropna().astype(str):
        text = value.strip()
        if not text:
            continue
        values.append(text)
        if len(values) >= limit:
            break
    return values


def _columns_look_swapped(df) -> bool:
    if 'Particulars' not in df.columns or 'SKUs' not in df.columns or df.empty:
        return False
    particulars = _sample_texts(df['Particulars'])
    skus = _sample_texts(df['SKUs'])
    if not particulars or not skus:
        return False
    particulars_store_ratio = sum(1 for value in particulars if looks_like_store_label(value)) / len(particulars)
    particulars_sku_ratio = sum(1 for value in particulars if looks_like_sku_label(value)) / len(particulars)
    skus_store_ratio = sum(1 for value in skus if looks_like_store_label(value)) / len(skus)
    skus_sku_ratio = sum(1 for value in skus if looks_like_sku_label(value)) / len(skus)
    return (
        skus_store_ratio >= 0.45 and
        particulars_sku_ratio >= 0.45 and
        skus_store_ratio > particulars_store_ratio and
        particulars_sku_ratio > skus_sku_ratio
    )


def _swap_dimension_columns(df):
    swapped = df.copy()
    if 'Particulars' not in swapped.columns or 'SKUs' not in swapped.columns:
        return swapped
    swapped = swapped.rename(columns={'Particulars': '__tmp_particulars__', 'SKUs': 'Particulars'})
    swapped = swapped.rename(columns={'__tmp_particulars__': 'SKUs'})
    return swapped


def normalize_dimension_columns(df):
    normalized = df.rename(columns=COLUMN_RENAME_MAP).copy()

    if 'Retailers' in normalized.columns and 'SKUs' not in normalized.columns:
        if 'Particulars' in normalized.columns:
            normalized = normalized.rename(columns={'Particulars': '__tmp_sku__', 'Retailers': 'Particulars'})
            normalized = normalized.rename(columns={'__tmp_sku__': 'SKUs'})
        else:
            normalized = normalized.rename(columns={'Retailers': 'Particulars'})

    if _columns_look_swapped(normalized):
        normalized = _swap_dimension_columns(normalized)

    return normalized


def _is_csv(file_source):
    """Return True if file_source is a CSV (by name or sniffing first bytes)."""
    if isinstance(file_source, str):
        return file_source.lower().endswith('.csv')
    if hasattr(file_source, 'name'):
        return getattr(file_source, 'name', '').lower().endswith('.csv')
    # Sniff: CSV files never start with PK (xlsx zip magic) or \xD0\xCF (xls OLE2)
    if hasattr(file_source, 'read'):
        header = file_source.read(4)
        file_source.seek(0)
        return header[:2] not in (b'PK', b'\xD0\xCF')
    return False


@lru_cache(maxsize=None)
def _resolve_known_brand_name(raw_name: str) -> str:
    """
    Map approved aliases to their canonical brand names without blocking unknown imports.
    Unknown names still fall back to lightweight normalization.
    """
    global _CATALOG_STORE

    canonical = canonicalize_brand_name(raw_name)
    if not canonical:
        return ''

    if _CATALOG_STORE is None:
        try:
            from .data_store import DataStore
            _CATALOG_STORE = DataStore()
        except Exception:
            _CATALOG_STORE = False

    if _CATALOG_STORE:
        try:
            brand = _CATALOG_STORE.resolve_brand_master(raw_name)
            if brand:
                return brand['canonical_name']
        except Exception:
            pass

    return canonical


def load_and_clean(file_source):
    """
    Load a Tally data file and apply standard column renaming.

    Supported formats:
      - .xlsx / .xls  (Tally Excel export — the default)
      - .csv          (large historical datasets; read in 50 000-row chunks)

    The file may carry a .xls extension but be xlsx internally — we try
    openpyxl first and fall back to xlrd for genuine binary .xls files.

    Args:
        file_source: file path (str), file-like object (BytesIO), or
                     file-like object from Flask's request.files.

    Returns:
        pd.DataFrame with standardised column names and correct dtypes.

    Raises:
        ValueError: if expected columns are absent after cleaning.
    """
    is_csv = _is_csv(file_source)

    if is_csv:
        # ── Chunked CSV read ─────────────────────────────────────────────────
        chunks = []
        for chunk in pd.read_csv(file_source, chunksize=_CSV_CHUNKSIZE,
                                  low_memory=False, encoding='utf-8-sig'):
            chunk = normalize_dimension_columns(chunk)
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    else:
        # ── Excel read ───────────────────────────────────────────────────────
        try:
            df = pd.read_excel(file_source, engine='openpyxl')
        except Exception:
            if hasattr(file_source, 'seek'):
                file_source.seek(0)
            df = pd.read_excel(file_source, engine='xlrd')
        df = normalize_dimension_columns(df)

    required_cols = [
        'Brand Partner', 'SKUs', 'Date', 'Particulars',
        'Vch Type', 'Vch No.', 'Quantity', 'Sales_Value',
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Upload rejected — missing columns after cleaning: {missing}. "
            f"Columns found: {df.columns.tolist()}"
        )

    # ── Type casting ─────────────────────────────────────────────────────────
    df['Date']         = pd.to_datetime(df['Date'], errors='coerce')
    df['Quantity']     = pd.to_numeric(df['Quantity'],    errors='coerce').fillna(0)
    df['Sales_Value']  = pd.to_numeric(df['Sales_Value'], errors='coerce').fillna(0)
    
    # Normalize brand names to prevent duplicates (Wilson vs Wilson's, etc.)
    df['Brand Partner'] = df['Brand Partner'].astype(str).str.strip().apply(_resolve_known_brand_name)
    df['SKUs']          = df['SKUs'].astype(str).str.strip()
    df['Particulars']   = df['Particulars'].astype(str).str.strip()
    df['Vch Type']      = df['Vch Type'].astype(str).str.strip()
    df['Vch No.']       = df['Vch No.'].astype(str).str.strip()

    # Drop rows where date could not be parsed
    df = df.dropna(subset=['Date']).reset_index(drop=True)

    return df


def filter_by_date(df, start_date, end_date):
    """
    Filter the dataset to the operator-selected date range (inclusive on both ends).

    Args:
        df: cleaned DataFrame from load_and_clean().
        start_date: str 'YYYY-MM-DD' or date-like.
        end_date:   str 'YYYY-MM-DD' or date-like.

    Returns:
        Filtered DataFrame.
    """
    start = pd.Timestamp(start_date)
    end   = pd.Timestamp(end_date)
    mask  = (df['Date'] >= start) & (df['Date'] <= end)
    return df[mask].copy().reset_index(drop=True)


def split_by_brand(df):
    """
    Split the dataset into per-brand DataFrames.

    Only brands with at least one Sales row are included.
    Zero-sales brands are silently skipped (per project spec).

    Args:
        df: date-filtered DataFrame.

    Returns:
        dict {brand_name (str): brand_df (pd.DataFrame)}, sorted alphabetically.
    """
    active_brands = sorted(
        df[df['Vch Type'] == VCH_SALES]['Brand Partner'].unique()
    )

    return {
        brand: df[df['Brand Partner'] == brand].copy().reset_index(drop=True)
        for brand in active_brands
    }


def get_all_brands(df):
    """
    Get all unique brand names in the dataset (regardless of Vch Type).
    Used to identify zero-sales brands for the summary notice.
    
    Args:
        df: date-filtered DataFrame.
    
    Returns:
        set of all brand names present in the data.
    """
    return set(df['Brand Partner'].unique())


def load_brand_file(file_source, brand_name: str) -> pd.DataFrame:
    """
    Load an individual per-brand Tally-export file (wide multi-section format).

    Excel layout:
      Row 1 (Excel):  Section headers — "Exported from Tally", "Dala Sales & Deliveries", etc.
      Row 2 (Excel):  Column labels  — "item name", "Date", "Particulars", "Vch Type", "Vch No.", "Quantity", "Value"
      Row 3+ (Excel): Data rows

    Only the first 7 columns ("Exported from Tally" section) are extracted.
    The brand name is injected as the "Brand Partner" column.
    """
    df_raw = pd.read_excel(file_source, engine='openpyxl')
    if df_raw.empty:
        raise ValueError(f"Empty file for brand: {brand_name}")

    # Row 0 of DataFrame = sub-header row ("item name", "Date", "Particulars", ...)
    col_labels = df_raw.iloc[0, :7].tolist()

    # Data starts at DataFrame row 1
    data = df_raw.iloc[1:, :7].copy()
    data.columns = col_labels

    data = data.rename(columns={'item name': 'SKUs', 'Value': 'Sales_Value'})
    data = normalize_dimension_columns(data)
    data['Brand Partner'] = brand_name

    data = data.dropna(subset=['SKUs', 'Date', 'Sales_Value'], how='all')
    data['Date']         = pd.to_datetime(data['Date'], errors='coerce')
    data['Quantity']     = pd.to_numeric(data['Quantity'],    errors='coerce').fillna(0)
    data['Sales_Value']  = pd.to_numeric(data['Sales_Value'], errors='coerce').fillna(0)
    data = data.dropna(subset=['Date']).reset_index(drop=True)

    for col in ['Particulars', 'Vch Type', 'Vch No.']:
        if col not in data.columns:
            data[col] = ''

    data['SKUs']         = data['SKUs'].astype(str).str.strip()
    data['Particulars']  = data['Particulars'].astype(str).str.strip()
    data['Vch Type']     = data['Vch Type'].astype(str).str.strip()
    data['Vch No.']      = data['Vch No.'].astype(str).str.strip()
    data['Brand Partner'] = data['Brand Partner'].astype(str).str.strip().apply(_resolve_known_brand_name)

    return data[['Brand Partner', 'SKUs', 'Date', 'Particulars', 'Vch Type', 'Vch No.', 'Quantity', 'Sales_Value']]

