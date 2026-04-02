"""
activity_intelligence.py — tolerant activity report importer and summariser.

Supports:
  - .xlsx / true .xls
  - CSV / TXT / TSV
  - tab-delimited text files saved with .xls extension

Builds a normalized payload for DataStore.save_activity_import().
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from datetime import datetime
from typing import Iterable

import pandas as pd

from .brand_names import canonicalize_brand_name


EXPECTED_COLUMNS = {
    'activity_date',
    'salesman_name',
    'salesman_code',
    'salesman_designation',
    'reporting_person_name',
    'survey_code',
    'survey_name',
    'survey_start_date',
    'survey_end_date',
    'retailer_code',
    'retailer_name',
    'retailer_type',
    'retailer_state',
    'retailer_district',
    'retailer_city',
    'question',
    'answer_type',
    'label',
    'answer',
}


COLUMN_ALIASES = {
    'activity date': 'activity_date',
    'salesman name': 'salesman_name',
    'salesman code': 'salesman_code',
    'salesman designation': 'salesman_designation',
    'reporting person name': 'reporting_person_name',
    'survey code': 'survey_code',
    'survey name': 'survey_name',
    'survey start date': 'survey_start_date',
    'survey end date': 'survey_end_date',
    'retailer code': 'retailer_code',
    'retailer name': 'retailer_name',
    'retailer type': 'retailer_type',
    'retailer state': 'retailer_state',
    'retailer district': 'retailer_district',
    'retailer city': 'retailer_city',
    'question': 'question',
    'answer type': 'answer_type',
    'label': 'label',
    'answer': 'answer',
}


ISSUE_KEYWORDS = (
    ('out of stock', 'out_of_stock', 'high'),
    ('oos', 'out_of_stock', 'high'),
    ('packaging', 'packaging_issue', 'medium'),
    ('competitor', 'competitor_issue', 'medium'),
    ('expiry', 'expiry_issue', 'high'),
    ('expired', 'expiry_issue', 'high'),
    ('rejected', 'rejection_issue', 'medium'),
    ('under consideration', 'opportunity', 'medium'),
    ('opportunit', 'opportunity', 'medium'),
    ('concern', 'store_challenge', 'medium'),
    ('credit', 'credit_note', 'medium'),
    ('damaged', 'packaging_issue', 'high'),
)

SKU_HINT_PATTERN = re.compile(
    r'(\d+\s?(ml|g|kg|cl|l|ltr|litre|litres)|yogh|yogurt|greek|protein|vanilla|strawberry|choco|juice|tea|drink|water)',
    re.I,
)


def _coerce_bytes(file_source) -> tuple[bytes, str]:
    if hasattr(file_source, 'read'):
        pos = None
        try:
            pos = file_source.tell()
        except Exception:
            pos = None
        raw = file_source.read()
        if pos is not None:
            try:
                file_source.seek(pos)
            except Exception:
                pass
        name = getattr(file_source, 'name', '') or getattr(file_source, 'filename', '') or ''
        return raw, os.path.basename(name)
    if isinstance(file_source, (bytes, bytearray)):
        return bytes(file_source), ''
    with open(file_source, 'rb') as fh:
        return fh.read(), os.path.basename(str(file_source))


def _looks_like_text(raw: bytes) -> bool:
    head = raw[:4096]
    if not head:
        return False
    if head.startswith(b'PK\x03\x04'):
        return False
    binary_hits = sum(1 for b in head if b == 0)
    return binary_hits == 0


def _looks_like_zip(raw: bytes) -> bool:
    return raw[:4] == b'PK\x03\x04'


def _looks_like_ooxml_excel(raw: bytes) -> bool:
    if not _looks_like_zip(raw):
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = {name.lower() for name in zf.namelist()}
    except Exception:
        return False
    return '[content_types].xml' in names and any(
        candidate in names
        for candidate in (
            'xl/workbook.xml',
            'xl/worksheets/sheet1.xml',
        )
    )


def _decode_text(raw: bytes) -> str:
    for enc in ('utf-8-sig', 'utf-16', 'latin1'):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode('utf-8', errors='ignore')


def _read_text_table(raw: bytes) -> pd.DataFrame:
    text = _decode_text(raw)
    sample = '\n'.join(text.splitlines()[:5])
    if '\t' in sample:
        return pd.read_csv(io.StringIO(text), sep='\t', dtype=str)
    return pd.read_csv(io.StringIO(text), sep=None, engine='python', dtype=str)


def _read_activity_file(raw: bytes, filename: str = '') -> tuple[pd.DataFrame, str]:
    ext = os.path.splitext(filename or '')[1].lower()
    if ext in ('.csv', '.txt', '.tsv'):
        return _read_text_table(raw), 'text'

    if _looks_like_text(raw):
        try:
            return _read_text_table(raw), 'text'
        except Exception:
            pass

    if ext in ('.xlsx', '.xlsm', '.xltx', '.xltm'):
        for engine in (None, 'openpyxl'):
            try:
                kwargs = {'dtype': str}
                if engine:
                    kwargs['engine'] = engine
                return pd.read_excel(io.BytesIO(raw), **kwargs), 'excel'
            except Exception:
                continue
        return _read_text_table(raw), 'text'

    if ext == '.xls':
        for engine in ('xlrd', 'openpyxl'):
            try:
                return pd.read_excel(io.BytesIO(raw), dtype=str, engine=engine), 'excel'
            except Exception:
                continue
        if _looks_like_ooxml_excel(raw):
            for engine in (None, 'openpyxl'):
                try:
                    kwargs = {'dtype': str}
                    if engine:
                        kwargs['engine'] = engine
                    return pd.read_excel(io.BytesIO(raw), **kwargs), 'excel'
                except Exception:
                    continue
        return _read_text_table(raw), 'text'

    for engine in (None, 'openpyxl', 'xlrd'):
        try:
            kwargs = {'dtype': str}
            if engine:
                kwargs['engine'] = engine
            return pd.read_excel(io.BytesIO(raw), **kwargs), 'excel'
        except Exception:
            continue

    return _read_text_table(raw), 'text'


def _infer_zip_survey_name(member_name: str) -> str:
    base = os.path.splitext(os.path.basename(member_name))[0]
    cleaned = re.sub(r'[_\-]+', ' ', base)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def _infer_zip_source_category(member_name: str) -> str:
    norm = _infer_zip_survey_name(member_name).lower()
    if 'corporate' in norm:
        return 'corporate_feedback'
    if 'general' in norm:
        return 'general_feedback'
    if 'outlet' in norm:
        return 'outlet_feedback'
    if 'credit' in norm or 'complaint' in norm or 'issue' in norm:
        return 'credit_notes_and_issues'
    return 'brand_feedback'


def _should_skip_zip_member(member_name: str) -> bool:
    norm = member_name.replace('\\', '/').lower()
    base = os.path.basename(norm)
    if base.startswith('~$'):
        return True
    if '/weekly_reports_output_' in norm:
        return True
    if 'activity report march week' in base:
        return True
    if 'weekly_brand_summary' in base:
        return True
    return False


def _read_activity_zip(raw: bytes) -> tuple[pd.DataFrame, dict]:
    frames = []
    manifest = []
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        members = [
            info for info in zf.infolist()
            if not info.is_dir()
            and info.filename.lower().endswith(('.xlsx', '.xls', '.csv', '.txt', '.tsv'))
            and not _should_skip_zip_member(info.filename)
        ]
        for info in members:
            with zf.open(info) as fh:
                member_raw = fh.read()
            frame, source_type = _read_activity_file(member_raw, info.filename)
            frame = _normalise_columns(frame)
            for col in frame.columns:
                frame[col] = frame[col].fillna('').astype(str).str.strip()
            if not frame.empty:
                survey_name = _infer_zip_survey_name(info.filename)
                source_category = _infer_zip_source_category(info.filename)
                if not frame['survey_name'].astype(str).str.strip().any():
                    frame['survey_name'] = survey_name
                frame['survey_code'] = frame['survey_code'].replace('', survey_name)
                frame['answer_type'] = frame['answer_type'].replace('', 'text')
                frame['activity_date'] = frame['activity_date'].apply(_to_iso_date)
                frame['survey_start_date'] = frame['survey_start_date'].apply(_to_iso_date)
                frame['survey_end_date'] = frame['survey_end_date'].apply(_to_iso_date)
                frame = frame[frame['activity_date'].notna()].copy()
                frame['source_category'] = source_category
                frame['source_file'] = os.path.basename(info.filename)
            frames.append(frame)
            manifest.append({
                'member_name': info.filename,
                'row_count': int(len(frame)),
                'source_type': source_type,
                'survey_name': _infer_zip_survey_name(info.filename),
                'source_category': _infer_zip_source_category(info.filename),
            })
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(EXPECTED_COLUMNS))
    if 'source_category' not in merged.columns:
        merged['source_category'] = ''
    if 'source_file' not in merged.columns:
        merged['source_file'] = ''
    meta = {
        'source_type': 'zip_bundle',
        'row_count': int(len(merged)),
        'bundle_manifest': manifest,
        'file_count': len(manifest),
    }
    return merged, meta


def _clean_colname(name: str) -> str:
    return re.sub(r'\s+', ' ', str(name or '').strip().replace('\ufeff', '')).lower()


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for col in df.columns:
        alias = COLUMN_ALIASES.get(_clean_colname(col))
        if alias:
            renamed[col] = alias
    df = df.rename(columns=renamed).copy()
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    return df[list(EXPECTED_COLUMNS)].copy()


def _to_iso_date(value) -> str | None:
    if value is None or str(value).strip() == '':
        return None
    dt = pd.to_datetime(value, errors='coerce', dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.strftime('%Y-%m-%d')


def load_activity_dataframe(file_source, expected_source: str | None = None) -> tuple[pd.DataFrame, dict]:
    raw, filename = _coerce_bytes(file_source)
    source_choice = str(expected_source or 'auto').strip().lower()
    ext = os.path.splitext(filename or '')[1].lower()
    is_zip_bundle = str(filename).lower().endswith('.zip')
    if not is_zip_bundle and _looks_like_zip(raw):
        is_zip_bundle = (
            ext not in ('.xlsx', '.xlsm', '.xltx', '.xltm', '.xls')
            and not _looks_like_ooxml_excel(raw)
        )

    if source_choice == 'cleaned_zip' and not is_zip_bundle:
        raise ValueError('Expected a cleaned weekly zip file, but the uploaded file is not a zip archive.')
    if source_choice == 'raw_export' and is_zip_bundle:
        raise ValueError('Expected a raw activity export file, but the uploaded file is a zip archive.')

    if is_zip_bundle:
        df, meta = _read_activity_zip(raw)
    else:
        df, source_type = _read_activity_file(raw, filename)
        df = _normalise_columns(df)
        for col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
        df['activity_date'] = df['activity_date'].apply(_to_iso_date)
        df['survey_start_date'] = df['survey_start_date'].apply(_to_iso_date)
        df['survey_end_date'] = df['survey_end_date'].apply(_to_iso_date)
        df = df[df['activity_date'].notna()].copy()
        meta = {
            'source_type': source_type,
            'row_count': len(df),
        }
    meta['source_selection'] = source_choice
    meta['filename'] = filename
    return df, meta


def _norm_text(value: str) -> str:
    return re.sub(r'\s+', ' ', str(value or '').strip().lower())


def _extract_brand_candidate_from_survey(survey_name: str) -> str | None:
    name = str(survey_name or '').strip()
    if not name:
        return None
    cleaned = re.sub(r'feedback|general feedback|survey', '', name, flags=re.I).strip(' -')
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if not cleaned or len(cleaned) < 3:
        return None
    return canonicalize_brand_name(cleaned)


def _build_brand_lookup(ds) -> list[tuple[str, str, int]]:
    if not ds:
        return []
    rows = []
    for brand in ds.get_all_brand_master(status='all'):
        canonical = str(brand['canonical_name']).strip()
        if canonical:
            rows.append((canonical, canonical.lower(), brand['id']))
        for alias in ds.get_brand_aliases(brand['id']):
            alias_text = str(alias['alias_name']).strip().lower()
            if alias_text:
                rows.append((canonical, alias_text, brand['id']))
    rows.sort(key=lambda item: len(item[1]), reverse=True)
    return rows


def _build_sku_lookup(ds) -> dict[int, list[tuple[str, str]]]:
    if not ds:
        return {}
    cache: dict[int, list[tuple[str, str]]] = {}
    for brand in ds.get_all_brand_master(status='all'):
        brand_id = int(brand['id'])
        rows: list[tuple[str, str]] = []
        for sku in ds.get_brand_skus(brand_id, status='all'):
            sku_name = str(sku['sku_name']).strip()
            if sku_name:
                rows.append((sku_name, sku_name.lower()))
        for alias in ds.get_sku_aliases(brand_id):
            sku = ds.get_sku_master(alias['sku_id'])
            if not sku:
                continue
            alias_text = str(alias['alias_name']).strip().lower()
            sku_name = str(sku['sku_name']).strip()
            if alias_text and sku_name:
                rows.append((sku_name, alias_text))
        deduped = []
        seen = set()
        for sku_name, key in rows:
            token = (sku_name, key)
            if token in seen:
                continue
            seen.add(token)
            deduped.append((sku_name, key))
        deduped.sort(key=lambda item: len(item[1]), reverse=True)
        cache[brand_id] = deduped
    return cache


def _extract_brand_mentions(text: str, brand_lookup: Iterable[tuple[str, str, int]]) -> list[dict]:
    norm = _norm_text(text)
    if not norm:
        return []
    matches = []
    seen = set()
    for canonical_name, alias_text, brand_id in brand_lookup:
        if len(alias_text) < 3:
            continue
        if alias_text in norm and canonical_name not in seen:
            matches.append({'brand_name': canonical_name, 'brand_id': brand_id})
            seen.add(canonical_name)
    return matches


def _extract_sku_mentions(text: str, sku_lookup: dict[int, list[tuple[str, str]]], brand_id: int | None) -> list[str]:
    if not brand_id:
        return []
    norm = _norm_text(text)
    if not norm:
        return []
    matches = []
    seen = set()
    for sku_name, key in sku_lookup.get(int(brand_id), []):
        if len(key) >= 4 and key in norm and sku_name not in seen:
            matches.append(sku_name)
            seen.add(sku_name)
    return matches


def _issue_type(question: str, label: str, answer: str, answer_type: str) -> tuple[str | None, str]:
    blob = ' '.join(str(v or '') for v in (question, label, answer)).lower()
    if answer_type and answer_type.lower() == 'image' and not str(answer or '').strip():
        return None, 'medium'
    if not str(answer or '').strip() and answer_type.lower() != 'image':
        return None, 'medium'
    for key, issue_type, severity in ISSUE_KEYWORDS:
        if key in blob:
            return issue_type, severity
    if 'what should dala know' in blob or 'general feedback' in blob:
        return 'general_feedback', 'low'
    return None, 'medium'


def _opportunity_count(issue_type: str | None) -> int:
    return 1 if issue_type == 'opportunity' else 0


def _photo_count(answer_type: str, answer: str) -> int:
    return 1 if str(answer_type or '').lower() == 'image' and str(answer or '').strip() else 0


def build_activity_payload(df: pd.DataFrame, ds=None, source_filename: str = '',
                           report_id: int = None, progress_cb=None, source_meta: dict | None = None) -> dict:
    df = df.copy()
    if df.empty:
        return {
            'summary': {'row_count': 0, 'start_date': None, 'end_date': None, 'brands_detected': 0},
            'events': [],
            'visits': [],
            'issues': [],
            'brand_mentions': [],
        }

    events = []
    issues = []
    brand_mentions = []
    unmatched_brand_candidates = set()
    unmatched_sku_candidates = set()
    brand_lookup = _build_brand_lookup(ds) if ds else []
    sku_lookup = _build_sku_lookup(ds) if ds else {}
    survey_resolution_cache: dict[str, dict | None] = {}

    total_rows = max(len(df), 1)
    for idx, row in enumerate(df.to_dict(orient='records'), start=1):
        clean = {k: (None if str(v).strip() in ('', 'nan', 'NaT') else str(v).strip()) for k, v in row.items()}
        events.append(clean)

        if progress_cb and (idx == 1 or idx == total_rows or idx % 250 == 0):
            progress = 22 + int((idx / total_rows) * 38)
            progress_cb(progress, f"Reading activity rows {idx:,} of {total_rows:,}")

        survey_guess = _extract_brand_candidate_from_survey(clean.get('survey_name'))
        source_category = str(clean.get('source_category') or '').strip().lower()
        brand_rows = []
        if survey_guess and ds:
            if survey_guess not in survey_resolution_cache:
                survey_resolution_cache[survey_guess] = ds.resolve_brand_master(survey_guess)
            resolved = survey_resolution_cache[survey_guess]
            if resolved:
                brand_rows.append({'brand_name': resolved['canonical_name'], 'brand_id': resolved['id'], 'source_kind': 'survey_name'})
            else:
                unmatched_brand_candidates.add(survey_guess)

        text_blob = ' '.join(
            str(clean.get(k) or '')
            for k in ('survey_name', 'question', 'label', 'answer')
        )
        should_scan_text = (
            not brand_rows
            or source_category in {'general_feedback', 'outlet_feedback', 'corporate_feedback', 'credit_notes_and_issues'}
            or not source_category
        )
        if should_scan_text:
            brand_rows.extend(
                {**match, 'source_kind': 'text'} for match in _extract_brand_mentions(text_blob, brand_lookup)
            )

        deduped_brands = []
        seen_brands = set()
        for match in brand_rows:
            if match['brand_name'] in seen_brands:
                continue
            deduped_brands.append(match)
            seen_brands.add(match['brand_name'])

        issue_type, severity = _issue_type(
            clean.get('question') or '',
            clean.get('label') or '',
            clean.get('answer') or '',
            clean.get('answer_type') or '',
        )

        for match in deduped_brands:
            sku_names = _extract_sku_mentions(text_blob, sku_lookup, match['brand_id']) if SKU_HINT_PATTERN.search(text_blob) else []
            if not sku_names and clean.get('answer') and 'ml' in clean['answer'].lower():
                unmatched_sku_candidates.add((match['brand_name'], clean['answer']))
            brand_mentions.append({
                'brand_name': match['brand_name'],
                'sku_name': sku_names[0] if sku_names else None,
                'retailer_code': clean.get('retailer_code'),
                'retailer_name': clean.get('retailer_name'),
                'activity_date': clean.get('activity_date'),
                'source_kind': match.get('source_kind'),
                'source_value': clean.get('answer') or clean.get('label') or clean.get('question'),
            })
            if issue_type:
                issues.append({
                    'activity_date': clean.get('activity_date'),
                    'retailer_code': clean.get('retailer_code'),
                    'retailer_name': clean.get('retailer_name'),
                    'salesman_name': clean.get('salesman_name'),
                    'brand_name': match['brand_name'],
                    'sku_name': sku_names[0] if sku_names else None,
                    'issue_type': issue_type,
                    'severity': severity,
                    'question': clean.get('question'),
                    'label': clean.get('label'),
                    'answer': clean.get('answer'),
                })

        if issue_type and not deduped_brands:
            issues.append({
                'activity_date': clean.get('activity_date'),
                'retailer_code': clean.get('retailer_code'),
                'retailer_name': clean.get('retailer_name'),
                'salesman_name': clean.get('salesman_name'),
                'brand_name': survey_guess,
                'sku_name': None,
                'issue_type': issue_type,
                'severity': severity,
                'question': clean.get('question'),
                'label': clean.get('label'),
                'answer': clean.get('answer'),
            })

    if progress_cb:
        progress_cb(66, 'Building visit summary')

    visit_rows = []
    grouped = df.copy()
    grouped['visit_key'] = (
        grouped['activity_date'].fillna('') + '|' +
        grouped['salesman_name'].fillna('') + '|' +
        grouped['retailer_code'].fillna('') + '|' +
        grouped['survey_name'].fillna('')
    )
    for visit_key, grp in grouped.groupby('visit_key', dropna=False):
        rows = grp.to_dict(orient='records')
        first = rows[0]
        visit_issue_count = 0
        visit_opp_count = 0
        visit_photo_count = 0
        for row in rows:
            issue_type, _severity = _issue_type(row.get('question'), row.get('label'), row.get('answer'), row.get('answer_type') or '')
            if issue_type:
                visit_issue_count += 1
                visit_opp_count += _opportunity_count(issue_type)
            visit_photo_count += _photo_count(row.get('answer_type') or '', row.get('answer') or '')
        visit_rows.append({
            'visit_key': visit_key,
            'activity_date': first.get('activity_date'),
            'salesman_name': first.get('salesman_name'),
            'survey_name': first.get('survey_name'),
            'retailer_code': first.get('retailer_code'),
            'retailer_name': first.get('retailer_name'),
            'retailer_state': first.get('retailer_state'),
            'retailer_city': first.get('retailer_city'),
            'event_count': len(rows),
            'issue_count': visit_issue_count,
            'opportunity_count': visit_opp_count,
            'photo_count': visit_photo_count,
        })

    if progress_cb:
        progress_cb(78, 'Checking brand and SKU matches')

    if ds:
        for brand_name in sorted(unmatched_brand_candidates):
            candidate = ds.find_brand_duplicate_candidate(brand_name)
            ds.queue_catalog_candidate(
                'brand',
                brand_name,
                canonical_candidate=brand_name,
                brand_candidate=None,
                source_report_id=report_id,
                source_filename=source_filename,
                reason='possible_duplicate' if candidate else 'new_detected',
                suggested_match_name=candidate['brand']['canonical_name'] if candidate else None,
                similarity_score=candidate['score'] if candidate else 0.0,
            )
        for brand_name, sku_name in sorted(unmatched_sku_candidates):
            brand = ds.resolve_brand_master(brand_name)
            candidate = ds.find_sku_duplicate_candidate(brand['id'], sku_name) if brand else None
            ds.queue_catalog_candidate(
                'sku',
                sku_name,
                canonical_candidate=sku_name,
                brand_candidate=brand_name,
                source_report_id=report_id,
                source_filename=source_filename,
                reason='possible_duplicate' if candidate else 'new_detected',
                suggested_match_name=candidate['sku']['sku_name'] if candidate else None,
                similarity_score=candidate['score'] if candidate else 0.0,
            )

    if progress_cb:
        progress_cb(88, 'Preparing summary')

    activity_dates = [d for d in df['activity_date'].dropna().tolist() if d]
    nonempty_salespeople = (
        df['salesman_name'].fillna('').astype(str).str.strip().replace('', pd.NA).dropna().nunique()
        if 'salesman_name' in df.columns else 0
    )
    summary = {
        'row_count': int(len(df)),
        'start_date': min(activity_dates) if activity_dates else None,
        'end_date': max(activity_dates) if activity_dates else None,
        'brands_detected': len({m['brand_name'] for m in brand_mentions if m.get('brand_name')}),
        'stores_visited': int(df['retailer_code'].nunique()),
        'salespeople': int(nonempty_salespeople),
        'issue_count': len(issues),
        'visit_count': len(visit_rows),
    }
    if source_meta:
        summary['source_meta'] = source_meta
        if source_meta.get('source_type') == 'zip_bundle' and int(nonempty_salespeople) == 0:
            summary['quality_flags'] = ['salesperson_metadata_missing_in_cleaned_zip']

    return {
        'summary': summary,
        'events': events,
        'visits': visit_rows,
        'issues': issues,
        'brand_mentions': brand_mentions,
    }
