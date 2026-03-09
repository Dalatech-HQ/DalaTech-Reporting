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

    for engine in (None, 'openpyxl', 'xlrd'):
        try:
            kwargs = {'dtype': str}
            if engine:
                kwargs['engine'] = engine
            return pd.read_excel(io.BytesIO(raw), **kwargs), 'excel'
        except Exception:
            continue

    return _read_text_table(raw), 'text'


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


def load_activity_dataframe(file_source) -> tuple[pd.DataFrame, dict]:
    raw, filename = _coerce_bytes(file_source)
    df, source_type = _read_activity_file(raw, filename)
    df = _normalise_columns(df)
    for col in df.columns:
        df[col] = df[col].fillna('').astype(str).str.strip()
    df['activity_date'] = df['activity_date'].apply(_to_iso_date)
    df['survey_start_date'] = df['survey_start_date'].apply(_to_iso_date)
    df['survey_end_date'] = df['survey_end_date'].apply(_to_iso_date)
    df = df[df['activity_date'].notna()].copy()
    meta = {
        'filename': filename,
        'source_type': source_type,
        'row_count': len(df),
    }
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


def _iter_brand_lookup(ds) -> Iterable[tuple[str, str, int]]:
    if not ds:
        return []
    rows = []
    for brand in ds.get_all_brand_master(status='all'):
        rows.append((brand['canonical_name'], brand['canonical_name'].lower(), brand['id']))
        for alias in ds.get_brand_aliases(brand['id']):
            rows.append((brand['canonical_name'], str(alias['alias_name']).strip().lower(), brand['id']))
    rows.sort(key=lambda item: len(item[1]), reverse=True)
    return rows


def _extract_brand_mentions(text: str, ds) -> list[dict]:
    norm = _norm_text(text)
    if not norm or not ds:
        return []
    matches = []
    seen = set()
    for canonical_name, alias_text, brand_id in _iter_brand_lookup(ds):
        if len(alias_text) < 3:
            continue
        if alias_text in norm and canonical_name not in seen:
            matches.append({'brand_name': canonical_name, 'brand_id': brand_id})
            seen.add(canonical_name)
    return matches


def _extract_sku_mentions(text: str, ds, brand_id: int | None) -> list[str]:
    if not ds or not brand_id:
        return []
    norm = _norm_text(text)
    if not norm:
        return []
    matches = []
    seen = set()
    for sku in ds.get_brand_skus(brand_id, status='all'):
        sku_name = str(sku['sku_name']).strip()
        key = sku_name.lower()
        if len(key) >= 4 and key in norm and sku_name not in seen:
            matches.append(sku_name)
            seen.add(sku_name)
    for alias in ds.get_sku_aliases(brand_id):
        alias_name = str(alias['alias_name']).strip()
        key = alias_name.lower()
        if len(key) >= 4 and key in norm:
            sku = ds.get_sku_master(alias['sku_id'])
            if sku and sku['sku_name'] not in seen:
                matches.append(sku['sku_name'])
                seen.add(sku['sku_name'])
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
                           report_id: int = None, progress_cb=None) -> dict:
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

    total_rows = max(len(df), 1)
    for idx, row in enumerate(df.to_dict(orient='records'), start=1):
        clean = {k: (None if str(v).strip() in ('', 'nan', 'NaT') else str(v).strip()) for k, v in row.items()}
        events.append(clean)

        if progress_cb and (idx == 1 or idx == total_rows or idx % 250 == 0):
            progress = 22 + int((idx / total_rows) * 38)
            progress_cb(progress, f"Reading activity rows {idx:,} of {total_rows:,}")

        survey_guess = _extract_brand_candidate_from_survey(clean.get('survey_name'))
        brand_rows = []
        if survey_guess and ds:
            resolved = ds.resolve_brand_master(survey_guess)
            if resolved:
                brand_rows.append({'brand_name': resolved['canonical_name'], 'brand_id': resolved['id'], 'source_kind': 'survey_name'})
            else:
                unmatched_brand_candidates.add(survey_guess)

        text_blob = ' '.join(
            str(clean.get(k) or '')
            for k in ('survey_name', 'question', 'label', 'answer')
        )
        brand_rows.extend(
            {**match, 'source_kind': 'text'} for match in _extract_brand_mentions(text_blob, ds)
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
            sku_names = _extract_sku_mentions(text_blob, ds, match['brand_id'])
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
    summary = {
        'row_count': int(len(df)),
        'start_date': min(activity_dates) if activity_dates else None,
        'end_date': max(activity_dates) if activity_dates else None,
        'brands_detected': len({m['brand_name'] for m in brand_mentions if m.get('brand_name')}),
        'stores_visited': int(df['retailer_code'].nunique()),
        'salespeople': int(df['salesman_name'].nunique()),
        'issue_count': len(issues),
        'visit_count': len(visit_rows),
    }

    return {
        'summary': summary,
        'events': events,
        'visits': visit_rows,
        'issues': issues,
        'brand_mentions': brand_mentions,
    }
