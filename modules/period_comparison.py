from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd


def normalize_report_type(report_type: str | None) -> str:
    value = str(report_type or '').strip().lower()
    if value in {'weekly', 'biweekly', 'monthly', 'quarterly', 'halfyear', 'yearly'}:
        return value
    return 'custom'


def comparison_grain(report_type: str | None) -> str:
    report_type = normalize_report_type(report_type)
    if report_type in {'weekly', 'biweekly'}:
        return 'day'
    if report_type == 'monthly':
        return 'week'
    if report_type in {'quarterly', 'halfyear', 'yearly'}:
        return 'month'
    return 'day'


def comparison_basis_label(report_type: str | None) -> str:
    grain = comparison_grain(report_type)
    if grain == 'day':
        return 'day-by-day'
    if grain == 'week':
        return 'week-by-week'
    if grain == 'month':
        return 'month-by-month'
    return 'period-by-period'


def _to_frame(rows) -> pd.DataFrame:
    if rows is None:
        return pd.DataFrame()
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(list(rows))


def _coerce_datetime(value):
    return pd.to_datetime(value, errors='coerce')


def _bucket_index(ts: pd.Timestamp, start: pd.Timestamp, grain: str) -> int:
    if pd.isna(ts) or pd.isna(start):
        return -1
    ts = ts.normalize()
    start = start.normalize()
    if grain == 'day':
        return max(0, int((ts - start).days))
    if grain == 'week':
        return max(0, int((ts - start).days // 7))
    return max(0, int((ts.year - start.year) * 12 + (ts.month - start.month)))


def _bucket_label(start: pd.Timestamp, bucket: int, grain: str) -> str:
    if pd.isna(start) or bucket < 0:
        return f'Bucket {bucket + 1}'
    if grain == 'day':
        return (start.normalize() + timedelta(days=bucket)).strftime('%a %d %b')
    if grain == 'week':
        return f'Week {bucket + 1}'
    if grain == 'month':
        dt = start + pd.DateOffset(months=bucket)
        return dt.strftime('%b')
    return f'Bucket {bucket + 1}'


def aggregate_period_rows(
    rows,
    *,
    date_key: str,
    metric_columns: Iterable[str],
    report_type: str | None,
    period_start: str | None = None,
) -> pd.DataFrame:
    df = _to_frame(rows)
    if df.empty or date_key not in df.columns:
        return pd.DataFrame(columns=['bucket', 'label', *metric_columns])

    df = df.copy()
    df[date_key] = pd.to_datetime(df[date_key], errors='coerce')
    df = df[df[date_key].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=['bucket', 'label', *metric_columns])

    grain = comparison_grain(report_type)
    start = _coerce_datetime(period_start) if period_start else df[date_key].min()
    if pd.isna(start):
        start = df[date_key].min()

    df['_bucket'] = df[date_key].apply(lambda ts: _bucket_index(ts, start, grain))
    grouped = df.groupby('_bucket', dropna=False)
    result = grouped.agg({col: 'sum' for col in metric_columns if col in df.columns}).reset_index()
    result = result.rename(columns={'_bucket': 'bucket'})
    result['label'] = result['bucket'].apply(lambda bucket: _bucket_label(start, int(bucket), grain))
    ordered_cols = ['bucket', 'label', *[col for col in metric_columns if col in result.columns]]
    return result[ordered_cols].sort_values('bucket').reset_index(drop=True)


def build_period_comparison(
    current_rows,
    previous_rows,
    *,
    date_key: str,
    metric_columns: Iterable[str],
    report_type: str | None,
    current_start: str | None = None,
    previous_start: str | None = None,
) -> dict:
    grain = comparison_grain(report_type)
    current_df = aggregate_period_rows(
        current_rows,
        date_key=date_key,
        metric_columns=metric_columns,
        report_type=report_type,
        period_start=current_start,
    )
    previous_df = aggregate_period_rows(
        previous_rows,
        date_key=date_key,
        metric_columns=metric_columns,
        report_type=report_type,
        period_start=previous_start,
    )

    current_map = {int(row['bucket']): row for _, row in current_df.iterrows()}
    previous_map = {int(row['bucket']): row for _, row in previous_df.iterrows()}
    all_buckets = sorted(set(current_map) | set(previous_map))

    rows = []
    for bucket in all_buckets:
        current_row = current_map.get(bucket, {})
        previous_row = previous_map.get(bucket, {})
        label = str(current_row.get('label') or previous_row.get('label') or f'Bucket {bucket + 1}')
        current_values = {}
        previous_values = {}
        deltas = {}
        for metric in metric_columns:
            cur = float(current_row.get(metric, 0) or 0)
            prev = float(previous_row.get(metric, 0) or 0)
            current_values[metric] = cur
            previous_values[metric] = prev
            if prev == 0:
                deltas[metric] = None if cur == 0 else 100.0
            else:
                deltas[metric] = round(((cur - prev) / prev) * 100, 1)
        rows.append({
            'bucket': bucket,
            'label': label,
            'current': current_values,
            'previous': previous_values,
            'delta_pct': deltas,
        })

    return {
        'grain': grain,
        'grain_label': comparison_basis_label(report_type),
        'rows': rows,
        'current': current_df.to_dict('records'),
        'previous': previous_df.to_dict('records'),
    }


def compare_same_type_reports(reports: list[dict], current_report: dict) -> dict:
    """Return previous comparable and same-period-last-year reports from a report list."""
    report_type = normalize_report_type(current_report.get('report_type'))
    current_start = _coerce_datetime(current_report.get('start_date'))
    current_end = _coerce_datetime(current_report.get('end_date'))
    current_bucket = None
    if not pd.isna(current_start):
        if report_type == 'monthly':
            current_bucket = (current_start.year, current_start.month)
        elif report_type == 'quarterly':
            current_bucket = (current_start.year, ((current_start.month - 1) // 3) + 1)
        elif report_type == 'halfyear':
            current_bucket = (current_start.year, 1 if current_start.month <= 6 else 2)
        elif report_type == 'yearly':
            current_bucket = (current_start.year,)
        elif report_type == 'weekly':
            iso = current_start.isocalendar()
            current_bucket = (iso.year, iso.week)
        elif report_type == 'biweekly':
            current_bucket = (current_start.year, int((current_start.dayofyear - 1) // 14) + 1)

    same_type = [
        dict(row)
        for row in reports
        if normalize_report_type(row.get('report_type')) == report_type and row.get('start_date')
    ]
    for row in same_type:
        row['_start_dt'] = _coerce_datetime(row.get('start_date'))
        row['_end_dt'] = _coerce_datetime(row.get('end_date'))
        if report_type == 'monthly':
            row['_bucket'] = (row['_start_dt'].year, row['_start_dt'].month)
        elif report_type == 'quarterly':
            row['_bucket'] = (row['_start_dt'].year, ((row['_start_dt'].month - 1) // 3) + 1)
        elif report_type == 'halfyear':
            row['_bucket'] = (row['_start_dt'].year, 1 if row['_start_dt'].month <= 6 else 2)
        elif report_type == 'yearly':
            row['_bucket'] = (row['_start_dt'].year,)
        elif report_type == 'weekly':
            iso = row['_start_dt'].isocalendar()
            row['_bucket'] = (iso.year, iso.week)
        elif report_type == 'biweekly':
            row['_bucket'] = (row['_start_dt'].year, int((row['_start_dt'].dayofyear - 1) // 14) + 1)
        else:
            row['_bucket'] = None

    same_type.sort(key=lambda item: item.get('_start_dt') if not pd.isna(item.get('_start_dt')) else pd.Timestamp.min)
    current_index = None
    for idx, row in enumerate(same_type):
        if row.get('id') == current_report.get('id'):
            current_index = idx
            break
        if not pd.isna(current_start) and row.get('_start_dt') == current_start:
            current_index = idx
            break

    previous = same_type[current_index - 1] if current_index and current_index > 0 else None
    trailing = same_type[max(0, (current_index or len(same_type)) - 4):current_index] if current_index else same_type[-4:]

    same_period_last_year = None
    if current_bucket is not None:
        target_year = current_start.year - 1 if not pd.isna(current_start) else None
        candidates = []
        for row in same_type:
            if row.get('_bucket') is None or pd.isna(row.get('_start_dt')):
                continue
            if target_year is not None:
                if report_type in {'monthly', 'quarterly', 'halfyear', 'yearly'}:
                    if row['_bucket'][0] == target_year and row['_bucket'][1:] == current_bucket[1:]:
                        candidates.append(row)
                elif report_type == 'weekly':
                    target = current_start - timedelta(days=364)
                    if abs((row['_start_dt'] - target).days) <= 7:
                        candidates.append(row)
                elif report_type == 'biweekly':
                    target = current_start - timedelta(days=364)
                    if abs((row['_start_dt'] - target).days) <= 14:
                        candidates.append(row)
        if candidates:
            same_period_last_year = sorted(candidates, key=lambda row: abs((row['_start_dt'] - (current_start - timedelta(days=364))).days) if not pd.isna(current_start) else 0)[0]

    for row in same_type:
        row.pop('_start_dt', None)
        row.pop('_end_dt', None)
        row.pop('_bucket', None)

    return {
        'previous': previous,
        'trailing': trailing,
        'same_period_last_year': same_period_last_year,
    }
