"""
gmv.py

Shared helpers for building a brand GMV window from monthly history and
rendering it as lightweight inline SVG for the sales report surfaces.
"""

from __future__ import annotations

from datetime import datetime
from html import escape

from .predictor import _forecast_band, _forecast_point, _history_profile, _month_index


def _parse_month_start(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return datetime(value.year, value.month, 1)
    text = str(value)[:10]
    for fmt in ('%Y-%m-%d', '%Y-%m', '%b %Y', '%Y/%m'):
        try:
            dt = datetime.strptime(text, fmt)
            return datetime(dt.year, dt.month, 1)
        except Exception:
            continue
    return None


def _month_add(base: datetime, months: int) -> datetime:
    month_zero = (base.year * 12) + (base.month - 1) + months
    year = month_zero // 12
    month = (month_zero % 12) + 1
    return datetime(year, month, 1)


def _month_diff(start: datetime, end: datetime) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _mean(values) -> float:
    return sum(values) / len(values) if values else 0.0


def format_naira_compact(value: float) -> str:
    value = float(value or 0)
    if value >= 1_000_000:
        return f"₦{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"₦{value / 1_000:.1f}K"
    return f"₦{value:,.0f}"


def format_naira_full(value: float) -> str:
    return f"₦{float(value or 0):,.0f}"


def build_gmv_window(history, cutoff_date=None, back_months: int = 6, forward_months: int = 6) -> dict:
    monthly_rows = [dict(row) for row in (history or []) if row.get('report_type') == 'monthly']
    monthly_rows.sort(key=lambda row: _month_index(row, 0))

    cutoff_month = _parse_month_start(cutoff_date)
    if cutoff_month:
        cutoff_idx = cutoff_month.year * 12 + cutoff_month.month
        filtered_rows = []
        for row in monthly_rows:
            row_month = _parse_month_start(row.get('start_date')) or cutoff_month
            row_idx = row_month.year * 12 + row_month.month
            if row_idx <= cutoff_idx:
                filtered_rows.append(row)
        monthly_rows = filtered_rows

    if not monthly_rows:
        return {
            'available': False,
            'points': [],
            'relationship_months': 0,
            'back_months_shown': 0,
            'forward_months_shown': 0,
            'anchor_label': None,
            'anchor_value': 0.0,
            'current_gmv_display': '₦0',
            'history_average_display': '₦0',
            'forward_total_display': '₦0',
            'forward_range_display': '₦0 to ₦0',
            'window_note': 'GMV will appear once monthly history is available.',
        }

    anchor = monthly_rows[-1]
    anchor_month = _parse_month_start(anchor.get('start_date')) or datetime.today().replace(day=1)
    first_month = _parse_month_start(monthly_rows[0].get('start_date')) or anchor_month
    relationship_months = max(1, _month_diff(first_month, anchor_month) + 1)

    actual_rows = monthly_rows[max(0, len(monthly_rows) - (back_months + 1)):]
    actual_points = [
        {
            'label': row.get('month_label') or (anchor_month.strftime('%b %Y')),
            'month_start': _parse_month_start(row.get('start_date')) or anchor_month,
            'value': float(row.get('total_revenue', 0) or 0),
            'kind': 'actual',
            'is_anchor': row is anchor,
        }
        for row in actual_rows
    ]

    prior_values = [float(row.get('total_revenue', 0) or 0) for row in monthly_rows]
    profile = _history_profile(monthly_rows)
    forecast_seed = list(prior_values)
    forecast_points = []
    for step in range(1, forward_months + 1):
        forecast_value, _ = _forecast_point(forecast_seed, 1)
        low, high = _forecast_band(forecast_value, forecast_seed, step, profile['confidence_score'])
        month_start = _month_add(anchor_month, step)
        forecast_points.append({
            'label': month_start.strftime('%b %Y'),
            'month_start': month_start,
            'value': round(float(forecast_value), 2),
            'low': float(low),
            'high': float(high),
            'kind': 'forecast',
            'is_anchor': False,
        })
        forecast_seed.append(float(forecast_value))

    points = actual_points + forecast_points
    actual_values = [point['value'] for point in actual_points] or [0.0]
    forecast_values = [point['value'] for point in forecast_points] or [0.0]
    back_months_shown = max(0, len(actual_points) - 1)

    if relationship_months <= 1:
        window_note = 'This brand is early in its journey with DALA, so the GMV view starts from the first live month.'
    elif back_months_shown < back_months:
        window_note = (
            f"Showing {back_months_shown} month{'s' if back_months_shown != 1 else ''} back because "
            f"this brand has {relationship_months} month{'s' if relationship_months != 1 else ''} with DALA."
        )
    else:
        window_note = 'Showing the latest 6 months of actual GMV with a 6 month forward view.'

    forward_total = sum(forecast_values)
    forward_low_total = sum(point.get('low', point['value']) for point in forecast_points)
    forward_high_total = sum(point.get('high', point['value']) for point in forecast_points)

    return {
        'available': True,
        'anchor_label': actual_points[-1]['label'],
        'anchor_value': actual_points[-1]['value'],
        'relationship_months': relationship_months,
        'back_months_shown': back_months_shown,
        'forward_months_shown': len(forecast_points),
        'current_gmv_display': format_naira_full(actual_points[-1]['value']),
        'history_average_display': format_naira_compact(_mean(actual_values)),
        'forward_total_display': format_naira_compact(forward_total),
        'forward_range_display': (
            f"{format_naira_compact(forward_low_total)} to {format_naira_compact(forward_high_total)}"
        ),
        'window_note': window_note,
        'points': points,
    }


def render_gmv_window_svg(window: dict, width: int = 760, height: int = 188) -> str:
    if not window or not window.get('available') or not window.get('points'):
        return (
            '<div style="padding:18px;border:1px solid #DDE3ED;border-radius:14px;'
            'background:#F7F9FD;color:#6B7A99;font-size:12px;">'
            'GMV history will show here after the first monthly report is saved.'
            '</div>'
        )

    points = window['points']
    max_value = max(
        max(float(point.get('value', 0) or 0), float(point.get('high', point.get('value', 0)) or 0))
        for point in points
    )
    max_value = max(max_value, 1.0)

    left = 44
    right = 18
    top = 18
    bottom = 38
    inner_w = width - left - right
    inner_h = height - top - bottom
    step_x = inner_w / max(len(points) - 1, 1)

    def _x(idx: int) -> float:
        return left + (idx * step_x)

    def _y(value: float) -> float:
        scaled = max(min(float(value or 0) / max_value, 1.0), 0.0)
        return top + inner_h - (scaled * inner_h * 0.92)

    actual_points = [(idx, point) for idx, point in enumerate(points) if point.get('kind') == 'actual']
    forecast_points = [(idx, point) for idx, point in enumerate(points) if point.get('kind') == 'forecast']

    grid = []
    for guide in (0.25, 0.5, 0.75, 1.0):
        y = top + inner_h - (guide * inner_h * 0.92)
        value = max_value * guide
        grid.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" '
            f'stroke="#E8EDF6" stroke-width="1" />'
            f'<text x="6" y="{y + 4:.1f}" fill="#7A849E" font-size="10">{escape(format_naira_compact(value))}</text>'
        )

    anchor_idx = max(idx for idx, point in actual_points if point.get('is_anchor'))
    anchor_x = _x(anchor_idx)

    actual_path = ''
    if actual_points:
        actual_coords = ' '.join(f'{_x(idx):.1f},{_y(point["value"]):.1f}' for idx, point in actual_points)
        actual_path = (
            f'<polyline points="{actual_coords}" fill="none" stroke="#E8192C" '
            f'stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
        )

    band_path = ''
    forecast_path = ''
    if forecast_points:
        high_points = ' '.join(f'{_x(idx):.1f},{_y(point.get("high", point["value"])):.1f}' for idx, point in forecast_points)
        low_points = ' '.join(
            f'{_x(idx):.1f},{_y(point.get("low", point["value"])):.1f}'
            for idx, point in reversed(forecast_points)
        )
        band_path = (
            f'<polygon points="{high_points} {low_points}" fill="rgba(46,134,193,0.18)" stroke="none" />'
        )
        forecast_coords = ' '.join(f'{_x(idx):.1f},{_y(point["value"]):.1f}' for idx, point in forecast_points)
        forecast_path = (
            f'<polyline points="{forecast_coords}" fill="none" stroke="#2E86C1" stroke-width="3" '
            f'stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="6 5" />'
        )

    point_marks = []
    for idx, point in actual_points:
        point_marks.append(
            f'<circle cx="{_x(idx):.1f}" cy="{_y(point["value"]):.1f}" r="4.2" fill="#E8192C" stroke="#FFFFFF" stroke-width="2" />'
        )
    for idx, point in forecast_points:
        point_marks.append(
            f'<circle cx="{_x(idx):.1f}" cy="{_y(point["value"]):.1f}" r="4" fill="#2E86C1" stroke="#FFFFFF" stroke-width="2" />'
        )

    labels = []
    for idx, point in enumerate(points):
        labels.append(
            f'<text x="{_x(idx):.1f}" y="{height - 12}" text-anchor="middle" fill="#6B7A99" '
            f'font-size="10">{escape(point["month_start"].strftime("%b %y"))}</text>'
        )

    anchor_value = points[anchor_idx]['value']
    anchor_label = escape(points[anchor_idx]['label'])
    anchor_note_y = max(top + 14, _y(anchor_value) - 10)

    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
        f'style="display:block;width:100%;height:auto;">'
        f'<rect x="0" y="0" width="{width}" height="{height}" rx="16" fill="#FFFFFF" />'
        f'{"".join(grid)}'
        f'<line x1="{anchor_x:.1f}" y1="{top - 2}" x2="{anchor_x:.1f}" y2="{height - bottom + 6}" '
        f'stroke="#C6D1E5" stroke-width="1.5" stroke-dasharray="4 5" />'
        f'{band_path}'
        f'{actual_path}'
        f'{forecast_path}'
        f'{"".join(point_marks)}'
        f'<text x="{anchor_x + 8:.1f}" y="{anchor_note_y:.1f}" fill="#1B2B5E" font-size="10" font-weight="700">'
        f'{anchor_label}: {escape(format_naira_compact(anchor_value))}</text>'
        f'{"".join(labels)}'
        '</svg>'
    )
