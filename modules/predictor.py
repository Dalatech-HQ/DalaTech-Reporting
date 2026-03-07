"""
predictor.py — Predictive analytics for DALA brand partners.

Provides:
  - next_month_revenue_forecast(history)  — linear trend projection
  - stock_depletion_date(kpis)            — when stock runs out
  - reorder_prediction(brand_history)     — expected next reorder window
  - growth_label(history)                 — 'Growing', 'Stable', 'Declining'
"""

from datetime import datetime, timedelta
from math import sqrt

from .brand_names import canonicalize_brand_name


def _linear_trend(values):
    """
    Simple least-squares slope over a list of values.
    Returns (slope, r_squared) or (0, 0) if insufficient data.
    """
    n = len(values)
    if n < 2:
        return 0, 0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    if den == 0:
        return 0, 0
    slope = num / den
    # R-squared
    y_pred = [y_mean + slope * (x - x_mean) for x in xs]
    ss_res = sum((y - yp) ** 2 for y, yp in zip(values, y_pred))
    ss_tot = sum((y - y_mean) ** 2 for y in values)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return slope, max(r2, 0)


def _mean(values):
    return sum(values) / len(values) if values else 0


def _stdev(values):
    if len(values) < 2:
        return 0
    avg = _mean(values)
    return sqrt(sum((v - avg) ** 2 for v in values) / len(values))


def _month_index(row, fallback_idx):
    """Extract a comparable month index from a history row."""
    for key in ('start_date', 'date'):
        value = row.get(key)
        if value:
            try:
                dt = datetime.strptime(str(value)[:10], '%Y-%m-%d')
                return dt.year * 12 + dt.month
            except Exception:
                pass

    value = row.get('month_label')
    if value:
        for fmt in ('%b %Y', '%Y-%m', '%Y/%m'):
            try:
                dt = datetime.strptime(str(value), fmt)
                return dt.year * 12 + dt.month
            except Exception:
                pass

    year = row.get('year')
    month = row.get('month')
    if year and month:
        return int(year) * 12 + int(month)

    return fallback_idx


def _history_profile(history):
    """Compute forecasting readiness metrics from a history series."""
    n = len(history)
    if not history:
        return {
            'data_points': 0,
            'consecutive_months': 0,
            'max_consecutive_months': 0,
            'r_squared': 0.0,
            'coefficient_of_variation': 0.0,
            'confidence_score': 0,
            'confidence_band': 'Insufficient',
        }

    revenues = [max(float(h.get('total_revenue', 0) or 0), 0) for h in history]
    slope, r2 = _linear_trend(revenues[-12:])
    month_indexes = [_month_index(row, idx) for idx, row in enumerate(history)]

    streak = 1 if month_indexes else 0
    best = streak
    for idx in range(1, len(month_indexes)):
        if month_indexes[idx] == month_indexes[idx - 1] + 1:
            streak += 1
        else:
            streak = 1
        best = max(best, streak)

    recent = revenues[-min(6, len(revenues)):]
    avg_recent = _mean(recent)
    cv = (_stdev(recent) / avg_recent) if avg_recent > 0 else 0

    depth_score = min(n / 18, 1.0) * 35
    continuity_score = min(best / 12, 1.0) * 30
    fit_score = min(r2, 1.0) * 20
    stability_score = max(0.0, 1 - min(cv, 1.5) / 1.5) * 15
    confidence_score = int(round(depth_score + continuity_score + fit_score + stability_score))

    if confidence_score >= 75:
        band = 'Strong'
    elif confidence_score >= 55:
        band = 'Moderate'
    elif confidence_score >= 35:
        band = 'Weak'
    else:
        band = 'Insufficient'

    return {
        'data_points': n,
        'consecutive_months': streak,
        'max_consecutive_months': best,
        'r_squared': round(r2, 3),
        'coefficient_of_variation': round(cv, 3),
        'confidence_score': confidence_score,
        'confidence_band': band,
    }


def _horizon_eligibility(profile, horizon_months):
    """Gate longer horizons based on depth and continuity."""
    data_points = profile['data_points']
    consecutive = profile['max_consecutive_months']

    if horizon_months == 1:
        return data_points >= 2, 'Need at least 2 months of history.'
    if horizon_months == 3:
        return data_points >= 6 and consecutive >= 6, 'Need 6 consecutive months for a 3-month forecast.'
    if horizon_months == 6:
        return data_points >= 12 and consecutive >= 9, 'Need 12 months of history and 9 consecutive months for a 6-month forecast.'
    if horizon_months == 12:
        return data_points >= 18 and consecutive >= 12, 'Need 18 months of history and 12 consecutive months for a 12-month forecast.'
    return False, 'Unsupported horizon.'


def _confidence_from_score(score):
    """Map the richer confidence score to the legacy High/Medium/Low labels."""
    if score >= 75:
        return 'High'
    if score >= 50:
        return 'Medium'
    return 'Low'


def _forecast_point(values, horizon_months):
    """
    Forecast using a blended damped-trend + trailing-average baseline.
    This is still lightweight, but behaves better over longer horizons
    than a raw unbounded linear extrapolation.
    """
    last = values[-1]
    trailing_avg = _mean(values[-min(3, len(values)):])
    slope, r2 = _linear_trend(values[-min(12, len(values)):])
    damp = max(0.35, 1 - 0.12 * (horizon_months - 1))
    trend_projection = max(last + (slope * horizon_months * damp), 0)
    blend_weight = 0.65 if len(values) >= 6 else 0.45
    forecast = trend_projection * blend_weight + trailing_avg * (1 - blend_weight)
    return max(forecast, 0), max(r2, 0)


def _forecast_band(forecast, values, horizon_months, confidence_score):
    recent = values[-min(6, len(values)):]
    avg_recent = _mean(recent)
    cv = (_stdev(recent) / avg_recent) if avg_recent > 0 else 0
    uncertainty = 0.12 + min(cv, 1.2) * 0.45 + (horizon_months - 1) * 0.05
    uncertainty *= max(0.6, 1.15 - (confidence_score / 100))
    uncertainty = min(max(uncertainty, 0.08), 0.9)
    low = max(forecast * (1 - uncertainty), 0)
    high = forecast * (1 + uncertainty)
    return round(low, 2), round(high, 2)


def _merge_brand_histories(brand_histories):
    """Merge obvious duplicate brand variants into canonical brand histories."""
    merged = {}
    for brand_name, history in (brand_histories or {}).items():
        canonical = canonicalize_brand_name(brand_name)
        month_map = merged.setdefault(canonical, {})
        for idx, row in enumerate(history or []):
            key = row.get('start_date') or row.get('month_label') or f'row-{idx}'
            entry = month_map.setdefault(key, {
                'month_label': row.get('month_label'),
                'start_date': row.get('start_date'),
                'year': row.get('year'),
                'month': row.get('month'),
                'total_revenue': 0.0,
            })
            entry['total_revenue'] += float(row.get('total_revenue', 0) or 0)

    result = {}
    for brand_name, month_map in merged.items():
        rows = list(month_map.values())
        rows.sort(key=lambda row: _month_index(row, 0))
        result[brand_name] = rows
    return result


def multi_horizon_revenue_forecast(history, horizons=(1, 3, 6, 12)):
    """
    Build a multi-horizon forecast set for one brand.

    Returns a dict with readiness, confidence, and per-horizon values.
    """
    profile = _history_profile(history)
    if not history:
        return {
            'forecast': 0,
            'confidence': 'Low',
            'confidence_score': 0,
            'confidence_band': 'Insufficient',
            'trend_label': 'Unknown',
            'pct_change': 0,
            'data_points': 0,
            'consecutive_months': 0,
            'growth_label': 'Insufficient Data',
            'growth_color': growth_color('Insufficient Data'),
            'eligible_horizons': [],
            'horizons': {},
        }

    revenues = [max(float(h.get('total_revenue', 0) or 0), 0) for h in history]
    last_rev = revenues[-1]
    confidence = _confidence_from_score(profile['confidence_score'])
    growth = growth_label(history)
    horizon_rows = {}
    eligible_horizons = []

    for horizon_months in horizons:
        eligible, reason = _horizon_eligibility(profile, horizon_months)
        forecast, r2 = _forecast_point(revenues, horizon_months)
        low, high = _forecast_band(forecast, revenues, horizon_months, profile['confidence_score'])
        pct_change = ((forecast - last_rev) / last_rev * 100) if last_rev > 0 else 0
        if eligible:
            eligible_horizons.append(horizon_months)
        horizon_rows[f'{horizon_months}m'] = {
            'months_ahead': horizon_months,
            'eligible': eligible,
            'reason': None if eligible else reason,
            'forecast': round(forecast, 2),
            'low': low,
            'high': high,
            'pct_change': round(pct_change, 1),
            'r_squared': round(r2, 3),
        }

    primary = horizon_rows['1m']
    return {
        'forecast': primary['forecast'],
        'confidence': confidence,
        'confidence_score': profile['confidence_score'],
        'confidence_band': profile['confidence_band'],
        'trend_label': 'Upward' if growth == 'Growing' else 'Downward' if growth == 'Declining' else 'Flat',
        'pct_change': primary['pct_change'],
        'data_points': profile['data_points'],
        'consecutive_months': profile['max_consecutive_months'],
        'growth_label': growth,
        'growth_color': growth_color(growth),
        'eligible_horizons': eligible_horizons,
        'horizons': horizon_rows,
    }


def next_month_revenue_forecast(history):
    """
    Forecast next month's revenue using linear trend.
    Enhanced to provide forecasts even with limited data.

    Args:
        history: list of dicts with keys 'total_revenue', 'month_label',
                 ordered OLDEST first.

    Returns:
        dict: {
            'forecast':     float,   — predicted revenue
            'confidence':   str,     — 'High' / 'Medium' / 'Low'
            'trend_label':  str,     — 'Upward' / 'Flat' / 'Downward'
            'pct_change':   float,   — % change vs last month
            'data_points':  int,
        }
    """
    forecast = multi_horizon_revenue_forecast(history, horizons=(1,))
    return {
        'forecast': forecast['forecast'],
        'confidence': forecast['confidence'],
        'trend_label': forecast['trend_label'],
        'pct_change': forecast['pct_change'],
        'data_points': forecast['data_points'],
    }


def stock_depletion_date(kpis, as_of_date=None):
    """
    Estimate the date stock will run out based on current cover.

    Returns:
        dict: {
            'days_remaining':   int,
            'depletion_date':   str (YYYY-MM-DD) or None,
            'urgency':          'critical' / 'warning' / 'ok' / 'unknown',
        }
    """
    cover = kpis.get('stock_days_cover', 0)
    stock = kpis.get('total_closing_stock', 0)

    if stock == 0:
        return {'days_remaining': 0, 'depletion_date': None, 'urgency': 'unknown'}

    base = as_of_date or datetime.today()
    if isinstance(base, str):
        base = datetime.strptime(base[:10], '%Y-%m-%d')

    depletion = base + timedelta(days=cover)
    urgency = (
        'critical' if cover < 7 else
        'warning'  if cover < 14 else
        'ok'
    )
    return {
        'days_remaining': int(cover),
        'depletion_date': depletion.strftime('%Y-%m-%d'),
        'urgency':        urgency,
    }


def growth_label(history):
    """
    Classify brand trajectory from history list (oldest → newest).
    Now works with single data point by comparing to portfolio average.

    Returns: 'Growing' | 'Stable' | 'Declining' | 'Insufficient Data'
    """
    if not history or len(history) < 1:
        return 'Insufficient Data'
    
    # If we have at least 2 data points, use trend analysis
    if len(history) >= 2:
        revenues = [h['total_revenue'] for h in history]
        slope, _ = _linear_trend(revenues)
        last = revenues[-1]
        pct = slope / last * 100 if last > 0 else 0
        if pct > 5:
            return 'Growing'
        elif pct < -5:
            return 'Declining'
        return 'Stable'
    
    # Single data point - use performance score if available
    if len(history) == 1 and history[0].get('perf_score'):
        score = history[0].get('perf_score', 0)
        if score >= 70:
            return 'Growing'
        elif score <= 40:
            return 'Declining'
        return 'Stable'
    
    return 'Insufficient Data'


def growth_color(label):
    return {
        'Growing':           '#1E8449',
        'Stable':            '#2E86C1',
        'Declining':         '#E8192C',
        'Insufficient Data': '#7A849E',
    }.get(label, '#7A849E')


def build_brand_forecasts(brand_histories):
    """
    Build forecast summary for all brands.

    Args:
        brand_histories: dict {brand_name: [history_rows oldest→newest]}

    Returns:
        dict {brand_name: {forecast, confidence, trend_label, pct_change, growth_label}}
    """
    result = {}
    merged_histories = _merge_brand_histories(brand_histories)
    for brand, hist in merged_histories.items():
        result[brand] = multi_horizon_revenue_forecast(hist)
    return result
