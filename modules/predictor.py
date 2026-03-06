"""
predictor.py — Predictive analytics for DALA brand partners.

Provides:
  - next_month_revenue_forecast(history)  — linear trend projection
  - stock_depletion_date(kpis)            — when stock runs out
  - reorder_prediction(brand_history)     — expected next reorder window
  - growth_label(history)                 — 'Growing', 'Stable', 'Declining'
"""

from datetime import datetime, timedelta


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


def next_month_revenue_forecast(history):
    """
    Forecast next month's revenue using linear trend.

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
    if not history:
        return {'forecast': 0, 'confidence': 'Low', 'trend_label': 'Unknown',
                'pct_change': 0, 'data_points': 0}

    revenues = [h['total_revenue'] for h in history]
    n = len(revenues)
    slope, r2 = _linear_trend(revenues)

    # Forecast = last value + slope
    last_rev = revenues[-1]
    forecast = max(last_rev + slope, 0)

    # Confidence
    if n >= 6 and r2 >= 0.7:
        confidence = 'High'
    elif n >= 3 and r2 >= 0.4:
        confidence = 'Medium'
    else:
        confidence = 'Low'

    # Trend label
    pct = (slope / last_rev * 100) if last_rev > 0 else 0
    if pct > 5:
        trend_label = 'Upward'
    elif pct < -5:
        trend_label = 'Downward'
    else:
        trend_label = 'Flat'

    pct_change = (forecast - last_rev) / last_rev * 100 if last_rev > 0 else 0

    return {
        'forecast':    round(forecast, 2),
        'confidence':  confidence,
        'trend_label': trend_label,
        'pct_change':  round(pct_change, 1),
        'data_points': n,
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

    Returns: 'Growing' | 'Stable' | 'Declining' | 'Insufficient Data'
    """
    if not history or len(history) < 2:
        return 'Insufficient Data'
    revenues = [h['total_revenue'] for h in history]
    slope, _ = _linear_trend(revenues)
    last = revenues[-1]
    pct = slope / last * 100 if last > 0 else 0
    if pct > 5:
        return 'Growing'
    elif pct < -5:
        return 'Declining'
    return 'Stable'


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
    for brand, hist in brand_histories.items():
        f = next_month_revenue_forecast(hist)
        f['growth_label'] = growth_label(hist)
        f['growth_color'] = growth_color(f['growth_label'])
        result[brand] = f
    return result
