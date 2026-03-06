"""
alerts.py — Smart Alerts Engine for DALA Analytics.

Checks conditions after each report generation and writes alerts
to the DataStore. Also provides helper for rendering alert badges.
"""

from modules.data_store import DataStore


ALERT_TYPES = {
    'revenue_drop':    'Revenue Drop',
    'low_stock':       'Low Stock',
    'no_reorders':     'No Reorders',
    'top_performer':   'Top Performer',
    'new_milestone':   'New Milestone',
    'single_sku_risk': 'Single SKU Risk',
    'no_trading':      'Inactive Brand',
}

SEVERITY_COLORS = {
    'high':   '#E8192C',
    'medium': '#C0922A',
    'low':    '#2E86C1',
    'info':   '#1E8449',
}


def check_and_save_alerts(report_id, brand_name, kpis,
                           portfolio_avg_revenue, history=None, ds=None):
    """
    Evaluate all alert conditions for one brand and persist any triggered alerts.

    Args:
        report_id:              ID of the current report in DB
        brand_name:             Brand name string
        kpis:                   KPI dict from calculate_kpis()
        portfolio_avg_revenue:  Portfolio average revenue (for benchmarking)
        history:                List of previous brand_kpis dicts (newest first, excl. current)
        ds:                     DataStore instance (created if None)
    """
    ds = ds or DataStore()
    revenue     = kpis.get('total_revenue', 0)
    stock       = kpis.get('total_closing_stock', 0)
    inv_status  = kpis.get('inv_health_status', '')
    repeat_pct  = kpis.get('repeat_pct', 0)
    num_stores  = kpis.get('num_stores', 0)
    trading_days = kpis.get('trading_days', 0)
    unique_skus  = kpis.get('unique_skus', 0)

    # ── 1. Revenue vs portfolio average ──────────────────────────────────────
    if portfolio_avg_revenue > 0:
        ratio = revenue / portfolio_avg_revenue
        if ratio >= 2.0:
            ds.save_alert(report_id, brand_name, 'top_performer', 'info',
                f"{brand_name} is performing at {ratio:.1f}x the portfolio average — "
                f"₦{revenue:,.0f} vs avg ₦{portfolio_avg_revenue:,.0f}.")
        elif ratio < 0.3:
            ds.save_alert(report_id, brand_name, 'revenue_drop', 'high',
                f"{brand_name} revenue (₦{revenue:,.0f}) is only {ratio*100:.0f}% of the "
                f"portfolio average. Immediate attention recommended.")

    # ── 2. MoM Revenue Drop (requires history) ────────────────────────────────
    if history and len(history) >= 1:
        prev_rev = history[0].get('total_revenue', 0)
        if prev_rev > 0:
            change_pct = (revenue - prev_rev) / prev_rev * 100
            if change_pct <= -25:
                ds.save_alert(report_id, brand_name, 'revenue_drop', 'high',
                    f"{brand_name} revenue dropped {abs(change_pct):.1f}% vs last month "
                    f"(₦{prev_rev:,.0f} → ₦{revenue:,.0f}).")
            elif change_pct <= -10:
                ds.save_alert(report_id, brand_name, 'revenue_drop', 'medium',
                    f"{brand_name} revenue is down {abs(change_pct):.1f}% vs last month.")

    # ── 3. Low stock / No stock ───────────────────────────────────────────────
    if inv_status == 'Low Stock':
        cover = kpis.get('stock_days_cover', 0)
        ds.save_alert(report_id, brand_name, 'low_stock', 'high',
            f"{brand_name} has only {cover:.1f} days of stock cover remaining. "
            f"Restocking urgently required.")
    elif inv_status == 'Watch':
        cover = kpis.get('stock_days_cover', 0)
        ds.save_alert(report_id, brand_name, 'low_stock', 'medium',
            f"{brand_name} stock cover is {cover:.1f} days — monitor closely.")

    # ── 4. No repeat orders ───────────────────────────────────────────────────
    if repeat_pct == 0 and num_stores >= 3:
        ds.save_alert(report_id, brand_name, 'no_reorders', 'medium',
            f"{brand_name}: None of the {num_stores} stores placed repeat orders "
            f"this period. Consider a restock push campaign.")

    # ── 5. Very low repeat rate ───────────────────────────────────────────────
    elif repeat_pct < 20 and num_stores >= 5:
        ds.save_alert(report_id, brand_name, 'no_reorders', 'low',
            f"{brand_name} repeat rate is {repeat_pct:.1f}% — "
            f"only {kpis.get('repeat_stores',0)} of {num_stores} stores reordered.")

    # ── 6. Single SKU risk ────────────────────────────────────────────────────
    if unique_skus == 1:
        ds.save_alert(report_id, brand_name, 'single_sku_risk', 'medium',
            f"{brand_name} revenue is entirely dependent on 1 SKU — "
            f"high concentration risk.")

    # ── 7. Inactive / minimal trading ────────────────────────────────────────
    if trading_days <= 3 and revenue < portfolio_avg_revenue * 0.2:
        ds.save_alert(report_id, brand_name, 'no_trading', 'high',
            f"{brand_name} traded only {trading_days} day(s) this period with very low revenue. "
            f"Check distribution agreement status.")

    # ── 8. Revenue milestone crossed (₦5M, ₦10M, ₦20M) ─────────────────────
    milestones = [5_000_000, 10_000_000, 20_000_000]
    prev_rev = history[0].get('total_revenue', 0) if history else 0
    for m in milestones:
        if revenue >= m and prev_rev < m:
            ds.save_alert(report_id, brand_name, 'new_milestone', 'info',
                f"{brand_name} crossed the ₦{m/1_000_000:.0f}M revenue milestone this month!")
            break


def run_portfolio_alerts(report_id, all_brand_kpis_db, ds=None):
    """
    Portfolio-level alerts (cross-brand).
    all_brand_kpis_db: list of dicts from get_all_brand_kpis()
    """
    ds = ds or DataStore()
    if not all_brand_kpis_db:
        return

    revenues = [b['total_revenue'] for b in all_brand_kpis_db]
    total = sum(revenues)
    avg = total / len(revenues) if revenues else 0

    # Concentration risk: top brand > 40% of portfolio
    top = max(all_brand_kpis_db, key=lambda x: x['total_revenue'])
    if total > 0 and top['total_revenue'] / total > 0.4:
        pct = top['total_revenue'] / total * 100
        ds.save_alert(report_id, None, 'single_sku_risk', 'medium',
            f"Portfolio concentration risk: {top['brand_name']} accounts for "
            f"{pct:.1f}% of all revenue (₦{top['total_revenue']:,.0f}).")

    # Count brands with low stock
    low_stock_brands = [b['brand_name'] for b in all_brand_kpis_db
                        if b.get('inv_health_status') == 'Low Stock']
    if len(low_stock_brands) >= 3:
        ds.save_alert(report_id, None, 'low_stock', 'high',
            f"{len(low_stock_brands)} brands are in low stock status: "
            f"{', '.join(low_stock_brands[:5])}.")
