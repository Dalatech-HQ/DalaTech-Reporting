"""
pdf_generator_html.py — HTML-based PDF builder using Playwright.

Generates professional 2-page PDFs from Jinja2 templates with embedded charts.
Page 1: Dashboard overview (matches Power BI layout)
Page 2: Inventory detail, performance scorecard, store heatmap
"""

import os
import base64
import calendar as _calendar
import asyncio
import glob
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.sync_api import sync_playwright

from .kpi import generate_narrative, calculate_perf_score, build_reorder_trend
from .predictor import monthly_growth_outlook
from .gmv import render_gmv_window_svg
from .charts_html import (
    chart_top_stores,
    chart_product_value,
    chart_dual_trend,
    chart_stock_vertical,
    chart_reorder,
    chart_store_heatmap,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
LOGO_PATH    = os.path.join(BASE_DIR, 'logo.jpeg')

jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml'])
)


def _daily_sparkline_svg(daily_df, col: str = 'Revenue',
                         color: str = '#E8192C') -> str:
    """Generate inline SVG area+line sparkline from daily sales data."""
    if daily_df is None or daily_df.empty or col not in daily_df.columns:
        return ''
    vals = [float(v) for v in daily_df[col].fillna(0).tolist()]
    if len(vals) == 1:
        return (
            f'<svg viewBox="0 0 90 22" xmlns="http://www.w3.org/2000/svg" '
            f'style="display:block;width:100%;height:22px;">'
            f'<circle cx="45" cy="11" r="4" fill="{color}" opacity="0.7"/>'
            f'</svg>'
        )
    if not vals:
        return ''
    W, H = 90, 22
    maxv = max(vals) or 1
    minv = min(vals)
    rangev = (maxv - minv) or 1
    n = len(vals)
    coords = []
    for i, v in enumerate(vals):
        x = round(i / (n - 1) * W, 1)
        y = round(H - 3 - ((v - minv) / rangev * (H - 8)), 1)
        coords.append((x, y))
    line_pts = ' '.join(f'{x},{y}' for x, y in coords)
    fill_pts = f'0,{H} {line_pts} {W},{H}'
    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="display:block;width:100%;height:{H}px;">'
        f'<polygon points="{fill_pts}" fill="{color}" opacity="0.15"/>'
        f'<polyline points="{line_pts}" fill="none" stroke="{color}" '
        f'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>'
        f'</svg>'
    )


def _infer_report_type(start_date: str, end_date: str, override: str | None = None) -> str:
    """Infer the report period from dates when no explicit type is supplied."""
    report_type = (override or '').strip().lower()
    if report_type in {'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly'}:
        return report_type

    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except Exception:
        return 'custom'

    days = (end_dt - start_dt).days + 1
    is_full_year = start_dt.month == 1 and start_dt.day == 1 and end_dt.month == 12 and end_dt.day == 31
    is_quarter = (
        start_dt.day == 1 and
        start_dt.month in (1, 4, 7, 10) and
        end_dt.month == start_dt.month + 2 and
        ((end_dt.month in (3, 12) and end_dt.day == 31) or
         (end_dt.month == 6 and end_dt.day == 30) or
         (end_dt.month == 9 and end_dt.day == 30))
    )
    next_day = end_dt.toordinal() + 1
    next_dt = datetime.fromordinal(next_day)
    is_full_month = start_dt.day == 1 and next_dt.day == 1

    if is_full_year or days in (365, 366):
        return 'yearly'
    if is_quarter or 85 <= days <= 95:
        return 'quarterly'
    if is_full_month or 28 <= days <= 31:
        return 'monthly'
    if days <= 7:
        return 'weekly'
    if days <= 14:
        return 'biweekly'
    return 'custom'


def _build_narrative_sections(brand_name: str, kpis: dict,
                               start_date: str, end_date: str,
                               report_type: str | None = None,
                               month_label: str | None = None) -> dict:
    """Build structured narrative bullet sections matching the ORISIRISI PDF format."""
    start_dt   = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt     = datetime.strptime(end_date,   '%Y-%m-%d')
    month_name = start_dt.strftime('%B')
    report_days = (end_dt - start_dt).days + 1

    report_type = _infer_report_type(start_date, end_date, report_type)
    is_closed_period = report_type in ('monthly', 'quarterly', 'yearly')

    next_month_num  = end_dt.month % 12 + 1
    next_month_name = _calendar.month_name[next_month_num]
    next_year = start_dt.year + 1
    month_end_day = _calendar.monthrange(end_dt.year, end_dt.month)[1]
    remaining_days_in_month = max(0, month_end_day - end_dt.day)
    same_calendar_month = start_dt.year == end_dt.year and start_dt.month == end_dt.month

    if report_type == 'weekly':
        title = f'Week of {start_dt.strftime("%d %b %Y")} Sales Report For {brand_name}'
        period_label = f'Week of {start_dt.strftime("%d %b %Y")} KPIs'
        period_desc = f'week of {start_dt.strftime("%d %b %Y")}'
        next_period_label = 'Recommendations'
    elif report_type == 'biweekly':
        range_label = f'{start_dt.strftime("%d %b")} - {end_dt.strftime("%d %b %Y")}'
        title = f'Biweekly Sales Report For {brand_name}'
        period_label = f'{range_label} KPIs'
        period_desc = f'{report_days}-day period'
        next_period_label = 'Recommendations'
    elif report_type == 'quarterly':
        quarter = ((start_dt.month - 1) // 3) + 1
        title = f'Q{quarter} {start_dt.year} Quarterly Sales Report For {brand_name}'
        period_label = f'Q{quarter} {start_dt.year} KPIs'
        period_desc = f'quarter of Q{quarter} {start_dt.year}'
        next_period_label = 'Recommendations'
    elif report_type == 'yearly':
        title = f'{start_dt.year} Annual Sales Report For {brand_name}'
        period_label = f'{start_dt.year} KPIs'
        period_desc = f'year {start_dt.year}'
        next_period_label = 'Recommendations'
    elif report_type == 'monthly':
        title = f'{month_name} Monthly Sales Report For {brand_name}'
        period_label = f'{month_name} KPIs'
        period_desc = 'month'
        next_period_label = 'Recommendations'
    else:
        label = month_label or f'{start_dt.strftime("%d %b %Y")} - {end_dt.strftime("%d %b %Y")}'
        title = f'Sales Report For {brand_name}'
        period_label = f'{label} KPIs'
        period_desc = f'{report_days}-day period'
        next_period_label = 'Recommendations'

    def _full(v):
        return f'\u20a6{v:,.2f}'

    def _short(v):
        if v >= 1_000_000: return f'\u20a6{v/1_000_000:,.2f}M'
        if v >= 1_000:     return f'\u20a6{v/1_000:,.1f}K'
        return f'\u20a6{v:,.0f}'

    total_rev   = float(kpis.get('total_revenue') or 0)
    total_qty   = float(kpis.get('total_qty') or 0)
    # Support both key names used in different code paths
    store_count = int(kpis.get('num_stores') or kpis.get('store_count') or 0)
    repeat_pct  = float(kpis.get('repeat_pct') or 0)
    wow_rev     = float(kpis.get('wow_rev_change') or 0)
    stock_days  = float(kpis.get('stock_days_cover') or 0)
    trading_days = int(kpis.get('trading_days') or report_days)

    weekly_rev_pct = kpis.get('weekly_rev_pct') or [0, 0, 0, 0]

    top_stores_df = kpis.get('top_stores')
    top_store_name, top_store_rev = '', 0.0
    if top_stores_df is not None and not top_stores_df.empty:
        top_stores_df = top_stores_df.sort_values('Revenue', ascending=False)
        top_store_name = top_stores_df.iloc[0]['Store']
        top_store_rev  = float(top_stores_df.iloc[0]['Revenue'])

    product_df = kpis.get('product_value')
    top_product_name, top_product_rev = '', 0.0
    bottom_product_name, bottom_product_rev = '', 0.0
    if product_df is not None and not product_df.empty:
        product_df = product_df.sort_values('Revenue', ascending=False)
        top_product_name = product_df.iloc[0]['SKU']
        top_product_rev  = float(product_df.iloc[0]['Revenue'])
        if len(product_df) > 1:
            bottom_product_name = product_df.iloc[-1]['SKU']
            bottom_product_rev  = float(product_df.iloc[-1]['Revenue'])

    closing_stock_df = kpis.get('closing_stock')

    # ── KPI bullets ───────────────────────────────────────────────────────────
    kpi_bullets = [
        f'Total Sales Revenue: {_full(total_rev)} ({period_desc}).',
        f'Total Quantity Sold: {total_qty:,.2f} packs.',
        f'Sales Reach: {store_count} active supermarket{"s" if store_count != 1 else ""}.',
    ]
    if top_product_name:
        vol_desc = 'high' if total_qty > 100 else ('moderate' if total_qty > 20 else 'early-stage')
        kpi_bullets.append(
            f'Customer Response: {top_product_name} is the primary revenue driver at '
            f'{_short(top_product_rev)}, indicating {brand_name} is a {vol_desc}-volume brand.'
        )

    # ── Strengths ─────────────────────────────────────────────────────────────
    strengths = []

    if is_closed_period:
        nonzero = [v for v in weekly_rev_pct if v > 0]
        if len(nonzero) >= 2 and nonzero[-1] > nonzero[0]:
            strengths.append(
                f'Consistent Growth Trend: {brand_name} maintained a steady upward trajectory '
                f'across the reporting period, with WoW revenue share growing from {nonzero[0]:.0f}% to {nonzero[-1]:.0f}%.'
            )
        elif wow_rev > 0:
            strengths.append(
                f'Positive Momentum: Revenue grew {wow_rev:+.1f}% week-over-week in the final stretch, '
                f'demonstrating strong closing momentum for {brand_name}.'
            )
    else:
        # Open period: focus on current pacing
        if total_rev > 0 and trading_days > 0:
            daily_avg = total_rev / trading_days
            projected = daily_avg * 28
            strengths.append(
                f'Current Momentum: {brand_name} generated {_full(total_rev)} in this '
                f'{report_days}-day window, implying a {_short(projected)} '
                f'monthly run rate if this pace holds.'
            )

    if top_product_name and top_product_rev > 0 and total_rev > 0:
        pct = top_product_rev / total_rev * 100
        strengths.append(
            f'Product Market Leader: {top_product_name} accounts for {pct:.0f}% of revenue '
            f'at {_short(top_product_rev)}.'
        )

    if top_store_name and top_store_rev > 0:
        strengths.append(
            f'Retail Anchor: {top_store_name} is the primary distribution point, '
            f'contributing {_short(top_store_rev)}.'
        )

    if is_closed_period and repeat_pct >= 50:
        strengths.append(
            f'Strong Loyalty: {repeat_pct:.0f}% repeat order rate demonstrates solid customer retention.'
        )

    if not strengths:
        strengths.append(
            f'{brand_name} registered sales activity during the reporting period. '
            f'More data is needed to identify clear trend patterns.'
        )

    # ── Gaps ──────────────────────────────────────────────────────────────────
    gaps = []

    if store_count < 5 and not is_closed_period:
        gaps.append(
            f'Limited Distribution: Only {store_count} store{"s" if store_count != 1 else ""} '
            f'active in this {report_days}-day window. '
            f'Significantly more outlets need to be activated to build meaningful sales volume.'
        )
    elif store_count < 10 and is_closed_period:
        gaps.append(
            f'Narrow Distribution Base: {store_count} active stores is below the threshold for '
            f'sustainable revenue. Expanding reach is the highest-priority growth lever.'
        )

    if (bottom_product_name and bottom_product_rev > 0
            and top_product_rev > 0 and bottom_product_rev < top_product_rev * 0.3):
        gaps.append(
            f'SKU Revenue Gap: {bottom_product_name} ({_short(bottom_product_rev)}) contributes '
            f'significantly less than the lead SKU, {top_product_name} ({_short(top_product_rev)}). '
            f'Review placement and pricing of underperforming variants.'
        )

    if closing_stock_df is not None and not closing_stock_df.empty:
        low = closing_stock_df[closing_stock_df['Closing Stock (Cartons)'] < 10]
        if not low.empty:
            sku_names = ' and '.join(low['SKU'].tolist()[:2])
            gaps.append(
                f'Low Inventory: {sku_names} {"have" if " and " in sku_names else "has"} fewer '
                f'than 10 packs in stock, which may restrict sales if demand picks up.'
            )

    if 0 < stock_days < 14 and is_closed_period:
        gaps.append(
            f'Stock Cover Risk: At {stock_days:.0f} days of remaining cover, inventory replenishment '
            f'must be initiated immediately to avoid stockouts next month.'
        )

    if is_closed_period and repeat_pct < 40 and store_count > 0:
        gaps.append(
            f'Low Repeat Rate: {repeat_pct:.0f}% repeat ordering is below the 40% benchmark, '
            f'suggesting inconsistent stock availability or weak demand at current outlets.'
        )

    if not gaps:
        gaps.append(
            'No critical performance gaps identified for this period. '
            'Continue monitoring inventory levels and distribution activity.'
        )

    # ── Recommendations ───────────────────────────────────────────────────────
    recs = []

    if is_closed_period:
        rec_label = next_period_label

        if closing_stock_df is not None and not closing_stock_df.empty:
            low = closing_stock_df[closing_stock_df['Closing Stock (Cartons)'] < 10]
            for _, row in low.head(2).iterrows():
                needed = max(10, int(row['Closing Stock (Cartons)'] or 0) * 4)
                next_label = next_month_name if report_type == 'monthly' else (
                    f'Q{((start_dt.month - 1) // 3) + 2}' if report_type == 'quarterly' and ((start_dt.month - 1) // 3) < 3
                    else (f'Q1 {next_year}' if report_type == 'quarterly' else str(next_year))
                )
                recs.append(
                    f'Stock Optimization: Deliver at least {needed} packs of {row["SKU"]} to the '
                    f'warehouse before {next_label} begins to meet anticipated demand.'
                )

        # Underperformer promotion
        if (bottom_product_name and top_product_rev > 0
                and bottom_product_rev < top_product_rev * 0.3):
            recs.append(
                f'Variant Promotion: Develop a bundle or discount offer for {bottom_product_name} '
                f'in {next_month_name} to lift its revenue contribution and inventory turnover.'
            )

        # Store expansion
        if store_count < 30:
            next_label = next_month_name if report_type == 'monthly' else (
                f'Q{((start_dt.month - 1) // 3) + 2}' if report_type == 'quarterly' and ((start_dt.month - 1) // 3) < 3
                else (f'Q1 {next_year}' if report_type == 'quarterly' else str(next_year))
            )
            recs.append(
                f'Distribution Expansion: Target at least {store_count + 8} active stores in '
                f'{next_label} to reduce single-store revenue concentration risk.'
            )

        # Retention
        if repeat_pct < 40:
            next_label = next_month_name if report_type == 'monthly' else (
                f'Q{((start_dt.month - 1) // 3) + 2}' if report_type == 'quarterly' and ((start_dt.month - 1) // 3) < 3
                else (f'Q1 {next_year}' if report_type == 'quarterly' else str(next_year))
            )
            recs.append(
                f'Retention Drive: Re-engage the {store_count} current stores with targeted '
                f'incentives to push the repeat order rate above 50% in {next_label}.'
            )

    else:
        rec_label = next_period_label
        if report_type == 'weekly':
            period_run_label = 'this weekly run'
            purchaser_label = 'this week'
            pacing_prefix = f'At this weekly pace, {month_name} would close around'
        elif report_type == 'biweekly':
            period_run_label = 'this biweekly run'
            purchaser_label = 'this reporting window'
            pacing_prefix = f'At this current biweekly pace, {month_name} would close around'
        else:
            period_run_label = 'this reporting window'
            purchaser_label = 'this period'
            pacing_prefix = f'At this current run rate, {month_name} would close around'

        if same_calendar_month and remaining_days_in_month > 0:
            activation_window = f'through the rest of {month_name}'
        elif same_calendar_month:
            activation_window = f'before {month_name} closes'
        else:
            activation_window = 'through the rest of the current cycle'

        # Distribution push is almost always the top priority early in the month
        if store_count < 15:
            target = max(store_count * 3, store_count + 10)
            recs.append(
                f'Distribution Drive: With only {store_count} store{"s" if store_count != 1 else ""} '
                f'active in {period_run_label}, aggressively target at least {target} stores '
                f'{activation_window}.'
            )

        # Urgent stock replenishment (more critical for early-month since sales will ramp)
        if closing_stock_df is not None and not closing_stock_df.empty:
            low = closing_stock_df[closing_stock_df['Closing Stock (Cartons)'] < 10]
            for _, row in low.head(2).iterrows():
                needed = max(15, int(row['Closing Stock (Cartons)'] or 0) * 5)
                recs.append(
                    f'Immediate Restock: {row["SKU"]} has fewer than 10 packs remaining. '
                    f'Replenish with at least {needed} packs now — demand is likely to grow '
                    f'as more stores activate this month.'
                )

        # Re-engage early stores for repeat orders
        if top_store_name:
            recs.append(
                f'Account Development: Follow up with {top_store_name} and other purchasers from '
                f'{purchaser_label} to secure repeat orders and gather intel on in-store demand.'
            )

        # Pacing awareness with projected monthly revenue
        if total_rev > 0 and trading_days > 0:
            daily_avg   = total_rev / trading_days
            projected   = daily_avg * 28
            recs.append(
                f'Monthly Pacing: {pacing_prefix} {_short(projected)} if current revenue '
                f'({_full(total_rev)}) holds. Accelerate store activation to close any '
                f'gap against targets before month-end.'
            )

    if not recs:
        recs.append(
            f'Build on current momentum by expanding distribution and ensuring adequate '
            f'stock levels throughout the remainder of {month_name}.'
        )

    return {
        'report_title':     title,
        'period_label':     period_label,
        'next_month_label': rec_label,
        'kpi_bullets':      kpi_bullets,
        'strengths':        strengths,
        'gaps':             gaps,
        'recommendations':  recs,
        'is_full_month':    is_closed_period,
        'report_days':      report_days,
        'report_type':      report_type,
    }


def render_pdf_report_html(brand_name: str, kpis: dict,
                           start_date: str, end_date: str,
                           portfolio_avg_revenue: float = None,
                           total_portfolio_revenue: float = None,
                           report_type: str | None = None,
                           month_label: str | None = None,
                           ai_narrative: str = None,
                           sheets_url: str = None,
                           growth_outlook: dict | None = None,
                           gmv_window: dict | None = None,
                           coach: dict | None = None) -> str:
    """Render the print-oriented report HTML used for PDF export."""
    # ── Charts (matplotlib → base64) ──────────────────────────────────────────
    # Use _for_print=True so charts are generated at print-optimised sizes
    # with fonts scaled to be legible at the PDF snapshot display size.
    dual_trend_chart   = chart_dual_trend(kpis['daily_sales'], _for_print=True)
    stock_chart        = chart_stock_vertical(kpis['closing_stock'], _for_print=True)
    reorder_chart      = chart_reorder(kpis['reorder_analysis'])
    heatmap_chart      = chart_store_heatmap(kpis['store_heatmap_df'])
    top_stores_chart   = chart_top_stores(
        kpis['top_stores'],
        _for_print=True,
        total_store_count=int(kpis.get('num_stores') or kpis.get('store_count') or 0),
        total_revenue=float(kpis.get('total_revenue') or 0),
    )
    top_products_chart = chart_product_value(kpis['product_value'], _for_print=True)

    # ── Performance scorecard ──────────────────────────────────────────────────
    perf = calculate_perf_score(kpis, portfolio_avg_revenue)
    growth_outlook = growth_outlook or monthly_growth_outlook([])
    gmv_chart_svg = render_gmv_window_svg(gmv_window) if gmv_window else ''

    # ── Portfolio share ────────────────────────────────────────────────────────
    portfolio_share = None
    if total_portfolio_revenue and total_portfolio_revenue > 0:
        portfolio_share = round(kpis['total_revenue'] / total_portfolio_revenue * 100, 1)

    # ── Table data ─────────────────────────────────────────────────────────────
    def _naira_k(v):
        if v >= 1_000_000: return f'₦{v/1_000_000:.1f}M'
        if v >= 1_000:     return f'₦{v/1_000:.1f}K'
        return f'₦{v:,.0f}'

    total_rev = kpis['total_revenue'] or 1

    top_stores_sorted = kpis['top_stores'].sort_values('Revenue', ascending=False) if not kpis['top_stores'].empty else kpis['top_stores']
    product_value_sorted = kpis['product_value'].sort_values('Revenue', ascending=False) if not kpis['product_value'].empty else kpis['product_value']

    top_stores_table = [
        {
            'store': r['Store'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / total_rev * 100, 1),
        }
        for _, r in top_stores_sorted.head(5).iterrows()
    ]

    top_products_table = [
        {
            'sku':   r['SKU'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / total_rev * 100, 1),
        }
        for _, r in product_value_sorted.head(5).iterrows()
    ]

    store_summary = None
    if top_stores_sorted is not None and not top_stores_sorted.empty:
        top5_revenue = float(top_stores_sorted.head(5)['Revenue'].sum())
        total_active_stores = int(kpis.get('num_stores') or kpis.get('store_count') or len(top_stores_sorted))
        other_revenue = max(0.0, float(total_rev) - top5_revenue)
        store_rows = []
        for rank, (_, row) in enumerate(top_stores_sorted.head(10).iterrows(), start=1):
            revenue = float(row['Revenue'])
            store_rows.append({
                'rank': rank,
                'store': row['Store'],
                'value': _naira_k(revenue),
                'share': round(revenue / total_rev * 100, 1),
            })
        store_summary = {
            'total_active': total_active_stores,
            'top5_share': round(top5_revenue / total_rev * 100, 1) if total_rev > 0 else 0,
            'other_count': max(0, int(total_active_stores - 5)),
            'other_revenue': _naira_k(other_revenue),
            'left_rows': store_rows[:5],
            'right_rows': store_rows[5:10],
        }

    closing_stock_table = [
        {
            'sku':   r['SKU'],
            'qty':   f"{r['Closing Stock (Cartons)']:.0f}",
            'color': kpis['inv_health_color'],
        }
        for _, r in kpis['closing_stock'].iterrows()
    ]

    pickup_table = [
        {'sku': r['SKU'], 'qty': f"{r['Qty Picked Up']:.0f}", 'value': _naira_k(r['Value'])}
        for _, r in kpis['pickup_summary'].iterrows()
    ]

    supply_table = [
        {'sku': r['SKU'], 'qty': f"{r['Qty Supplied']:.0f}", 'value': _naira_k(r['Value'])}
        for _, r in kpis['supply_summary'].iterrows()
    ]

    # ── Structured narrative sections ──────────────────────────────────────────
    narrative_sections = _build_narrative_sections(
        brand_name,
        kpis,
        start_date,
        end_date,
        report_type=report_type,
        month_label=month_label,
    )

    # ── Dates ──────────────────────────────────────────────────────────────────
    start_dt      = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt        = datetime.strptime(end_date,   '%Y-%m-%d')
    display_start = start_dt.strftime('%d %b %Y')
    display_end   = end_dt.strftime('%d %b %Y')
    report_period_label = month_label or narrative_sections['period_label'].replace(' KPIs', '')

    # ── Logo ───────────────────────────────────────────────────────────────────
    logo_data = ''
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, 'rb') as f:
            logo_data = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"

    # ── Pre-computed display values ─────────────────────────────────────────────
    revenue_display = f'\u20a6{kpis["total_revenue"]:,.2f}'
    qty_display     = f'{kpis["total_qty"]:,.2f}'
    num_stores      = int(kpis.get('num_stores') or kpis.get('store_count') or 0)
    reorder_trend   = kpis.get('reorder_trend') or build_reorder_trend(kpis=kpis)

    # Total closing stock — prefer direct scalar key, fall back to summing the DataFrame
    total_stock_val = float(
        kpis.get('total_closing_stock') or kpis.get('closing_stock_total') or 0
    )
    if total_stock_val == 0:
        cs = kpis.get('closing_stock')
        if cs is not None and not cs.empty:
            total_stock_val = float(cs['Closing Stock (Cartons)'].sum())
    inventory_total = f'{total_stock_val:.1f}'

    # Daily sparkline SVGs (area+line, adapts to any report length)
    daily_df = kpis.get('daily_sales')
    rev_sparkline_svg = _daily_sparkline_svg(daily_df, col='Revenue',  color='#E8192C')
    qty_sparkline_svg = _daily_sparkline_svg(daily_df, col='Quantity', color='#1B2B5E')

    # ── Render template ────────────────────────────────────────────────────────
    template = jinja_env.get_template('report_template.html')
    return template.render(
        brand_name=brand_name,
        start_date=display_start,
        end_date=display_end,
        month_label=report_period_label,
        kpis=kpis,
        sheets_url=sheets_url,
        logo_path=logo_data,
        perf=perf,
        portfolio_share=portfolio_share,
        narrative_sections=narrative_sections,
        revenue_display=revenue_display,
        qty_display=qty_display,
        inventory_total=inventory_total,
        num_stores=num_stores,
        rev_sparkline_svg=rev_sparkline_svg,
        qty_sparkline_svg=qty_sparkline_svg,
        dual_trend_chart=dual_trend_chart,
        stock_chart=stock_chart,
        reorder_chart=reorder_chart,
        heatmap_chart=heatmap_chart,
        top_stores_chart=top_stores_chart,
        top_products_chart=top_products_chart,
        top_stores_table=top_stores_table,
        top_products_table=top_products_table,
        store_summary=store_summary,
        closing_stock_table=closing_stock_table,
        pickup_table=pickup_table,
        supply_table=supply_table,
        reorder_trend=reorder_trend,
        growth_outlook=growth_outlook,
        gmv_window=gmv_window,
        gmv_chart_svg=gmv_chart_svg,
        coach=coach or {},
    )


INTERACTIVE_PDF_PRINT_OVERRIDES = """
<style id="interactive-pdf-print-overrides">
@page { size: A4; margin: 10mm; }
@media print {
  html, body { background: #ffffff !important; }
  body { font-size: 11px !important; min-height: auto !important; }
  .header {
    position: static !important;
    top: auto !important;
    box-shadow: none !important;
    padding: 12px 16px !important;
  }
  .nav-tabs { display: none !important; }
  .filter-bar { padding: 8px 16px !important; }
  .section { display: block !important; }
  #section-overview {
    page-break-after: always;
    break-after: page;
  }
  .page-body {
    max-width: none !important;
    padding: 12px 14px !important;
  }
  .kpi-row {
    display: grid !important;
    grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
    gap: 12px !important;
  }
  .main-grid {
    display: grid !important;
    grid-template-columns: minmax(0, 1fr) 260px !important;
    gap: 12px !important;
  }
  .two-col,
  .detail-grid {
    display: grid !important;
    grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
    gap: 12px !important;
  }
  .kpi-card,
  .card,
  .scorecard,
  .trend-section,
  .heatmap-section {
    box-shadow: none !important;
    break-inside: avoid !important;
    page-break-inside: avoid !important;
  }
  .plotly-graph-div,
  .js-plotly-plot {
    break-inside: avoid !important;
    page-break-inside: avoid !important;
  }
  .card,
  .scorecard { padding: 12px 14px !important; }
  .header-center h1 { font-size: 18px !important; }
  .header-center .sub { font-size: 11px !important; }
}
</style>
"""


def prepare_interactive_html_for_pdf(html_content: str) -> str:
    """Inject print-only layout rules into the interactive HTML before PDF export."""
    if 'interactive-pdf-print-overrides' in html_content:
        return html_content
    if '</head>' in html_content:
        return html_content.replace('</head>', f'{INTERACTIVE_PDF_PRINT_OVERRIDES}\n</head>', 1)
    return INTERACTIVE_PDF_PRINT_OVERRIDES + html_content


# ── Persistent Chromium browser (launched once, reused for every PDF request) ──
import threading as _threading

_browser_lock = _threading.Lock()   # serialise page rendering across threads
_pw_instance  = None                # playwright handle
_browser_inst = None                # chromium browser handle


def _candidate_paths():
    """Return ordered list of Chromium executable candidates to try."""
    shell_candidates = []
    if os.name != 'nt':
        try:
            probe = subprocess.run(
                [
                    '/bin/sh', '-lc',
                    "command -v chromium || command -v chromium-browser || "
                    "command -v google-chrome || command -v .chromium-wrapped || "
                    "find /nix/store -type f \\( -name chromium -o -name chromium-browser "
                    "-o -name google-chrome -o -name .chromium-wrapped \\) 2>/dev/null | head -n 20"
                ],
                capture_output=True, text=True, timeout=15, check=False,
            )
            shell_candidates = [l.strip() for l in probe.stdout.splitlines() if l.strip()]
        except Exception:
            pass

    nix_candidates = []
    for pattern in (
        '/nix/store/*/bin/chromium',
        '/nix/store/*/bin/chromium-browser',
        '/nix/store/*/bin/google-chrome',
        '/nix/store/*/bin/.chromium-wrapped',
        '/nix/store/*chromium*/bin/chromium',
        '/nix/store/*chromium*/bin/chromium-browser',
        '/nix/store/*chromium*/bin/.chromium-wrapped',
        '/nix/var/nix/profiles/default/bin/chromium',
        '/etc/profiles/per-user/root/bin/chromium',
        '/run/current-system/sw/bin/chromium',
    ):
        nix_candidates.extend(glob.glob(pattern))

    raw = [
        os.getenv('PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'),
        shutil.which('chromium'),
        shutil.which('chromium-browser'),
        shutil.which('google-chrome'),
        shutil.which('.chromium-wrapped'),
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        *shell_candidates,
        *nix_candidates,
    ]
    seen, ordered = set(), []
    for c in raw:
        if c and c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered


def _run_playwright_install():
    """Install Chromium binary + system dependencies (libglib etc.)."""
    install_env = os.environ.copy()
    bp = os.getenv('PLAYWRIGHT_BROWSERS_PATH')
    if bp:
        install_env['PLAYWRIGHT_BROWSERS_PATH'] = bp
        os.makedirs(bp, exist_ok=True)
    # Install the browser binary
    subprocess.run(
        [sys.executable, '-m', 'playwright', 'install', 'chromium'],
        env=install_env, timeout=180, check=False,
    )
    # Install system deps (libglib-2.0, libnss, etc.) — fixes Railway Linux errors
    subprocess.run(
        [sys.executable, '-m', 'playwright', 'install-deps', 'chromium'],
        env=install_env, timeout=120, check=False,
    )


def _launch_chromium(p):
    """Launch Chromium via playwright handle `p`, trying every fallback path."""
    opts = {'headless': True, 'args': ['--no-sandbox', '--disable-dev-shm-usage']}

    def _try(executable_path=None):
        o = dict(opts)
        if executable_path:
            o['executable_path'] = executable_path
        return p.chromium.launch(**o)

    errors = []

    # 1. Default Playwright-managed browser
    try:
        return _try()
    except Exception as e:
        errors.append(f'default: {e}')

    # 2. Known system paths
    for path in _candidate_paths():
        if path and os.path.exists(path):
            try:
                return _try(executable_path=path)
            except Exception as e:
                errors.append(f'{path}: {e}')

    # 3. Auto-install browser + system deps, then retry
    # Triggered on ANY launch failure — covers missing binary AND missing libglib etc.
    _run_playwright_install()
    try:
        return _try()
    except Exception as e:
        errors.append(f'post-install default: {e}')
    for path in _candidate_paths():
        if path and os.path.exists(path):
            try:
                return _try(executable_path=path)
            except Exception as e:
                errors.append(f'post-install {path}: {e}')

    raise RuntimeError(f'Unable to launch Chromium. Errors: {" | ".join(errors[:6])}')


def _ensure_browser():
    """Return the live persistent Chromium browser, (re)launching if needed."""
    global _pw_instance, _browser_inst
    if _browser_inst is not None:
        try:
            _ = _browser_inst.contexts   # lightweight health check
            return _browser_inst
        except Exception:
            for obj in (_browser_inst, _pw_instance):
                try:
                    obj.close() if hasattr(obj, 'close') else obj.stop()
                except Exception:
                    pass
            _browser_inst = None
            _pw_instance = None

    _pw_instance = sync_playwright().start()
    _browser_inst = _launch_chromium(_pw_instance)
    return _browser_inst


def _system_browser_pdf_executable():
    explicit = [
        os.getenv('PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'),
        os.path.join(os.environ.get('ProgramFiles(x86)', ''), 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
        os.path.join(os.environ.get('ProgramFiles', ''), 'Microsoft', 'Edge', 'Application', 'msedge.exe'),
        os.path.join(os.environ.get('ProgramFiles', ''), 'Google', 'Chrome', 'Application', 'chrome.exe'),
        os.path.join(os.environ.get('ProgramFiles(x86)', ''), 'Google', 'Chrome', 'Application', 'chrome.exe'),
        shutil.which('msedge'),
        shutil.which('chrome'),
    ]
    for path in [*explicit, *_candidate_paths()]:
        if path and os.path.exists(path):
            return path
    return None


def render_pdf_bytes(html_content: str, page_size: dict | None = None) -> bytes:
    """Render PDF bytes, avoiding Playwright sync API calls inside an active asyncio loop.
    
    Args:
        html_content: The HTML content to render.
        page_size: Optional dict with Playwright page.pdf() options (e.g. {'width':'297mm','height':'420mm'}).
                   If omitted, defaults to A4.
    """
    default_size = {'format': 'A4', 'margin': {'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'}}
    size_opts = page_size or default_size

    try:
        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            try:
                return _render_pdf_bytes_oneoff(html_content, page_size=size_opts)
            except Exception as exc:
                if 'WinError 225' in str(exc) or 'potentially unwanted software' in str(exc):
                    return _render_pdf_bytes_system_browser(html_content, page_size=size_opts)
                raise
    except RuntimeError:
        pass

    try:
        return _render_pdf_bytes_persistent(html_content, page_size=size_opts)
    except Exception as exc:
        message = str(exc)
        if 'Playwright Sync API inside the asyncio loop' in message:
            try:
                return _render_pdf_bytes_oneoff(html_content, page_size=size_opts)
            except Exception as inner_exc:
                if 'WinError 225' in str(inner_exc) or 'potentially unwanted software' in str(inner_exc):
                    return _render_pdf_bytes_system_browser(html_content, page_size=size_opts)
                raise
        if 'WinError 225' in message or 'potentially unwanted software' in message:
            return _render_pdf_bytes_system_browser(html_content, page_size=size_opts)
        raise


def _render_pdf_bytes_persistent(html_content: str, page_size: dict | None = None) -> bytes:
    """Render PDF bytes reusing a persistent Chromium instance (no cold-start per call)."""
    global _browser_inst, _pw_instance
    size_opts = page_size or {'format': 'A4', 'margin': {'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'}}
    with _browser_lock:
        for attempt in range(2):
            try:
                browser = _ensure_browser()
                page = browser.new_page()
                try:
                    # 'load' fires once DOM + sync scripts finish — no network-idle delay
                    page.set_content(html_content, wait_until='load')
                    # Wait for web fonts (e.g. Inter) to finish loading so ₦ renders correctly
                    try:
                        page.evaluate("document.fonts.ready")
                    except Exception:
                        pass
                    try:
                        page.wait_for_function("window.__REPORT_READY === true", timeout=5000)
                    except Exception:
                        pass
                    pdf_bytes = page.pdf(print_background=True, **size_opts)
                finally:
                    page.close()
                return pdf_bytes
            except Exception:
                if attempt == 0:
                    # Reset and retry once on first failure
                    for obj in (_browser_inst, _pw_instance):
                        try:
                            obj.close() if hasattr(obj, 'close') else obj.stop()
                        except Exception:
                            pass
                    _browser_inst = None
                    _pw_instance = None
                else:
                    raise


def _render_pdf_bytes_oneoff(html_content: str, page_size: dict | None = None) -> bytes:
    """Render PDF bytes in a fresh worker context, safe for request threads with active event loops."""
    result: dict[str, bytes] = {}
    error: dict[str, Exception] = {}
    size_opts = page_size or {'format': 'A4', 'margin': {'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'}}

    def _worker():
        try:
            with sync_playwright() as pw:
                browser = _launch_chromium(pw)
                page = browser.new_page()
                try:
                    page.set_content(html_content, wait_until='load')
                    try:
                        page.evaluate("document.fonts.ready")
                    except Exception:
                        pass
                    try:
                        page.wait_for_function("window.__REPORT_READY === true", timeout=5000)
                    except Exception:
                        pass
                    result['bytes'] = page.pdf(print_background=True, **size_opts)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
                try:
                    browser.close()
                except Exception:
                    pass
        except Exception as exc:
            error['exc'] = exc

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    thread.join()
    if error.get('exc'):
        raise error['exc']
    return result['bytes']


def _render_pdf_bytes_system_browser(html_content: str, page_size: dict | None = None) -> bytes:
    """Render PDF bytes using an installed system browser when Playwright is unavailable."""
    executable = _system_browser_pdf_executable()
    if not executable:
        raise RuntimeError('No system browser executable available for PDF fallback')

    tmp_dir = tempfile.mkdtemp(prefix='dala-pdf-')
    html_path = os.path.join(tmp_dir, 'report.html')
    pdf_path = os.path.join(tmp_dir, 'report.pdf')
    try:
        with open(html_path, 'w', encoding='utf-8') as fh:
            fh.write(html_content)

        cmd = [
            executable,
            '--headless=new',
            '--disable-gpu',
            '--allow-file-access-from-files',
            '--run-all-compositor-stages-before-draw',
            '--virtual-time-budget=8000',
            f'--print-to-pdf={pdf_path}',
            Path(html_path).resolve().as_uri(),
        ]
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
            check=False,
        )
        if proc.returncode not in (0,):
            stderr = (proc.stderr or b'').decode('utf-8', errors='ignore').strip()
            stdout = (proc.stdout or b'').decode('utf-8', errors='ignore').strip()
            raise RuntimeError(f'System browser PDF render failed ({proc.returncode}): {stderr or stdout or "unknown error"}')

        for _ in range(30):
            if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                with open(pdf_path, 'rb') as fh:
                    return fh.read()
            time.sleep(0.25)
        raise RuntimeError('System browser PDF render did not produce an output file')
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def generate_pdf_html(output_path: str, brand_name: str, kpis: dict,
                      start_date: str, end_date: str,
                      portfolio_avg_revenue: float = None,
                      total_portfolio_revenue: float = None,
                      report_type: str | None = None,
                      month_label: str | None = None,
                      ai_narrative: str = None,
                      sheets_url: str = None,
                      growth_outlook: dict | None = None,
                      gmv_window: dict | None = None,
                      coach: dict | None = None) -> str:
    """
    Generate a 2-page PDF using HTML template + Playwright.

    Args:
        output_path:             Absolute path for the output PDF
        brand_name:              Brand partner display name
        kpis:                    Dict from calculate_kpis()
        start_date / end_date:   'YYYY-MM-DD'
        portfolio_avg_revenue:   Optional — avg revenue across all brands
        total_portfolio_revenue: Optional — total revenue all brands
        ai_narrative:            Optional — Gemini AI narrative text (replaces rule-based)
        sheets_url:              Optional — Google Sheets shareable URL to embed

    Returns:
        output_path
    """
    html_content = render_pdf_report_html(
        brand_name=brand_name,
        kpis=kpis,
        start_date=start_date,
        end_date=end_date,
        portfolio_avg_revenue=portfolio_avg_revenue,
        total_portfolio_revenue=total_portfolio_revenue,
        report_type=report_type,
        month_label=month_label,
        ai_narrative=ai_narrative,
        sheets_url=sheets_url,
        growth_outlook=growth_outlook,
        gmv_window=gmv_window,
        coach=coach,
    )

    # ── Render to PDF via Playwright ───────────────────────────────────────────
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Also save HTML version as fallback
    html_output_path = output_path.replace('/pdf/', '/html/').replace('.pdf', '.html')
    os.makedirs(os.path.dirname(html_output_path), exist_ok=True)
    with open(html_output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    # Try to generate PDF with Playwright
    try:
        pdf_bytes = render_pdf_bytes(html_content)
        with open(output_path, 'wb') as f:
            f.write(pdf_bytes)
        return output_path
    except Exception as e:
        # PDF generation failed - return HTML path as fallback
        print(f"PDF generation failed: {e}")
        print(f"HTML report saved to: {html_output_path}")
        return html_output_path


# ── Activity Report Generation ─────────────────────────────────────────────────

def generate_activity_report_html(output_path: str, brand_name: str, activity_data: dict,
                                  period_label: str = None, report_id: int = None) -> str:
    """
    Generate an enhanced activity report (PDF + HTML) with charts and insights.
    
    Args:
        output_path: Path where PDF should be saved
        brand_name: Brand name for the report
        activity_data: Dictionary containing all activity metrics
        period_label: Period label (e.g., "March Week 2, 2026")
        report_id: Optional report ID for linking
    
    Returns:
        Path to generated PDF or HTML file
    """
    from datetime import datetime
    
    template = jinja_env.get_template('activity_report.html')
    
    # Prepare context for template
    context = {
        'brand_name': brand_name,
        'period': period_label or 'Current Period',
        'generated_at': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'summary': activity_data.get('summary', {}),
        'week_over_week': activity_data.get('week_over_week', {}),
        'correlation': activity_data.get('correlation', {}),
        'issues': activity_data.get('issues', {}),
        'opportunities': activity_data.get('opportunities', {}),
        'daily_trend': activity_data.get('daily_trend', {}),
        'categories': activity_data.get('categories', {}),
        'geographic': activity_data.get('geographic', []),
        'narrative': activity_data.get('narrative', ''),
        'report_id': report_id,
        'zip': zip,  # Allow zip function in template
        'int': int,  # Allow int function in template
    }
    
    # Render HTML
    html_content = template.render(**context)
    
    # Save HTML version
    html_output_path = output_path.replace('.pdf', '.html')
    with open(html_output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Try to generate PDF with Playwright
    try:
        pdf_bytes = render_pdf_bytes(html_content)
        with open(output_path, 'wb') as f:
            f.write(pdf_bytes)
        return output_path
    except Exception as e:
        print(f"Activity PDF generation failed: {e}")
        print(f"HTML report saved to: {html_output_path}")
        return html_output_path


def prepare_activity_report_data(ds, brand_name: str, report_id: int = None,
                                  start_date: str = None, end_date: str = None) -> dict:
    """
    Prepare comprehensive activity report data from DataStore.
    
    Args:
        ds: DataStore instance
        brand_name: Brand name to filter by
        report_id: Optional report ID to filter by
        start_date: Start date for period (ISO format)
        end_date: End date for period (ISO format)
    
    Returns:
        Dictionary with all activity metrics for report generation
    """
    from datetime import datetime, timedelta
    import json
    
    # Get activity summary
    summary = ds.get_activity_summary(report_id=report_id, brand_name=brand_name)
    
    # Build week-over-week comparison
    week_over_week = _build_week_over_week(ds, brand_name, report_id, start_date, end_date)
    
    # Build activity-to-sales correlation
    correlation = _build_activity_correlation(ds, brand_name, report_id, summary)
    
    # Build issues and opportunities
    issues_opps = _build_issues_opportunities(ds, brand_name, report_id, summary)
    
    # Build daily trend
    daily_trend = _build_daily_trend(ds, brand_name, report_id, start_date, end_date)
    
    # Build category distribution
    categories = _build_category_distribution(ds, brand_name, report_id, summary)
    
    # Build geographic distribution
    geographic = _build_geographic_distribution(ds, brand_name, report_id, summary)
    
    # Generate narrative
    narrative = _generate_activity_narrative(summary, week_over_week, correlation, issues_opps)
    
    return {
        'summary': summary.get('totals', {}),
        'week_over_week': week_over_week,
        'correlation': correlation,
        'issues': issues_opps.get('issues', {}),
        'opportunities': issues_opps.get('opportunities', {}),
        'daily_trend': daily_trend,
        'categories': categories,
        'geographic': geographic,
        'narrative': narrative,
    }


def _build_week_over_week(ds, brand_name: str, report_id: int,
                          current_start: str, current_end: str) -> dict:
    """Calculate week-over-week comparison metrics with smart previous week detection."""
    # Get current period metrics
    current_summary = ds.get_activity_summary(report_id=report_id, brand_name=brand_name)
    current_totals = current_summary.get('totals', {})
    
    # Get previous week's report by looking at start_date
    prev_report = None
    prev_report_id = None
    
    if report_id and current_start:
        from datetime import datetime, timedelta
        try:
            current_start_dt = datetime.strptime(current_start, '%Y-%m-%d')
            # Previous week is 7 days before
            prev_week_start = (current_start_dt - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Find report that matches previous week
            all_reports = ds.get_all_reports()
            for r in all_reports:
                if r.get('start_date') == prev_week_start:
                    prev_report = r
                    prev_report_id = r['id']
                    break
            
            # Fallback: if no exact match, use the chronologically previous report
            if not prev_report:
                for i, r in enumerate(all_reports):
                    if r['id'] == report_id and i + 1 < len(all_reports):
                        prev_report = all_reports[i + 1]
                        prev_report_id = prev_report['id']
                        break
        except Exception:
            pass
    
    # Get previous week's activity data
    prev_totals = {}
    if prev_report_id:
        prev_summary = ds.get_activity_summary(report_id=prev_report_id, brand_name=brand_name)
        prev_totals = prev_summary.get('totals', {})
    
    # Get previous week's batch info for labels
    prev_week_label = "Previous Week"
    if prev_report:
        prev_week_label = prev_report.get('month_label', 'Previous Week')
    
    # Calculate changes
    def calc_change(current, previous):
        if not previous or previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 1)
    
    # Calculate absolute changes too
    def calc_abs_change(current, previous):
        if previous is None:
            return None
        return current - previous
    
    current_stores = current_totals.get('stores', 0) or 0
    current_visits = current_totals.get('visits', 0) or 0
    current_events = current_totals.get('events', 0) or 0
    current_issues = current_totals.get('issues', 0) or 0
    
    prev_stores = prev_totals.get('stores', 0) or 0
    prev_visits = prev_totals.get('visits', 0) or 0
    prev_events = prev_totals.get('events', 0) or 0
    prev_issues = prev_totals.get('issues', 0) or 0
    
    # Determine trend direction and insights
    stores_change = calc_change(current_stores, prev_stores)
    visits_change = calc_change(current_visits, prev_visits)
    events_change = calc_change(current_events, prev_events)
    issues_change = calc_change(current_issues, prev_issues)
    
    # Generate insight text
    insights = []
    if visits_change is not None:
        if visits_change > 20:
            insights.append(f"Activity increased significantly (+{visits_change}% visits)")
        elif visits_change > 5:
            insights.append(f"Activity increased (+{visits_change}% visits)")
        elif visits_change < -20:
            insights.append(f"Activity decreased significantly ({visits_change}% visits)")
        elif visits_change < -5:
            insights.append(f"Activity decreased ({visits_change}% visits)")
        else:
            insights.append("Activity remained stable")
    
    if stores_change is not None and abs(stores_change) > 10:
        if stores_change > 0:
            insights.append(f"store coverage expanded (+{stores_change}%)")
        else:
            insights.append(f"store coverage reduced ({stores_change}%)")
    
    if issues_change is not None:
        if issues_change > 30:
            insights.append(f"issues increased (+{issues_change}%)")
        elif issues_change < -30:
            insights.append(f"issues reduced ({issues_change}%)")
    
    return {
        'has_previous_week': prev_totals != {},
        'previous_week_label': prev_week_label,
        'stores_visited_change': stores_change,
        'stores_visited_abs_change': calc_abs_change(current_stores, prev_stores),
        'visits_change': visits_change,
        'visits_abs_change': calc_abs_change(current_visits, prev_visits),
        'images_change': events_change,
        'images_abs_change': calc_abs_change(current_events, prev_events),
        'issues_change': issues_change,
        'issues_abs_change': calc_abs_change(current_issues, prev_issues),
        'insights': insights,
        'comparison_data': {
            'labels': ['Stores', 'Visits', 'Events', 'Issues'],
            'current_data': [current_stores, current_visits, current_events, current_issues],
            'previous_data': [prev_stores, prev_visits, prev_events, prev_issues],
            'current_label': 'This Week',
            'previous_label': prev_week_label
        },
        'previous_week_summary': {
            'stores': prev_stores,
            'visits': prev_visits,
            'events': prev_events,
            'issues': prev_issues
        } if prev_totals else None
    }


def _build_activity_correlation(ds, brand_name: str, report_id: int, summary: dict) -> dict:
    """Build activity-to-sales correlation metrics."""
    totals = summary.get('totals', {})
    
    stores_visited = totals.get('stores', 0)
    
    # Get sales data if available for this brand
    orders_generated = 0
    stores_no_orders = stores_visited  # Default assumption
    
    if report_id and brand_name:
        try:
            # Try to get actual sales data from report
            report_data = ds.get_report_brand_data(report_id, brand_name)
            if report_data:
                orders_generated = report_data.get('order_count', 0)
                stores_no_orders = max(0, stores_visited - orders_generated)
        except:
            pass
    
    visit_to_order_rate = 0
    if stores_visited > 0:
        visit_to_order_rate = round((orders_generated / stores_visited) * 100, 1)
    
    return {
        'stores_visited': stores_visited,
        'orders_generated': orders_generated,
        'stores_no_orders': stores_no_orders,
        'visit_to_order_rate': visit_to_order_rate
    }


def _build_issues_opportunities(ds, brand_name: str, report_id: int, summary: dict) -> dict:
    """Extract and categorize issues and opportunities from activity data."""
    totals = summary.get('totals', {})
    issue_count = totals.get('issues', 0)
    
    # Get recent issues
    recent_issues = summary.get('recent_issues', [])
    
    # Categorize issues
    issue_breakdown = []
    issue_types = {}
    
    for issue in recent_issues:
        issue_type = issue.get('issue_type', 'unknown')
        severity = issue.get('severity', 'medium')
        
        if issue_type not in issue_types:
            issue_types[issue_type] = {'count': 0, 'severity': severity}
        issue_types[issue_type]['count'] += 1
    
    for issue_type, data in sorted(issue_types.items(), key=lambda x: x[1]['count'], reverse=True):
        issue_breakdown.append({
            'type': issue_type,
            'count': data['count'],
            'severity': data['severity']
        })
    
    # Extract opportunities from issue types
    opportunities = []
    opportunity_keywords = ['opportunity', 'restock', 'display', 'new_shelf']
    
    for issue in recent_issues:
        issue_type = issue.get('issue_type', '')
        answer = issue.get('answer', '')
        
        # Check for opportunity keywords
        if any(kw in issue_type.lower() or kw in str(answer).lower() for kw in opportunity_keywords):
            opportunities.append({
                'description': answer[:100] if answer else issue_type,
                'store_count': 1
            })
    
    return {
        'issues': {
            'total': issue_count,
            'breakdown': issue_breakdown[:5]
        },
        'opportunities': {
            'total': len(opportunities),
            'list': opportunities[:5]
        }
    }


def _build_daily_trend(ds, brand_name: str, report_id: int,
                       start_date: str, end_date: str) -> dict:
    """Build daily visit trend data."""
    # Get activity events grouped by date
    with ds._connect() as conn:
        query = """
            SELECT activity_date, COUNT(DISTINCT retailer_code) as store_count
            FROM activity_visits
            WHERE 1=1
        """
        params = []
        
        if report_id:
            query += " AND report_id=?"
            params.append(report_id)
        
        if brand_name:
            query += " AND survey_name LIKE ?"
            params.append(f"%{brand_name}%")
        
        query += " GROUP BY activity_date ORDER BY activity_date"
        
        rows = conn.execute(query, params).fetchall()
    
    labels = []
    values = []
    
    for row in rows:
        date_str = row['activity_date']
        if date_str:
            # Format date nicely
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                labels.append(dt.strftime('%a %d'))
            except:
                labels.append(date_str)
            values.append(row['store_count'])
    
    return {
        'labels': labels,
        'data': values
    }


def _build_category_distribution(ds, brand_name: str, report_id: int, summary: dict) -> dict:
    """Build store category distribution by querying actual retailer types from activity_events."""
    with ds._connect() as conn:
        query = """
            SELECT retailer_type, COUNT(DISTINCT retailer_code) as store_count
            FROM activity_events
            WHERE retailer_type IS NOT NULL AND retailer_type != ''
        """
        params = []
        
        if report_id:
            query += " AND report_id=?"
            params.append(report_id)
        
        if brand_name:
            query += " AND (survey_name LIKE ? OR retailer_name LIKE ?)"
            params.extend([f"%{brand_name}%", f"%{brand_name}%"])
        
        query += " GROUP BY retailer_type ORDER BY store_count DESC"
        
        rows = conn.execute(query, params).fetchall()
    
    # Map retailer types to readable names
    type_mapping = {
        'RTL': 'Retail',
        'CORP': 'Corporate', 
        'DALA': 'Dala',
        'DLR': 'Distributor',
        'RETAIL': 'Retail',
        'CORPORATE': 'Corporate',
        'DISTRIBUTOR': 'Distributor',
    }
    
    categories = {}
    for row in rows:
        rtype = row['retailer_type'] or 'Other'
        display_name = type_mapping.get(rtype.upper(), rtype)
        categories[display_name] = row['store_count']
    
    # Fallback if no data
    if not categories:
        total_stores = summary.get('totals', {}).get('stores', 0)
        if total_stores:
            categories = {'Retail': total_stores}
        else:
            categories = {'Retail': 1}
    
    return {
        'labels': list(categories.keys()),
        'data': list(categories.values())
    }


def _build_geographic_distribution(ds, brand_name: str, report_id: int, summary: dict) -> list:
    """Build geographic distribution by city."""
    with ds._connect() as conn:
        query = """
            SELECT retailer_city, retailer_state, COUNT(DISTINCT retailer_code) as store_count
            FROM activity_visits
            WHERE retailer_city IS NOT NULL AND retailer_city != ''
        """
        params = []
        
        if report_id:
            query += " AND report_id=?"
            params.append(report_id)
        
        if brand_name:
            query += " AND survey_name LIKE ?"
            params.append(f"%{brand_name}%")
        
        query += " GROUP BY retailer_city, retailer_state ORDER BY store_count DESC LIMIT 10"
        
        rows = conn.execute(query, params).fetchall()
    
    # Calculate total for percentages
    total = sum(r['store_count'] for r in rows) or 1
    
    geographic = []
    for row in rows:
        geographic.append({
            'name': row['retailer_city'],
            'state': row['retailer_state'] or 'Unknown',
            'store_count': row['store_count'],
            'percentage': round((row['store_count'] / total) * 100, 1)
        })
    
    return geographic


def _generate_activity_narrative(summary: dict, wow: dict, correlation: dict, issues_opps: dict) -> str:
    """Generate a narrative summary of the activity report with week-over-week insights."""
    totals = summary.get('totals', {})
    
    stores = totals.get('stores', 0)
    visits = totals.get('visits', 0)
    issues = totals.get('issues', 0)
    
    visit_rate = correlation.get('visit_to_order_rate', 0)
    stores_no_orders = correlation.get('stores_no_orders', 0)
    orders = correlation.get('orders_generated', 0)
    
    # Build narrative
    parts = []
    
    # Opening - always show stores and visits
    if stores and visits:
        parts.append(f"This period shows {stores} store{'s' if stores != 1 else ''} visited across {visits} field visit{'s' if visits != 1 else ''}.")
    elif stores:
        parts.append(f"This period shows {stores} store{'s' if stores != 1 else ''} visited.")
    elif visits:
        parts.append(f"This period shows {visits} field visit{'s' if visits != 1 else ''}.")
    
    # Week-over-week insights (add early for impact)
    if wow.get('has_previous_week'):
        wow_insights = wow.get('insights', [])
        if wow_insights:
            parts.append(" ".join(wow_insights) + " compared to " + wow.get('previous_week_label', 'the previous period') + ".")
        
        # Add specific comparisons if significant
        stores_change = wow.get('stores_visited_change')
        visits_change = wow.get('visits_change')
        issues_change = wow.get('issues_change')
        
        # Build comparison sentence
        comp_parts = []
        if visits_change is not None and abs(visits_change) >= 5:
            if visits_change > 0:
                comp_parts.append(f"{visits_change}% more visits")
            else:
                comp_parts.append(f"{abs(visits_change)}% fewer visits")
        
        if stores_change is not None and abs(stores_change) >= 10:
            if stores_change > 0:
                comp_parts.append(f"{stores_change}% more stores reached")
            else:
                comp_parts.append(f"{abs(stores_change)}% fewer stores")
        
        if comp_parts:
            parts.append("This represents " + ", ".join(comp_parts) + ".")
    
    # Performance - only mention if we have meaningful data
    if visit_rate and visit_rate > 0:
        if visit_rate >= 50:
            parts.append(f"Strong conversion rate at {visit_rate}% of visits generating orders.")
        elif visit_rate >= 25:
            parts.append(f"Moderate conversion with {visit_rate}% of visits resulting in orders.")
        else:
            if stores_no_orders and stores_no_orders > 0:
                parts.append(f"Opportunity for improvement: only {visit_rate}% of visits generated orders, with {stores_no_orders} stores not ordering.")
            else:
                parts.append(f"Opportunity for improvement: only {visit_rate}% of visits generated orders.")
    elif orders and orders > 0:
        parts.append(f"{orders} order{'s' if orders != 1 else ''} were generated from field activity.")
    
    # Issues - only mention if there are issues
    if issues and issues > 0:
        parts.append(f"{issues} issue{'s' if issues != 1 else ''} were identified in the field requiring attention.")
        
        # Add context about issue trend
        issues_change = wow.get('issues_change')
        if issues_change is not None:
            if issues_change > 30:
                parts.append(f"This is a {issues_change}% increase in issues compared to last week.")
            elif issues_change < -30:
                parts.append(f"This is a {abs(issues_change)}% decrease in issues compared to last week.")
    
    return " ".join(parts) if parts else "Activity data for this period is being processed."
