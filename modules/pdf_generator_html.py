"""
pdf_generator_html.py — HTML-based PDF builder using Playwright.

Generates professional 2-page PDFs from Jinja2 templates with embedded charts.
Page 1: Dashboard overview (matches Power BI layout)
Page 2: Inventory detail, performance scorecard, store heatmap
"""

import os
import base64
import calendar as _calendar
import glob
import shutil
import subprocess
import sys
from datetime import datetime

from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.sync_api import sync_playwright

from .kpi import generate_narrative, calculate_perf_score
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


def _build_narrative_sections(brand_name: str, kpis: dict,
                               start_date: str, end_date: str) -> dict:
    """Build structured narrative bullet sections matching the ORISIRISI PDF format."""
    start_dt   = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt     = datetime.strptime(end_date,   '%Y-%m-%d')
    month_name = start_dt.strftime('%B')
    report_days = (end_dt - start_dt).days + 1

    # Determine whether this is a full month (≥22 days) or a partial/weekly report
    is_full_month = report_days >= 22
    weeks_elapsed = max(1, (report_days + 6) // 7)  # ceiling division

    next_month_num  = end_dt.month % 12 + 1
    next_month_name = _calendar.month_name[next_month_num]

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
        top_store_name = top_stores_df.iloc[0]['Store']
        top_store_rev  = float(top_stores_df.iloc[0]['Revenue'])

    product_df = kpis.get('product_value')
    top_product_name, top_product_rev = '', 0.0
    bottom_product_name, bottom_product_rev = '', 0.0
    if product_df is not None and not product_df.empty:
        top_product_name = product_df.iloc[0]['SKU']
        top_product_rev  = float(product_df.iloc[0]['Revenue'])
        if len(product_df) > 1:
            bottom_product_name = product_df.iloc[-1]['SKU']
            bottom_product_rev  = float(product_df.iloc[-1]['Revenue'])

    closing_stock_df = kpis.get('closing_stock')

    # ── KPI bullets ───────────────────────────────────────────────────────────
    period_desc = 'month' if is_full_month else f'first {report_days} day{"s" if report_days != 1 else ""} of {month_name}'
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

    if is_full_month:
        nonzero = [v for v in weekly_rev_pct if v > 0]
        if len(nonzero) >= 2 and nonzero[-1] > nonzero[0]:
            strengths.append(
                f'Consistent Growth Trend: {brand_name} maintained a steady upward trajectory '
                f'across the month, with WoW revenue share growing from {nonzero[0]:.0f}% to {nonzero[-1]:.0f}%.'
            )
        elif wow_rev > 0:
            strengths.append(
                f'Positive Momentum: Revenue grew {wow_rev:+.1f}% week-over-week in the final week, '
                f'demonstrating strong closing momentum for {brand_name}.'
            )
    else:
        # Partial month: focus on what happened in the first N days
        if total_rev > 0 and trading_days > 0:
            daily_avg = total_rev / trading_days
            projected = daily_avg * 28
            strengths.append(
                f'Opening Momentum: {brand_name} generated {_full(total_rev)} in the first '
                f'{report_days} day{"s" if report_days != 1 else ""}, implying a {_short(projected)} '
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

    if is_full_month and repeat_pct >= 50:
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

    if store_count < 5 and not is_full_month:
        gaps.append(
            f'Limited Distribution: Only {store_count} store{"s" if store_count != 1 else ""} '
            f'active in the first {report_days} day{"s" if report_days != 1 else ""}. '
            f'Significantly more outlets need to be activated to build meaningful sales volume.'
        )
    elif store_count < 10 and is_full_month:
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

    if 0 < stock_days < 14 and is_full_month:
        gaps.append(
            f'Stock Cover Risk: At {stock_days:.0f} days of remaining cover, inventory replenishment '
            f'must be initiated immediately to avoid stockouts next month.'
        )

    if is_full_month and repeat_pct < 40 and store_count > 0:
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

    if is_full_month:
        # Full month → strategic recommendations for NEXT month
        rec_label = 'Recommendations'

        # Stock replenishment
        if closing_stock_df is not None and not closing_stock_df.empty:
            low = closing_stock_df[closing_stock_df['Closing Stock (Cartons)'] < 10]
            for _, row in low.head(2).iterrows():
                needed = max(10, int(row['Closing Stock (Cartons)'] or 0) * 4)
                recs.append(
                    f'Stock Optimization: Deliver at least {needed} packs of {row["SKU"]} to the '
                    f'warehouse before {next_month_name} begins to meet anticipated demand.'
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
            recs.append(
                f'Distribution Expansion: Target at least {store_count + 8} active stores in '
                f'{next_month_name} to reduce single-store revenue concentration risk.'
            )

        # Retention
        if repeat_pct < 40:
            recs.append(
                f'Retention Drive: Re-engage the {store_count} current stores with targeted '
                f'incentives to push the repeat order rate above 50% in {next_month_name}.'
            )

    else:
        # Partial month → actionable focus items for the REMAINDER of this month
        rec_label = 'Recommendations'
        week_label = (
            'Week 1' if weeks_elapsed == 1 else
            f'Week {weeks_elapsed}'
        )
        remaining_weeks = max(1, 4 - weeks_elapsed)

        # Distribution push is almost always the top priority early in the month
        if store_count < 15:
            target = max(store_count * 3, store_count + 10)
            recs.append(
                f'Distribution Drive: With only {store_count} store{"s" if store_count != 1 else ""} '
                f'active after {week_label}, aggressively target at least {target} stores across the '
                f'remaining {remaining_weeks} week{"s" if remaining_weeks != 1 else ""} of {month_name}.'
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
                f'Account Development: Follow up with {top_store_name} and other {week_label} '
                f'purchasers to secure repeat orders and gather intel on in-store demand.'
            )

        # Pacing awareness with projected monthly revenue
        if total_rev > 0 and trading_days > 0:
            daily_avg   = total_rev / trading_days
            projected   = daily_avg * 28
            recs.append(
                f'Monthly Pacing: Current {week_label} revenue ({_full(total_rev)}) projects a '
                f'{_short(projected)} monthly total. Accelerate store activation to close any '
                f'gap against targets before month-end.'
            )

    if not recs:
        recs.append(
            f'Build on current momentum by expanding distribution and ensuring adequate '
            f'stock levels throughout the remainder of {month_name}.'
        )

    return {
        'report_title':     f'{month_name} Monthly Sales Report For {brand_name}',
        'period_label':     f'{month_name} KPIs',
        'next_month_label': rec_label,
        'kpi_bullets':      kpi_bullets,
        'strengths':        strengths,
        'gaps':             gaps,
        'recommendations':  recs,
        'is_full_month':    is_full_month,
        'report_days':      report_days,
    }


def render_pdf_report_html(brand_name: str, kpis: dict,
                           start_date: str, end_date: str,
                           portfolio_avg_revenue: float = None,
                           total_portfolio_revenue: float = None,
                           ai_narrative: str = None,
                           sheets_url: str = None) -> str:
    """Render the print-oriented report HTML used for PDF export."""
    # ── Charts (matplotlib → base64) ──────────────────────────────────────────
    # Use _for_print=True so charts are generated at print-optimised sizes
    # with fonts scaled to be legible at the PDF snapshot display size.
    dual_trend_chart   = chart_dual_trend(kpis['daily_sales'], _for_print=True)
    stock_chart        = chart_stock_vertical(kpis['closing_stock'], _for_print=True)
    reorder_chart      = chart_reorder(kpis['reorder_analysis'])
    heatmap_chart      = chart_store_heatmap(kpis['store_heatmap_df'])
    top_stores_chart   = chart_top_stores(kpis['top_stores'], _for_print=True)
    top_products_chart = chart_product_value(kpis['product_value'], _for_print=True)

    # ── Performance scorecard ──────────────────────────────────────────────────
    perf = calculate_perf_score(kpis, portfolio_avg_revenue)

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

    top_stores_table = [
        {
            'store': r['Store'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / total_rev * 100, 1),
        }
        for _, r in kpis['top_stores'].head(5).iterrows()
    ]

    top_products_table = [
        {
            'sku':   r['SKU'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / total_rev * 100, 1),
        }
        for _, r in kpis['product_value'].head(5).iterrows()
    ]

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
    narrative_sections = _build_narrative_sections(brand_name, kpis, start_date, end_date)

    # ── Dates ──────────────────────────────────────────────────────────────────
    start_dt      = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt        = datetime.strptime(end_date,   '%Y-%m-%d')
    display_start = start_dt.strftime('%d %b %Y')
    display_end   = end_dt.strftime('%d %b %Y')
    month_label   = start_dt.strftime('%B')

    # ── Logo ───────────────────────────────────────────────────────────────────
    logo_data = ''
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, 'rb') as f:
            logo_data = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"

    # ── Pre-computed display values ─────────────────────────────────────────────
    revenue_display = f'\u20a6{kpis["total_revenue"]:,.2f}'
    qty_display     = f'{kpis["total_qty"]:,.2f}'
    num_stores      = int(kpis.get('num_stores') or kpis.get('store_count') or 0)

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
        month_label=month_label,
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
        closing_stock_table=closing_stock_table,
        pickup_table=pickup_table,
        supply_table=supply_table,
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


def render_pdf_bytes(html_content: str) -> bytes:
    """Render PDF bytes reusing a persistent Chromium instance (no cold-start per call)."""
    global _browser_inst, _pw_instance
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
                    pdf_bytes = page.pdf(
                        format='A4',
                        print_background=True,
                        margin={'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'},
                    )
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


def generate_pdf_html(output_path: str, brand_name: str, kpis: dict,
                      start_date: str, end_date: str,
                      portfolio_avg_revenue: float = None,
                      total_portfolio_revenue: float = None,
                      ai_narrative: str = None,
                      sheets_url: str = None) -> str:
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
        ai_narrative=ai_narrative,
        sheets_url=sheets_url,
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
