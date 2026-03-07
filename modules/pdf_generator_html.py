"""
pdf_generator_html.py — HTML-based PDF builder using Playwright.

Generates professional 2-page PDFs from Jinja2 templates with embedded charts.
Page 1: Dashboard overview (matches Power BI layout)
Page 2: Inventory detail, performance scorecard, store heatmap
"""

import os
import base64
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


def render_pdf_report_html(brand_name: str, kpis: dict,
                           start_date: str, end_date: str,
                           portfolio_avg_revenue: float = None,
                           total_portfolio_revenue: float = None,
                           ai_narrative: str = None,
                           sheets_url: str = None) -> str:
    """Render the print-oriented report HTML used for PDF export."""
    # ── Charts (matplotlib → base64) ──────────────────────────────────────────
    dual_trend_chart  = chart_dual_trend(kpis['daily_sales'])
    stock_chart       = chart_stock_vertical(kpis['closing_stock'])
    reorder_chart     = chart_reorder(kpis['reorder_analysis'])
    heatmap_chart     = chart_store_heatmap(kpis['store_heatmap_df'])
    top_stores_chart  = chart_top_stores(kpis['top_stores'])
    top_products_chart = chart_product_value(kpis['product_value'])

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

    # Reports always use the deterministic analysis summary.
    narrative = generate_narrative(brand_name, kpis, start_date, end_date)

    # ── Dates ──────────────────────────────────────────────────────────────────
    start_dt      = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt        = datetime.strptime(end_date,   '%Y-%m-%d')
    display_start = start_dt.strftime('%d %b %Y')
    display_end   = end_dt.strftime('%d %b %Y')

    # ── Logo ───────────────────────────────────────────────────────────────────
    logo_data = ''
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, 'rb') as f:
            logo_data = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"

    # ── Render template ────────────────────────────────────────────────────────
    template = jinja_env.get_template('report_template.html')
    return template.render(
        brand_name=brand_name,
        start_date=display_start,
        end_date=display_end,
        kpis=kpis,
        narrative=narrative,
        sheets_url=sheets_url,
        logo_path=logo_data,
        perf=perf,
        portfolio_share=portfolio_share,
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


def render_pdf_bytes(html_content: str) -> bytes:
    """Render PDF bytes from already-built HTML."""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_content(html_content, wait_until='networkidle')
        pdf_bytes = page.pdf(
            format='A4',
            print_background=True,
            margin={'top': '0mm', 'right': '0mm', 'bottom': '0mm', 'left': '0mm'},
        )
        browser.close()
    return pdf_bytes


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
