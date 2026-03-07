"""
pdf_generator_html.py — HTML-based PDF builder using Playwright.

Generates professional 2-page PDFs from Jinja2 templates with embedded charts.
Page 1: Dashboard overview (matches Power BI layout)
Page 2: Inventory detail, performance scorecard, store heatmap
"""

import os
import base64
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


def _launch_chromium(p):
    """Launch Chromium via playwright handle `p`, trying every fallback path."""
    opts = {'headless': True, 'args': ['--no-sandbox', '--disable-dev-shm-usage']}

    def _try(executable_path=None):
        o = dict(opts)
        if executable_path:
            o['executable_path'] = executable_path
        return p.chromium.launch(**o)

    errors = []
    needs_install = False

    # 1. Default Playwright-managed browser
    try:
        return _try()
    except Exception as e:
        errors.append(f'default: {e}')
        needs_install = 'playwright install' in str(e).lower()

    # 2. Known system paths
    for path in _candidate_paths():
        if path and os.path.exists(path):
            try:
                return _try(executable_path=path)
            except Exception as e:
                errors.append(f'{path}: {e}')

    # 3. Auto-install then retry
    if needs_install:
        install_env = os.environ.copy()
        bp = os.getenv('PLAYWRIGHT_BROWSERS_PATH')
        if bp:
            install_env['PLAYWRIGHT_BROWSERS_PATH'] = bp
            os.makedirs(bp, exist_ok=True)
        subprocess.run(
            [sys.executable, '-m', 'playwright', 'install', 'chromium'],
            env=install_env, timeout=180, check=False,
        )
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
