"""
app.py — DALA Analytics Portal
Full web application: upload, generate, view, compare, alerts, brand portal.

Routes:
  GET  /                    Portal home + upload
  POST /generate            Upload Tally file → generate PDFs + HTMLs + save to DB
  GET  /dashboard           Portfolio dashboard (latest month)
  GET  /brands              All brand partners list
  GET  /brand/<slug>        Brand detail + historical charts
  GET  /history             Month selector
  GET  /compare             Brand vs brand comparison
  GET  /alerts              Smart alerts dashboard
  GET  /settings            Settings (email, WhatsApp, token management)
  GET  /portal/<token>      Brand partner self-service view
  POST /api/acknowledge     Acknowledge an alert
  POST /api/update_contact  Update brand email/WhatsApp
  POST /api/regenerate_token
  POST /api/deliver         Send email/WhatsApp to brand partners
  GET  /download/pdf/<fn>   Serve PDF
  GET  /download/html/<fn>  Serve HTML
  GET  /api/reports         JSON list of all reports
"""

import os, io, json, traceback, shutil, uuid, threading, tempfile, zipfile, hashlib, math
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from datetime import datetime
from functools import wraps
import numpy as np
import pandas as pd

from flask import (
    Flask, render_template, request, send_file,
    jsonify, redirect, url_for, abort, session, Response,
)

# Job tracking is persisted to SQLite via DataStore.create_job / update_job / get_job
# This fixes the multi-worker bug where in-memory dicts don't survive across gunicorn workers.
_JOBS = {}   # kept for legacy compatibility only — new code uses ds.get_job()

from modules.ingestion        import (
    load_and_clean, filter_by_date, split_by_brand,
    looks_like_store_label, looks_like_sku_label,
)
from modules.kpi              import calculate_kpis, calculate_perf_score, generate_narrative, build_reorder_trend
from modules.pdf_generator_html import generate_pdf_html
from modules.pdf_generator      import generate_pdf as generate_pdf_reportlab
from modules.pdf_generator_html import render_pdf_report_html, render_pdf_bytes, prepare_interactive_html_for_pdf
from modules.html_generator   import generate_html, render_html_report
from modules.portfolio_generator import generate_portfolio_html
from modules.data_store       import DataStore
from modules.alerts           import check_and_save_alerts, run_portfolio_alerts
from modules.predictor        import build_brand_forecasts, stock_depletion_date, growth_label, growth_color
from modules.gmv              import build_gmv_window
from modules.activity_intelligence import load_activity_dataframe, build_activity_payload
from modules.agent_copilot    import build_default_agent_actions, answer_admin_query, execute_admin_request
from modules.coach_features   import (
    history_available as coach_history_available,
    build_scope_snapshot,
    build_retailer_index,
    build_retailer_group_index,
    build_retailer_detail,
    build_retailer_group_detail,
    build_brand_coach_data,
)
from modules.coach_signals    import build_coach_payload, get_signal_thresholds
from modules.coach_operations import run_coach_refresh, backfill_recent_periods, validate_signal_quality
from modules.historical       import (
    get_brand_monthly_history, get_portfolio_monthly_trend,
    get_repeat_purchase_map_data, get_store_repeat_analysis, generate_insights,
    get_color_scheme_for_month, get_monthly_metrics
)
from modules.brand_names      import canonicalize_brand_name
from modules.geocoding        import is_geocoding_available
from modules.retailer_reports import render_retailer_html_report, render_retailer_pdf_report_html
from modules.narrative_ai     import (
    generate_brand_narrative, generate_portfolio_narrative,
    generate_bulk_narratives, gemini_available
)

# ── App config ────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dala-dev-secret-2026')
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024   # 100 MB

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR  = os.path.join(BASE_DIR, 'output')
PDF_DIR     = os.path.join(OUTPUT_DIR, 'pdf')
HTML_DIR    = os.path.join(OUTPUT_DIR, 'html')
os.makedirs(PDF_DIR,  exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)

ds = DataStore()
REPORT_SOURCE_DIR = os.path.join(os.path.dirname(ds.db_path), 'report_sources')
os.makedirs(REPORT_SOURCE_DIR, exist_ok=True)


def _archive_report_source(file_bytes, report_id, filename):
    safe_name = ''.join(ch if ch.isalnum() or ch in ('-', '_', '.') else '_' for ch in os.path.basename(filename or 'report.xlsx'))
    path = os.path.join(REPORT_SOURCE_DIR, f"{report_id}_{safe_name}")
    with open(path, 'wb') as handle:
        handle.write(file_bytes)
    return {
        'path': path,
        'size_bytes': len(file_bytes or b''),
        'sha1': hashlib.sha1(file_bytes or b'').hexdigest(),
    }


def _run_startup_data_repairs():
    try:
        summary = ds.repair_swapped_dimension_rows()
        if summary.get('rows_repaired'):
            ds.save_agent_memory(
                scope_type='workspace',
                scope_key='report-integrity',
                memory_text=(
                    f"Repaired {summary['rows_repaired']} historical brand detail row(s) after the retailer/SKU schema fix."
                ),
                memory_kind='repair',
                confidence=0.95,
                source='startup_repair',
                memory_layer='workspace',
                subject_type='workspace',
                subject_key='report-integrity',
                tags=['repair', 'schema', 'imports'],
                metadata=summary,
                pinned=True,
            )
    except Exception:
        pass


threading.Thread(target=_run_startup_data_repairs, daemon=True).start()
GENERATION_WORKERS = max(1, int(os.environ.get('GENERATION_WORKERS', '2') or '2'))
GENERATION_EXECUTOR = ThreadPoolExecutor(max_workers=GENERATION_WORKERS)


def _money_2dp(value):
    """Format currency-like floats consistently for UI display."""
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _money_csv_2dp(value):
    """Format currency-like floats for CSV/export payloads without separators."""
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _coerce_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _parse_generation_options(source):
    import_mode = (source.get('import_mode') or 'full').strip().lower()
    if import_mode not in {'fast', 'full'}:
        import_mode = 'full'

    defaults = {
        'generate_artifacts': import_mode == 'full',
        'generate_narratives': import_mode == 'full',
        'sync_sheets': import_mode == 'full',
        'refresh_copilot': import_mode == 'full',
        'launch_coach': import_mode == 'full',
    }

    return {
        'import_mode': import_mode,
        'generate_artifacts': _coerce_bool(source.get('generate_artifacts'), defaults['generate_artifacts']),
        'generate_narratives': _coerce_bool(source.get('generate_narratives'), defaults['generate_narratives']),
        'sync_sheets': _coerce_bool(source.get('sync_sheets'), defaults['sync_sheets']),
        'refresh_copilot': _coerce_bool(source.get('refresh_copilot'), defaults['refresh_copilot']),
        'launch_coach': _coerce_bool(source.get('launch_coach'), defaults['launch_coach']),
    }


def _generation_result_payload(report_id, report_meta, brands, portfolio_filename=None, brands_done=None, options=None, coach_job_id=None, import_audit=None):
    options = options or {}
    brands_done = brands_done or []
    report_type = report_meta.get('report_type') if report_meta else None
    pdf_count = sum(1 for row in brands_done if row.get('pdf'))
    html_count = sum(1 for row in brands_done if row.get('html'))
    dashboard_url = f"/dashboard?report_id={report_id}"
    latest_dashboard_url = "/dashboard"
    portfolio_dashboard = f"/download/html/{portfolio_filename}" if portfolio_filename else dashboard_url
    payload = {
        'report_id': report_id,
        'report_type': report_type,
        'brands': len(brands),
        'pdf_count': pdf_count,
        'html_count': html_count,
        'dashboard_url': dashboard_url,
        'latest_dashboard_url': latest_dashboard_url,
        'portfolio_dashboard': portfolio_dashboard,
        'portfolio_file': portfolio_filename,
        'artifacts_deferred': not options.get('generate_artifacts', True),
        'background_refresh_queued': bool(options.get('background_refresh_queued')),
        'import_mode': options.get('import_mode', 'full'),
        'coach_job_id': coach_job_id,
    }
    if import_audit:
        payload.update({
            'workbook_brand_count': int(import_audit.get('workbook_brand_count') or 0),
            'active_brand_count': int(import_audit.get('active_brand_count') or 0),
            'selected_brand_count': int(import_audit.get('selected_brand_count') or 0),
            'persisted_brand_count': int(import_audit.get('persisted_brand_count') or 0),
            'zero_sales_brands': list(import_audit.get('zero_sales_brands') or []),
            'filtered_out_brands': list(import_audit.get('filtered_out_brands') or []),
            'persisted_brands': list(import_audit.get('persisted_brands') or []),
            'missing_brands': list(import_audit.get('missing_brands') or []),
            'import_warnings': list(import_audit.get('warnings') or []),
        })
    return payload


def _run_post_import_maintenance(report_id, brands, all_kpis, report_type, end_date, portfolio_avg_revenue, filename, refresh_copilot=True, launch_coach=True):
    """Secondary refresh work that should not block the import completing."""
    try:
        report = ds.get_report(report_id) or {}
        for brand_name in brands:
            kpis = all_kpis.get(brand_name) or {}
            history = ds.get_brand_history(brand_name, limit=3)
            check_and_save_alerts(report_id, brand_name, kpis, portfolio_avg_revenue, history[1:], ds)

        run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
        _compute_and_save_churn(report_id)

        for brand_name in brands:
            kpis = all_kpis.get(brand_name)
            if not kpis:
                continue
            _attach_reorder_trend(
                brand_name=brand_name,
                kpis=kpis,
                report_type=report_type or report.get('report_type'),
                cutoff_date=end_date,
            )

        if refresh_copilot:
            _refresh_copilot_state(
                report_id=report_id,
                reason=f'post_import_refresh:{filename}',
                source='report_generation',
            )
        if launch_coach:
            _launch_coach_refresh_job(
                mode='current',
                report_id=report_id,
                include_pairs=True,
                source='report_generation',
            )
    except Exception:
        traceback.print_exc()


def _decorate_retailer_rows(rows):
    decorated = []
    for raw in rows or []:
        row = dict(raw)
        tags = []
        revenue_mom = row.get('revenue_mom')
        if revenue_mom is not None and revenue_mom <= -12:
            tags.append({'label': 'At Risk', 'class': 'badge-red'})
        elif revenue_mom is not None and revenue_mom >= 15:
            tags.append({'label': 'Accelerating', 'class': 'badge-green'})
        if float(row.get('repeat_rate') or 0) < 30 and int(row.get('active_brands') or 0) >= 3:
            tags.append({'label': 'Weak Reorder', 'class': 'badge-amber'})
        if int(row.get('active_brands') or 0) <= 2:
            tags.append({'label': 'Narrow Mix', 'class': 'badge-muted'})
        row['tags'] = tags[:3]
        decorated.append(row)
    return decorated


def _decorate_retailer_group_rows(rows):
    decorated = []
    for raw in rows or []:
        row = dict(raw)
        tags = []
        revenue_mom = row.get('revenue_mom')
        if revenue_mom is not None and revenue_mom <= -12:
            tags.append({'label': 'Chain Under Pressure', 'class': 'badge-red'})
        elif revenue_mom is not None and revenue_mom >= 15:
            tags.append({'label': 'Chain Accelerating', 'class': 'badge-green'})
        if float(row.get('repeat_rate') or 0) < 30 and int(row.get('retailer_count') or 0) >= 3:
            tags.append({'label': 'Weak Reorder Depth', 'class': 'badge-amber'})
        if int(row.get('retailer_count') or 0) >= 5:
            tags.append({'label': f"{int(row.get('retailer_count') or 0)} branches", 'class': 'badge-blue'})
        row['tags'] = tags[:3]
        decorated.append(row)
    return decorated


def _build_retailer_summary(rows):
    rows = rows or []
    total_revenue = sum(float(row.get('total_revenue') or 0) for row in rows)
    row_count = len(rows)
    return {
        'retailer_count': row_count,
        'total_revenue': total_revenue,
        'avg_repeat': (
            sum(float(row.get('repeat_rate') or 0) for row in rows) / row_count
            if row_count else 0
        ),
        'avg_brands': (
            sum(float(row.get('active_brands') or 0) for row in rows) / row_count
            if row_count else 0
        ),
        'avg_health': (
            sum(float(row.get('health_score') or 0) for row in rows) / row_count
            if row_count else 0
        ),
        'top_retailer': rows[0] if rows else None,
    }


def _build_retailer_group_summary(rows):
    rows = rows or []
    row_count = len(rows)
    total_revenue = sum(float(row.get('total_revenue') or 0) for row in rows)
    return {
        'group_count': row_count,
        'branch_count': sum(int(row.get('retailer_count') or 0) for row in rows),
        'total_revenue': total_revenue,
        'avg_repeat': (
            sum(float(row.get('repeat_rate') or 0) for row in rows) / row_count
            if row_count else 0
        ),
        'avg_branches': (
            sum(float(row.get('retailer_count') or 0) for row in rows) / row_count
            if row_count else 0
        ),
        'top_group': rows[0] if rows else None,
    }


@app.template_filter('money2')
def money2_filter(value):
    return _money_2dp(value)

# ── Admin Auth ────────────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', '')

# Public paths that never require admin auth (login flow + brand partner portals + webhooks)
_PUBLIC_PREFIXES = ('/login', '/logout', '/static', '/portal/', '/webhook/', '/api/reports', '/health')

@app.before_request
def _enforce_admin_auth():
    """Redirect to /login for protected routes when ADMIN_PASSWORD is set."""
    if not ADMIN_PASSWORD:
        return  # Auth disabled
    if session.get('admin_authenticated'):
        return  # Already authenticated
    path = request.path
    if any(path == p or path.startswith(p) for p in _PUBLIC_PREFIXES):
        return  # Public path
    return redirect(url_for('login_page', next=path))


def _safe_name(brand_name):
    return brand_name.replace(' ', '_').replace("'", '').replace('/', '-')


def _brand_report_context(brand_name: str, cutoff_date: str | None = None) -> dict:
    history = list(reversed(ds.get_brand_history(brand_name, limit=36)))
    if cutoff_date:
        cutoff = str(cutoff_date)[:10]
        history = [
            row for row in history
            if not row.get('start_date') or str(row.get('start_date'))[:10] <= cutoff
        ]

    forecast_bundle = build_brand_forecasts({brand_name: history}).get(
        ds.analytics_brand_name(brand_name), {}
    )
    return {
        'history': history,
        'growth_outlook': forecast_bundle.get('growth_outlook'),
        'gmv_window': build_gmv_window(history, cutoff_date=cutoff_date),
    }


def _mini_trend_svg(points: list[dict], value_key: str,
                    label_key: str = 'label',
                    stroke: str = '#1B2B5E',
                    fill: str = 'rgba(27,43,94,0.10)') -> str:
    if not points:
        return ''

    values = []
    labels = []
    for point in points:
        try:
            values.append(float(point.get(value_key) or 0))
        except Exception:
            values.append(0.0)
        labels.append(str(point.get(label_key) or ''))

    if len(values) == 1:
        values = values + values
        labels = labels + labels

    width = 640
    height = 210
    pad_x = 28
    pad_y = 18
    inner_w = width - (pad_x * 2)
    inner_h = height - (pad_y * 2) - 18
    min_v = min(values)
    max_v = max(values)
    span = (max_v - min_v) or 1.0

    coords = []
    count = len(values)
    for index, value in enumerate(values):
        x = pad_x + (inner_w * index / max(count - 1, 1))
        y = pad_y + inner_h - ((value - min_v) / span * inner_h)
        coords.append((round(x, 1), round(y, 1), value, labels[index]))

    path = ' '.join(f'{x},{y}' for x, y, _, _ in coords)
    area = f'{pad_x},{pad_y + inner_h} ' + path + f' {pad_x + inner_w},{pad_y + inner_h}'

    grid_lines = []
    for step in range(5):
        y = pad_y + (inner_h * step / 4)
        grid_lines.append(
            f'<line x1="{pad_x}" y1="{y:.1f}" x2="{pad_x + inner_w}" y2="{y:.1f}" '
            f'stroke="#E3E8F3" stroke-width="1"/>'
        )

    label_nodes = []
    if coords:
        stride = max(1, len(coords) // 6)
        for idx, (x, _, _, label) in enumerate(coords):
            if idx % stride == 0 or idx == len(coords) - 1:
                label_nodes.append(
                    f'<text x="{x:.1f}" y="{height - 6}" text-anchor="middle" '
                    f'font-size="10" fill="#70809D">{label}</text>'
                )

    point_nodes = []
    for x, y, value, label in coords:
        point_nodes.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{stroke}" opacity="0.95">'
            f'<title>{label}: {value:,.0f}</title></circle>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
        f'preserveAspectRatio="none" style="display:block;width:100%;height:{height}px;">'
        f'{"".join(grid_lines)}'
        f'<polygon points="{area}" fill="{fill}" />'
        f'<polyline points="{path}" fill="none" stroke="{stroke}" stroke-width="3" '
        f'stroke-linecap="round" stroke-linejoin="round" />'
        f'{"".join(point_nodes)}'
        f'{"".join(label_nodes)}'
        '</svg>'
    )


def _horizontal_bar_svg(rows: list[dict], value_key: str, label_key: str,
                        value_fmt=None, color: str = '#1B2B5E',
                        width: int = 640, bar_height: int = 30, max_rows: int = 6) -> str:
    rows = rows[:max_rows]
    if not rows:
        return ''
    values = []
    labels = []
    for row in rows:
        try:
            values.append(float(row.get(value_key) or 0))
        except Exception:
            values.append(0.0)
        labels.append(str(row.get(label_key) or ''))
    max_v = max(values) or 1.0
    left = 180
    right = 70
    top = 14
    usable_w = width - left - right
    gap = 16
    height = top + len(rows) * (bar_height + gap) + 16
    parts = [
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" style="display:block;width:100%;height:{height}px;">'
    ]
    for i, (label, value) in enumerate(zip(labels, values)):
        y = top + i * (bar_height + gap)
        w = max(10, usable_w * (value / max_v))
        shown = value_fmt(value) if value_fmt else f'{value:,.0f}'
        parts.append(f'<text x="0" y="{y + 19}" font-size="11" fill="#1B2B5E" font-weight="700">{label[:32]}</text>')
        parts.append(f'<rect x="{left}" y="{y}" width="{usable_w}" height="{bar_height}" rx="10" fill="#EEF2F8"/>')
        parts.append(f'<rect x="{left}" y="{y}" width="{w:.1f}" height="{bar_height}" rx="10" fill="{color}"/>')
        parts.append(f'<text x="{width - 4}" y="{y + 19}" text-anchor="end" font-size="11" fill="#6F7B92" font-weight="700">{shown}</text>')
    parts.append('</svg>')
    return ''.join(parts)


def _quadrant_svg(high_sales_low_activity: list[dict], high_activity_low_sales: list[dict],
                  width: int = 640, height: int = 220) -> str:
    def _dot(cx, cy, label, color):
        return (
            f'<circle cx="{cx}" cy="{cy}" r="7" fill="{color}" opacity="0.9"><title>{label}</title></circle>'
            f'<text x="{cx + 10}" y="{cy + 4}" font-size="10" fill="#1B2B5E">{label[:22]}</text>'
        )

    mid_x = width / 2
    mid_y = height / 2
    parts = [
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" style="display:block;width:100%;height:{height}px;">',
        f'<rect x="0" y="0" width="{mid_x}" height="{mid_y}" fill="rgba(26,122,74,0.04)"/>',
        f'<rect x="{mid_x}" y="0" width="{mid_x}" height="{mid_y}" fill="rgba(232,25,44,0.04)"/>',
        f'<rect x="0" y="{mid_y}" width="{mid_x}" height="{mid_y}" fill="rgba(46,134,193,0.04)"/>',
        f'<rect x="{mid_x}" y="{mid_y}" width="{mid_x}" height="{mid_y}" fill="rgba(192,146,42,0.05)"/>',
        f'<line x1="{mid_x}" y1="0" x2="{mid_x}" y2="{height}" stroke="#DDE3ED" stroke-width="2"/>',
        f'<line x1="0" y1="{mid_y}" x2="{width}" y2="{mid_y}" stroke="#DDE3ED" stroke-width="2"/>',
        '<text x="14" y="20" font-size="11" fill="#1A7A4A" font-weight="700">Strong on both</text>',
        f'<text x="{mid_x + 14}" y="20" font-size="11" fill="#E8192C" font-weight="700">High sales, low activity</text>',
        f'<text x="14" y="{mid_y + 20}" font-size="11" fill="#2E86C1" font-weight="700">High activity, low sales</text>',
        f'<text x="{mid_x + 14}" y="{mid_y + 20}" font-size="11" fill="#C0922A" font-weight="700">Low on both / watch</text>',
    ]
    hs = high_sales_low_activity[:4]
    ha = high_activity_low_sales[:4]
    for i, row in enumerate(hs):
        cx = mid_x + 32 + (i * 54)
        cy = 52 + (i % 2) * 40
        parts.append(_dot(cx, cy, str(row.get('store') or ''), '#E8192C'))
    for i, row in enumerate(ha):
        cx = 32 + (i * 54)
        cy = mid_y + 38 + (i % 2) * 40
        parts.append(_dot(cx, cy, str(row.get('retailer_name') or ''), '#2E86C1'))
    parts.append('</svg>')
    return ''.join(parts)


def _donut_svg(rows: list[dict], value_key: str, label_key: str,
               width: int = 420, height: int = 250,
               colors: list[str] | None = None) -> str:
    rows = [row for row in (rows or []) if float(row.get(value_key) or 0) > 0]
    if not rows:
        return ''

    palette = colors or ['#E8192C', '#1B2B5E', '#2E86C1', '#1A7A4A', '#C0922A', '#8E44AD', '#5D6D7E']
    total = sum(float(row.get(value_key) or 0) for row in rows) or 1.0
    cx = 110
    cy = height / 2
    outer_r = 74
    inner_r = 42
    legend_x = 220
    legend_y = 36

    def _polar(radius: float, angle_deg: float) -> tuple[float, float]:
        angle_rad = math.radians(angle_deg - 90)
        return cx + radius * math.cos(angle_rad), cy + radius * math.sin(angle_rad)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none" style="display:block;width:100%;height:{height}px;">',
        f'<circle cx="{cx}" cy="{cy}" r="{outer_r}" fill="#F4F7FC" />',
    ]

    start_angle = 0.0
    legend_rows = []
    for index, row in enumerate(rows[:6]):
        value = float(row.get(value_key) or 0)
        if value <= 0:
            continue
        sweep = (value / total) * 360.0
        end_angle = start_angle + sweep
        x1, y1 = _polar(outer_r, start_angle)
        x2, y2 = _polar(outer_r, end_angle)
        large_arc = 1 if sweep > 180 else 0
        color = palette[index % len(palette)]
        path = (
            f'M {cx},{cy} '
            f'L {x1:.2f},{y1:.2f} '
            f'A {outer_r},{outer_r} 0 {large_arc} 1 {x2:.2f},{y2:.2f} Z'
        )
        parts.append(f'<path d="{path}" fill="{color}" opacity="0.94"><title>{row.get(label_key)}: {value:,.0f}</title></path>')
        pct = (value / total) * 100.0
        label = str(row.get(label_key) or '')
        legend_rows.append(
            f'<rect x="{legend_x}" y="{legend_y + index * 28}" width="12" height="12" rx="3" fill="{color}"/>'
            f'<text x="{legend_x + 18}" y="{legend_y + 10 + index * 28}" font-size="11" fill="#1B2B5E" font-weight="700">{label[:28]}</text>'
            f'<text x="{width - 8}" y="{legend_y + 10 + index * 28}" text-anchor="end" font-size="11" fill="#6F7B92">{pct:.1f}%</text>'
        )
        start_angle = end_angle

    parts.append(f'<circle cx="{cx}" cy="{cy}" r="{inner_r}" fill="#FFFFFF"/>')
    parts.append(f'<text x="{cx}" y="{cy - 6}" text-anchor="middle" font-size="12" fill="#7A849E" font-weight="700">Total</text>')
    parts.append(f'<text x="{cx}" y="{cy + 16}" text-anchor="middle" font-size="22" fill="#1B2B5E" font-weight="800">{int(total):,}</text>')
    parts.extend(legend_rows)
    parts.append('</svg>')
    return ''.join(parts)


def _build_brand_activity_report(brand_name: str, report_id: int | None,
                                 kpis: dict | None = None) -> dict:
    brand_name = str(brand_name or '').strip()
    empty = {
        'available': False,
        'current': {
            'mentions': 0, 'stores': 0, 'active_days': 0, 'salespeople': 0,
            'visits': 0, 'issues': 0, 'opportunities': 0, 'photos': 0,
        },
        'history': {
            'mentions': 0, 'stores': 0, 'active_days': 0, 'all_time_salespeople': 0,
            'first_seen': None, 'last_seen': None,
        },
        'headline': 'No structured activity record is available for this brand yet.',
        'summary': 'Import activity reports to unlock retailer touchpoints, issue context, and field execution coverage.',
        'current_trend_svg': '',
        'history_trend_svg': '',
        'daily_points': [],
        'history_points': [],
        'issue_rows': [],
        'store_rows': [],
        'salesperson_rows': [],
        'salesman_activity_rows': [],
        'survey_rows': [],
        'retailer_type_rows': [],
        'state_rows': [],
        'detail_rows': [],
        'high_sales_low_activity': [],
        'high_activity_low_sales': [],
        'recommended_actions': [],
        'coverage_ratio': 0,
        'matched_store_count': 0,
        'top_salesmen_svg': '',
        'survey_mix_svg': '',
        'retailer_type_svg': '',
        'state_mix_svg': '',
    }
    if not brand_name:
        return empty

    current_summary = ds.get_activity_summary(report_id=report_id, brand_name=brand_name)
    history_summary = ds.get_activity_brand_summary(brand_name, limit=12)

    report_clause = ''
    report_params: list[object] = []
    if report_id:
        report_clause = ' AND abm.report_id=?'
        report_params.append(report_id)

    brand_scope_cte = """
        WITH brand_scope AS (
            SELECT DISTINCT
                abm.report_id,
                abm.activity_date,
                abm.retailer_code,
                COALESCE(NULLIF(abm.retailer_name, ''), abm.retailer_code) AS retailer_name,
                LOWER(COALESCE(abm.brand_name, '')) AS brand_key
            FROM activity_brand_mentions abm
            WHERE LOWER(COALESCE(abm.brand_name, ''))=LOWER(?)"""
    brand_scope_params: list[object] = [brand_name]
    if report_id:
        brand_scope_cte += " AND abm.report_id=?"
        brand_scope_params.append(report_id)
    brand_scope_cte += "\n        )\n"

    with ds._connect() as conn:
        daily_rows = conn.execute(
            f"""SELECT activity_date,
                       COUNT(*) AS mentions,
                       COUNT(DISTINCT retailer_code) AS stores
                  FROM activity_brand_mentions
                 WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?){report_clause.replace('abm.', '')}
                 GROUP BY activity_date
                 ORDER BY activity_date ASC""",
            (brand_name, *report_params),
        ).fetchall()
        history_rows = conn.execute(
            """SELECT substr(activity_date, 1, 7) AS period,
                      COUNT(*) AS mentions,
                      COUNT(DISTINCT retailer_code) AS stores
                 FROM activity_brand_mentions
                WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?)
                GROUP BY substr(activity_date, 1, 7)
                ORDER BY period ASC""",
            (brand_name,),
        ).fetchall()
        bounds = conn.execute(
            """SELECT MIN(activity_date) AS first_seen,
                      MAX(activity_date) AS last_seen,
                      COUNT(DISTINCT salesman_name) AS salespeople
                 FROM activity_events
                WHERE id IN (
                    SELECT ae.id
                      FROM activity_events ae
                      JOIN activity_brand_mentions abm
                        ON abm.retailer_code = ae.retailer_code
                       AND abm.activity_date = ae.activity_date
                     WHERE LOWER(COALESCE(abm.brand_name, ''))=LOWER(?)
                )""",
            (brand_name,),
        ).fetchone()
        store_rows = conn.execute(
            f"""{brand_scope_cte}
                SELECT s.retailer_code,
                       s.retailer_name,
                       s.mentions,
                       s.active_days,
                       s.skus,
                       COALESCE(v.visits, 0) AS visits,
                       COALESCE(i.issues, 0) AS issues
                  FROM (
                        SELECT abm.retailer_code,
                               COALESCE(NULLIF(abm.retailer_name, ''), abm.retailer_code) AS retailer_name,
                               COUNT(*) AS mentions,
                               COUNT(DISTINCT abm.activity_date) AS active_days,
                               COUNT(DISTINCT COALESCE(NULLIF(abm.sku_name, ''), NULL)) AS skus
                          FROM activity_brand_mentions abm
                         WHERE LOWER(COALESCE(abm.brand_name, ''))=LOWER(?){report_clause}
                         GROUP BY abm.retailer_code, COALESCE(NULLIF(abm.retailer_name, ''), abm.retailer_code)
                    ) s
             LEFT JOIN (
                        SELECT bs.retailer_code,
                               COUNT(DISTINCT av.id) AS visits
                          FROM brand_scope bs
                          JOIN activity_visits av
                            ON av.retailer_code = bs.retailer_code
                           AND av.activity_date = bs.activity_date
                           AND av.report_id = bs.report_id
                         GROUP BY bs.retailer_code
                    ) v
                    ON v.retailer_code = s.retailer_code
             LEFT JOIN (
                        SELECT bs.retailer_code,
                               COUNT(DISTINCT ai.id) AS issues
                          FROM brand_scope bs
                          JOIN activity_issues ai
                            ON ai.retailer_code = bs.retailer_code
                           AND ai.activity_date = bs.activity_date
                           AND ai.report_id = bs.report_id
                           AND LOWER(COALESCE(ai.brand_name, '')) = bs.brand_key
                         GROUP BY bs.retailer_code
                    ) i
                    ON i.retailer_code = s.retailer_code
                 ORDER BY s.mentions DESC, s.retailer_name ASC
                 LIMIT 12""",
            (*brand_scope_params, brand_name, *report_params),
        ).fetchall()
        salesperson_rows = conn.execute(
            f"""SELECT COALESCE(NULLIF(ae.salesman_name, ''), 'Unassigned') AS salesman_name,
                       COUNT(DISTINCT abm.id) AS mentions,
                       COUNT(DISTINCT abm.retailer_code) AS stores,
                       COUNT(DISTINCT abm.activity_date) AS active_days
                  FROM activity_brand_mentions abm
             LEFT JOIN activity_events ae
                    ON ae.retailer_code = abm.retailer_code
                   AND ae.activity_date = abm.activity_date
                   {'AND ae.report_id = abm.report_id' if report_id else ''}
                 WHERE LOWER(COALESCE(abm.brand_name, ''))=LOWER(?){report_clause}
                 GROUP BY COALESCE(NULLIF(ae.salesman_name, ''), 'Unassigned')
                 ORDER BY mentions DESC, salesman_name ASC
                 LIMIT 10""",
            (brand_name, *report_params),
        ).fetchall()
        brand_event_from = f"""{brand_scope_cte}
            SELECT ae.*
              FROM brand_scope bs
              JOIN activity_events ae
                ON ae.retailer_code = bs.retailer_code
               AND ae.activity_date = bs.activity_date
               AND ae.report_id = bs.report_id"""
        brand_event_params = list(brand_scope_params)

        survey_rows = conn.execute(
            f"""WITH scoped_events AS ({brand_event_from})
                SELECT COALESCE(NULLIF(survey_name, ''), 'Unspecified') AS survey_name,
                       COUNT(*) AS activities,
                       COUNT(DISTINCT retailer_code) AS stores
                  FROM scoped_events
                 GROUP BY COALESCE(NULLIF(survey_name, ''), 'Unspecified')
                 ORDER BY activities DESC, survey_name ASC
                 LIMIT 10""",
            brand_event_params,
        ).fetchall()
        retailer_type_rows = conn.execute(
            f"""WITH scoped_events AS ({brand_event_from})
                SELECT COALESCE(NULLIF(retailer_type, ''), 'Unspecified') AS retailer_type,
                       COUNT(*) AS activities,
                       COUNT(DISTINCT retailer_code) AS stores
                  FROM scoped_events
                 GROUP BY COALESCE(NULLIF(retailer_type, ''), 'Unspecified')
                 ORDER BY activities DESC, retailer_type ASC
                 LIMIT 8""",
            brand_event_params,
        ).fetchall()
        state_rows = conn.execute(
            f"""WITH scoped_events AS ({brand_event_from})
                SELECT COALESCE(NULLIF(retailer_state, ''), 'Unspecified') AS retailer_state,
                       COUNT(*) AS activities,
                       COUNT(DISTINCT retailer_code) AS stores
                  FROM scoped_events
                 GROUP BY COALESCE(NULLIF(retailer_state, ''), 'Unspecified')
                 ORDER BY activities DESC, retailer_state ASC
                 LIMIT 8""",
            brand_event_params,
        ).fetchall()
        salesman_activity_rows = conn.execute(
            f"""WITH scoped_events AS ({brand_event_from})
                SELECT COALESCE(NULLIF(salesman_name, ''), 'Unassigned') AS salesman_name,
                       COUNT(*) AS activities,
                       COUNT(DISTINCT retailer_code) AS stores,
                       COUNT(DISTINCT activity_date) AS active_days
                  FROM scoped_events
                 GROUP BY COALESCE(NULLIF(salesman_name, ''), 'Unassigned')
                 ORDER BY activities DESC, salesman_name ASC
                 LIMIT 10""",
            brand_event_params,
        ).fetchall()
        detail_rows = conn.execute(
            f"""WITH scoped_events AS ({brand_event_from})
                SELECT activity_date,
                       COALESCE(NULLIF(salesman_name, ''), 'Unassigned') AS salesman_name,
                       COALESCE(NULLIF(retailer_name, ''), retailer_code) AS retailer_name,
                       COALESCE(NULLIF(retailer_city, ''), retailer_state, 'Unknown') AS retailer_city,
                       COALESCE(NULLIF(survey_name, ''), 'Unspecified') AS survey_name,
                       question,
                       label,
                       answer
                  FROM scoped_events
                 ORDER BY activity_date DESC, id DESC
                 LIMIT 14""",
            brand_event_params,
        ).fetchall()

    current = {
        'activities': int(current_summary.get('totals', {}).get('events', 0)),
        'mentions': int(current_summary.get('totals', {}).get('events', 0)),
        'stores': int(current_summary.get('totals', {}).get('stores', 0)),
        'active_days': int(current_summary.get('totals', {}).get('active_days', 0)),
        'salespeople': len([r for r in salesperson_rows if (r['salesman_name'] or '').strip()]),
        'visits': int(current_summary.get('totals', {}).get('visits', 0)),
        'issues': int(current_summary.get('totals', {}).get('issues', 0)),
        'opportunities': int(current_summary.get('totals', {}).get('opportunities', 0)),
        'photos': int(current_summary.get('totals', {}).get('photos', 0)),
        'survey_types': len(survey_rows),
        'states': len(state_rows),
        'retailer_types': len(retailer_type_rows),
    }
    history = {
        'mentions': int(history_summary.get('mentions', 0)),
        'stores': int(history_summary.get('stores', 0)),
        'active_days': int(history_summary.get('active_days', 0)),
        'all_time_salespeople': int((bounds['salespeople'] if bounds else 0) or 0),
        'first_seen': bounds['first_seen'] if bounds else None,
        'last_seen': bounds['last_seen'] if bounds else None,
    }

    daily_points = [
        {
            'label': datetime.strptime(str(row['activity_date']), '%Y-%m-%d').strftime('%d %b'),
            'mentions': int(row['mentions'] or 0),
            'stores': int(row['stores'] or 0),
        }
        for row in daily_rows
    ]
    history_points = []
    for row in history_rows:
        period = str(row['period'] or '')
        label = period
        if len(period) == 7:
            try:
                label = datetime.strptime(period + '-01', '%Y-%m-%d').strftime('%b %Y')
            except Exception:
                label = period
        history_points.append({
            'label': label,
            'mentions': int(row['mentions'] or 0),
            'stores': int(row['stores'] or 0),
        })

    issue_total = max(current['issues'], 1)
    issue_rows = []
    for row in current_summary.get('top_issues', []):
        count = int(row.get('count') or 0)
        issue_rows.append({
            'issue_type': str(row.get('issue_type') or 'Unspecified').replace('_', ' ').title(),
            'count': count,
            'pct': round((count / issue_total) * 100, 1) if current['issues'] else 0,
        })

    current_store_rows = []
    for row in store_rows:
        current_store_rows.append({
            'retailer_code': row['retailer_code'],
            'retailer_name': row['retailer_name'],
            'mentions': int(row['mentions'] or 0),
            'active_days': int(row['active_days'] or 0),
            'skus': int(row['skus'] or 0),
            'visits': int(row['visits'] or 0),
            'issues': int(row['issues'] or 0),
        })

    salesperson_table = [
        {
            'salesman_name': row['salesman_name'],
            'mentions': int(row['mentions'] or 0),
            'stores': int(row['stores'] or 0),
            'active_days': int(row['active_days'] or 0),
        }
        for row in salesperson_rows
    ]
    salesman_activity_table = [
        {
            'salesman_name': row['salesman_name'],
            'activities': int(row['activities'] or 0),
            'stores': int(row['stores'] or 0),
            'active_days': int(row['active_days'] or 0),
        }
        for row in salesman_activity_rows
    ]
    survey_table = [
        {
            'survey_name': row['survey_name'],
            'activities': int(row['activities'] or 0),
            'stores': int(row['stores'] or 0),
        }
        for row in survey_rows
    ]
    retailer_type_table = [
        {
            'retailer_type': row['retailer_type'],
            'activities': int(row['activities'] or 0),
            'stores': int(row['stores'] or 0),
        }
        for row in retailer_type_rows
    ]
    state_table = [
        {
            'retailer_state': row['retailer_state'],
            'activities': int(row['activities'] or 0),
            'stores': int(row['stores'] or 0),
        }
        for row in state_rows
    ]
    detail_table = [
        {
            'activity_date': row['activity_date'],
            'salesman_name': row['salesman_name'],
            'retailer_name': row['retailer_name'],
            'retailer_city': row['retailer_city'],
            'survey_name': row['survey_name'],
            'question': str(row['question'] or '')[:82],
            'label': str(row['label'] or '')[:40],
            'answer': str(row['answer'] or '')[:56],
        }
        for row in detail_rows
    ]

    sales_store_rows = []
    if kpis and kpis.get('top_stores') is not None and not kpis['top_stores'].empty:
        sales_store_rows = [
            {
                'store': str(row['Store']),
                'revenue': float(row['Revenue'] or 0),
            }
            for _, row in kpis['top_stores'].sort_values('Revenue', ascending=False).head(8).iterrows()
        ]

    activity_store_names = {str(row['retailer_name']).strip().lower() for row in current_store_rows}
    sales_store_names = {str(row['store']).strip().lower() for row in sales_store_rows}

    high_sales_low_activity = [
        row for row in sales_store_rows
        if str(row['store']).strip().lower() not in activity_store_names
    ][:4]
    high_activity_low_sales = [
        row for row in current_store_rows
        if str(row['retailer_name']).strip().lower() not in sales_store_names
    ][:4]

    matched_store_count = len(activity_store_names & sales_store_names)
    coverage_ratio = 0
    if kpis and float(kpis.get('num_stores') or 0) > 0:
        coverage_ratio = round((matched_store_count / float(kpis.get('num_stores') or 1)) * 100, 1)

    if current['activities'] and current['stores']:
        headline = f'Field execution covered {current["stores"]} retailers across {current["active_days"]} active days.'
        summary = (
            f'{brand_name} logged {current["activities"]} activity responses, {current["visits"]} structured visits, '
            f'{current["issues"]} issues, and {current["survey_types"]} survey types in the current report period. '
            f'{matched_store_count} active selling stores are directly evidenced in the activity log, across {current["states"]} states.'
        )
    else:
        headline = 'This period has sales activity but little or no linked field evidence.'
        summary = (
            f'No report-scoped activity evidence was found for {brand_name}. '
            'The report still shows sales and inventory correctly, but field execution commentary is limited until activity imports are available for the same period.'
        )

    recommended_actions = []
    if high_sales_low_activity:
        stores = ', '.join(row['store'] for row in high_sales_low_activity[:3])
        recommended_actions.append(
            f'Protect revenue-heavy stores with no matched activity log this period: {stores}.'
        )
    if high_activity_low_sales:
        stores = ', '.join(row['retailer_name'] for row in high_activity_low_sales[:3])
        recommended_actions.append(
            f'Convert field attention into orders at activity-heavy outlets not showing up in top sales: {stores}.'
        )
    if current['issues'] and issue_rows:
        recommended_actions.append(
            f'Prioritise the dominant field issue ({issue_rows[0]["issue_type"]}) which represents {issue_rows[0]["pct"]}% of logged issues.'
        )
    if salesman_activity_table:
        lead_rep = salesman_activity_table[0]
        recommended_actions.append(
            f'Use {lead_rep["salesman_name"]} as the reference playbook: {lead_rep["activities"]} activity responses across {lead_rep["stores"]} retailers.'
        )
    if retailer_type_table:
        lead_type = retailer_type_table[0]
        recommended_actions.append(
            f'Keep execution weighted toward {lead_type["retailer_type"]}: it accounts for {lead_type["activities"]} tracked responses this period.'
        )
    if kpis and current['stores'] < int(kpis.get('num_stores') or 0):
        recommended_actions.append(
            f'Lift execution coverage from {current["stores"]} activity-tagged stores against {int(kpis.get("num_stores") or 0)} selling stores in the same period.'
        )
    if not recommended_actions:
        recommended_actions.append(
            'Maintain the current execution cadence and keep the next visit focused on availability, merchandising, and repeat-order conversion.'
        )

    empty.update({
        'available': bool(history['mentions'] or current['mentions']),
        'current': current,
        'history': history,
        'headline': headline,
        'summary': summary,
        'daily_points': daily_points,
        'history_points': history_points,
        'current_trend_svg': _mini_trend_svg(daily_points, 'mentions', stroke='#E8192C', fill='rgba(232,25,44,0.10)'),
        'history_trend_svg': _mini_trend_svg(history_points, 'mentions', stroke='#1B2B5E', fill='rgba(27,43,94,0.10)'),
        'issue_rows': issue_rows,
        'store_rows': current_store_rows,
        'salesperson_rows': salesperson_table,
        'salesman_activity_rows': salesman_activity_table,
        'survey_rows': survey_table,
        'retailer_type_rows': retailer_type_table,
        'state_rows': state_table,
        'detail_rows': detail_table,
        'high_sales_low_activity': high_sales_low_activity,
        'high_activity_low_sales': high_activity_low_sales,
        'recommended_actions': recommended_actions[:6],
        'coverage_ratio': coverage_ratio,
        'matched_store_count': matched_store_count,
        'issue_mix_svg': _horizontal_bar_svg(
            issue_rows,
            'count',
            'issue_type',
            value_fmt=lambda v: f'{int(v)}',
            color='#E8192C',
            width=620,
            bar_height=24,
        ),
        'store_ranking_svg': _horizontal_bar_svg(
            current_store_rows,
            'mentions',
            'retailer_name',
            value_fmt=lambda v: f'{int(v)} men.',
            color='#1B2B5E',
            width=620,
            bar_height=24,
        ),
        'salesperson_svg': _horizontal_bar_svg(
            salesperson_table,
            'mentions',
            'salesman_name',
            value_fmt=lambda v: f'{int(v)} men.',
            color='#2E86C1',
            width=620,
            bar_height=24,
        ),
        'top_salesmen_svg': _horizontal_bar_svg(
            salesman_activity_table,
            'activities',
            'salesman_name',
            value_fmt=lambda v: f'{int(v)} act.',
            color='#8E111B',
            width=620,
            bar_height=24,
        ),
        'survey_mix_svg': _horizontal_bar_svg(
            survey_table,
            'activities',
            'survey_name',
            value_fmt=lambda v: f'{int(v)}',
            color='#C8191E',
            width=620,
            bar_height=24,
        ),
        'retailer_type_svg': _donut_svg(
            retailer_type_table,
            'activities',
            'retailer_type',
            width=420,
            height=250,
        ),
        'state_mix_svg': _horizontal_bar_svg(
            state_table,
            'activities',
            'retailer_state',
            value_fmt=lambda v: f'{int(v)}',
            color='#1A7A4A',
            width=620,
            bar_height=24,
        ),
        'linkage_svg': _quadrant_svg(high_sales_low_activity, high_activity_low_sales),
    })
    return empty


def _attach_reorder_trend(brand_name: str, kpis: dict | None,
                          report_type: str | None = None,
                          cutoff_date: str | None = None) -> dict | None:
    """Attach a comparable-period reorder trend payload to the KPI dict."""
    if kpis is None:
        return None

    def _filter_history(rows):
        if not cutoff_date:
            return rows
        cutoff = str(cutoff_date)[:10]
        return [
            row for row in rows
            if not row.get('start_date') or str(row.get('start_date'))[:10] <= cutoff
        ]

    history = _filter_history(ds.get_brand_history(brand_name, limit=8, report_type=report_type))
    if len(history) < 2 and report_type:
        history = _filter_history(ds.get_brand_history(brand_name, limit=8))

    kpis['reorder_trend'] = build_reorder_trend(
        history_rows=list(reversed(history)),
        kpis=kpis,
    )
    return kpis


def _report_summary_payload(brand_name: str, kpis: dict, report: dict | None) -> dict:
    """Build a report-native summary block from saved KPI history."""
    report_type = str((report or {}).get('report_type') or '').strip().lower()
    period_label = (report or {}).get('month_label') or (
        f"{(report or {}).get('start_date', '')} to {(report or {}).get('end_date', '')}"
    ).strip()
    history_rows = list(reversed(ds.get_brand_history(brand_name, limit=18, report_type=report_type or None)))

    current_report_id = (report or {}).get('id')
    current_start = str((report or {}).get('start_date') or '')[:10]
    current_end = str((report or {}).get('end_date') or '')[:10]

    def _safe_float(value):
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _safe_int(value):
        try:
            return int(round(float(value or 0)))
        except Exception:
            return 0

    def _fmt_money(value):
        return f"₦{_safe_float(value):,.2f}"

    def _fmt_qty(value):
        return f"{_safe_float(value):,.1f}"

    def _pct_delta(current_value, previous_value):
        current_value = _safe_float(current_value)
        previous_value = _safe_float(previous_value)
        if previous_value == 0:
            return None if current_value == 0 else 100.0
        return round(((current_value - previous_value) / previous_value) * 100, 1)

    def _comparison_label(kind: str) -> str:
        if kind == 'weekly':
            return 'prior week'
        if kind == 'monthly':
            return 'prior month'
        if kind == 'quarterly':
            return 'prior quarter'
        if kind == 'yearly':
            return 'prior year'
        return 'prior comparable period'

    current_history_row = None
    previous_history_row = None
    current_index = None
    for idx, row in enumerate(history_rows):
        row_start = str(row.get('start_date') or '')[:10]
        row_end = str(row.get('end_date') or '')[:10]
        if (
            (current_report_id and row.get('report_id') == current_report_id) or
            (row_start == current_start and row_end == current_end)
        ):
            current_history_row = row
            current_index = idx
            break
    if current_index is not None and current_index > 0:
        previous_history_row = history_rows[current_index - 1]
    if current_history_row is None:
        current_history_row = {
            'report_id': current_report_id,
            'month_label': period_label,
            'start_date': current_start,
            'end_date': current_end,
            'report_type': report_type,
            'total_revenue': _safe_float(kpis.get('total_revenue')),
            'total_qty': _safe_float(kpis.get('total_qty')),
            'num_stores': _safe_int(kpis.get('num_stores')),
            'repeat_pct': _safe_float(kpis.get('repeat_pct')),
            'repeat_stores': _safe_int(kpis.get('repeat_stores')),
            'single_stores': _safe_int(kpis.get('single_stores')),
            'stock_days_cover': _safe_float(kpis.get('stock_days_cover')),
            'inv_health_status': str(kpis.get('inv_health_status') or ''),
            'top_store_name': kpis.get('top_store_name'),
            'top_store_revenue': _safe_float(kpis.get('top_store_revenue')),
        }

    current_revenue = _safe_float(kpis.get('total_revenue'))
    current_qty = _safe_float(kpis.get('total_qty'))
    current_stores = _safe_int(kpis.get('num_stores'))
    current_repeat = _safe_float(kpis.get('repeat_pct'))
    current_repeat_stores = _safe_int(kpis.get('repeat_stores'))
    current_single_stores = _safe_int(kpis.get('single_stores'))
    stock_days = _safe_float(kpis.get('stock_days_cover'))
    inventory_status = str(kpis.get('inv_health_status') or 'No Stock Data').strip() or 'No Stock Data'
    top_store_name = str(kpis.get('top_store_name') or '').strip()
    top_store_revenue = _safe_float(kpis.get('top_store_revenue'))
    top_store_pct = _safe_float(kpis.get('top_store_pct'))

    previous_revenue = _safe_float(previous_history_row.get('total_revenue')) if previous_history_row else 0.0
    previous_qty = _safe_float(previous_history_row.get('total_qty')) if previous_history_row else 0.0
    previous_repeat = _safe_float(previous_history_row.get('repeat_pct')) if previous_history_row else 0.0
    previous_stores = _safe_int(previous_history_row.get('num_stores')) if previous_history_row else 0

    revenue_delta = _pct_delta(current_revenue, previous_revenue) if previous_history_row else None
    quantity_delta = _pct_delta(current_qty, previous_qty) if previous_history_row else None
    repeat_delta = round(current_repeat - previous_repeat, 1) if previous_history_row else None
    store_delta = current_stores - previous_stores if previous_history_row else None
    compare_label = _comparison_label(report_type)

    if revenue_delta is not None and revenue_delta <= -12:
        headline = 'Revenue slowed'
    elif revenue_delta is not None and revenue_delta >= 12:
        headline = 'Revenue accelerated'
    elif repeat_delta is not None and repeat_delta >= 10:
        headline = 'Repeat purchase improved'
    elif inventory_status in {'Watch', 'Low Stock'} or (stock_days > 0 and stock_days < 7):
        headline = 'Inventory needs attention'
    else:
        headline = 'Performance summary'

    summary_parts = [
        (
            f"{brand_name} closed {period_label} at {_fmt_money(current_revenue)} "
            f"from {_fmt_qty(current_qty)} packs across {current_stores} "
            f"store{'s' if current_stores != 1 else ''}."
        )
    ]
    if previous_history_row:
        delta_bits = []
        if revenue_delta is not None:
            direction = 'up' if revenue_delta >= 0 else 'down'
            delta_bits.append(f"revenue was {direction} {abs(revenue_delta):.1f}% versus the {compare_label}")
        if quantity_delta is not None:
            direction = 'up' if quantity_delta >= 0 else 'down'
            delta_bits.append(f"quantity moved {direction} {abs(quantity_delta):.1f}%")
        if repeat_delta is not None:
            direction = 'improved to' if repeat_delta >= 0 else 'softened to'
            delta_bits.append(f"repeat purchase {direction} {current_repeat:.1f}% from {previous_repeat:.1f}%")
        if delta_bits:
            summary_parts.append(delta_bits[0].capitalize() + (f", while {', '.join(delta_bits[1:])}." if len(delta_bits) > 1 else '.'))
    else:
        summary_parts.append(
            f"This is the first stored {report_type or 'comparable'} period for direct comparison, so the next cycle will unlock a trend delta."
        )

    if inventory_status != 'No Stock Data' or stock_days > 0 or top_store_name:
        ops_bits = []
        if inventory_status != 'No Stock Data':
            ops_bits.append(f"inventory is currently {inventory_status.lower()}")
        if stock_days > 0:
            ops_bits.append(f"with {stock_days:.1f} days of cover")
        if top_store_name and top_store_revenue > 0:
            concentration = f", contributing {top_store_pct:.1f}% of revenue" if top_store_pct > 0 else ''
            ops_bits.append(f"the lead outlet was {top_store_name}{concentration}")
        if ops_bits:
            summary_parts.append(', '.join(ops_bits).capitalize() + '.')

    signals = []
    if previous_history_row and revenue_delta is not None:
        signals.append({
            'signal_type': 'revenue_comparison',
            'evidence': {
                'title': f"Revenue vs {compare_label.title()}",
                'message': (
                    f"{brand_name} posted {_fmt_money(current_revenue)} in {period_label}, "
                    f"{'up' if revenue_delta >= 0 else 'down'} {abs(revenue_delta):.1f}% versus the {compare_label}."
                ),
            },
        })
    else:
        signals.append({
            'signal_type': 'comparison_pending',
            'evidence': {
                'title': 'Comparable Trend',
                'message': f"No prior {report_type or 'comparable'} report is stored yet for a direct side-by-side comparison.",
            },
        })
    signals.append({
        'signal_type': 'repeat_purchase',
        'evidence': {
            'title': 'Repeat Purchase',
            'message': (
                f"{current_repeat_stores} repeat and {current_single_stores} single-order store"
                f"{'' if current_single_stores == 1 else 's'} produced a {current_repeat:.1f}% repeat rate."
            ),
        },
    })
    signals.append({
        'signal_type': 'inventory_and_distribution',
        'evidence': {
            'title': 'Inventory and Distribution',
            'message': (
                f"{current_stores} active stores supplied the period; inventory is {inventory_status}"
                + (f" with {stock_days:.1f} days of cover." if stock_days > 0 else '.')
            ),
        },
    })

    recommended_actions = []
    if inventory_status in {'Watch', 'Low Stock'} or (stock_days > 0 and stock_days < 7):
        recommended_actions.append('Restock the fastest-moving SKUs before the next trading cycle.')
    if previous_history_row and revenue_delta is not None and revenue_delta < 0:
        recommended_actions.append(f"Recover lost volume in the outlets that performed strongest in the {compare_label}.")
    if current_repeat < 35 or current_single_stores > current_repeat_stores:
        recommended_actions.append('Follow up one-time buyers to convert them into repeat orders.')
    if top_store_pct >= 40 and top_store_name:
        recommended_actions.append(f"Reduce concentration risk by widening distribution beyond {top_store_name}.")
    if current_stores < 8:
        recommended_actions.append('Increase active store count before the next comparable period closes.')
    if not recommended_actions:
        recommended_actions.append('Protect the current run rate and monitor the next comparable period closely.')

    return {
        'summary': {
            'headline': headline,
            'summary': ' '.join(part for part in summary_parts if part),
            'recommended_actions': recommended_actions[:4],
            'used_gemini': False,
        },
        'signals': signals[:3],
        'action_items': [],
        'recommendation_items': [],
    }


def _coach_summary_for_snapshot(snapshot: dict | None, persist: bool = True, use_gemini: bool = True) -> dict:
    if not snapshot:
        return {'summary': {'headline': 'Coach summary unavailable', 'summary': 'No data is available for this scope yet.', 'recommended_actions': [], 'used_gemini': False}, 'signals': [], 'action_items': [], 'recommendation_items': []}
    try:
        payload = build_coach_payload(ds, snapshot, persist=persist, use_gemini=use_gemini)
        ds.save_coach_run(
            run_type='coach_summary',
            scope_type=snapshot.get('scope_type') or 'portfolio',
            scope_key=snapshot.get('scope_key') or 'global',
            report_id=snapshot.get('report_id'),
            result=payload,
            status='completed',
        )
        return payload
    except Exception as exc:
        return {
            'summary': {
                'headline': 'Coach summary unavailable',
                'summary': str(exc),
                'recommended_actions': [],
                'used_gemini': False,
            },
            'signals': [],
            'action_items': [],
            'recommendation_items': [],
        }


def _run_coach_refresh_job(job_id: str, mode: str = 'current', report_id: int | None = None,
                           month_value: str | None = None, monthly_count: int = 6,
                           weekly_count: int = 4, include_pairs: bool = True,
                           source: str = 'manual'):
    def _progress(progress_value: int, message: str):
        ds.update_job(job_id, progress=max(1, min(int(progress_value), 99)), current_brand=message, report_id=report_id)

    try:
        ds.update_job(job_id, progress=2, current_brand='Preparing coach refresh', report_id=report_id)
        if mode == 'backfill_recent':
            result = backfill_recent_periods(
                ds,
                monthly_count=monthly_count,
                weekly_count=weekly_count,
                include_pairs=include_pairs,
                progress_cb=_progress,
            )
        else:
            result = run_coach_refresh(
                ds,
                report_id=report_id,
                month_value=month_value,
                include_pairs=include_pairs,
                persist=True,
                progress_cb=_progress,
            )
        ds.save_coach_run(
            run_type='coach_refresh',
            scope_type='portfolio',
            scope_key=month_value or str(report_id or 'latest'),
            report_id=result.get('report_id') or report_id,
            result=result,
            status='completed',
        )
        ds.update_job(
            job_id,
            status='done',
            progress=100,
            current_brand='Coach refresh complete',
            report_id=result.get('report_id') or report_id,
            result_json={'mode': mode, 'source': source, **result},
        )
    except Exception as exc:
        ds.save_coach_run(
            run_type='coach_refresh',
            scope_type='portfolio',
            scope_key=month_value or str(report_id or 'latest'),
            report_id=report_id,
            result={'mode': mode, 'source': source, 'error': str(exc)},
            status='failed',
        )
        ds.update_job(job_id, status='error', current_brand='Coach refresh failed', report_id=report_id, error_msg=str(exc))


def _launch_coach_refresh_job(mode: str = 'current', report_id: int | None = None,
                              month_value: str | None = None, monthly_count: int = 6,
                              weekly_count: int = 4, include_pairs: bool = True,
                              source: str = 'manual') -> str:
    job_id = uuid.uuid4().hex
    ds.create_job(job_id)
    thread = threading.Thread(
        target=_run_coach_refresh_job,
        args=(job_id,),
        kwargs={
            'mode': mode,
            'report_id': report_id,
            'month_value': month_value,
            'monthly_count': monthly_count,
            'weekly_count': weekly_count,
            'include_pairs': include_pairs,
            'source': source,
        },
        daemon=True,
    )
    thread.start()
    return job_id


def _compute_and_save_churn(report_id: int):
    """
    Compute store churn for all brands in a report by comparing top_stores_json
    with the previous report's stored data.  Called after brand_detail_json is saved.
    """
    try:
        from modules.kpi import calculate_churn
        report = ds.get_report(report_id)
        if not report:
            return
        # Find the immediately preceding report
        all_reports = ds.get_all_reports()
        prev_report = None
        for r in sorted(all_reports, key=lambda x: x['start_date']):
            if r['start_date'] < report['start_date']:
                prev_report = r
        if not prev_report:
            return

        brands = [b['brand_name'] for b in ds.get_all_brand_kpis(report_id)]
        for brand_name in brands:
            try:
                curr_detail = ds.get_brand_detail_json(report_id, brand_name)
                prev_detail = ds.get_brand_detail_json(prev_report['id'], brand_name)
                if not curr_detail:
                    continue
                import json
                import pandas as pd

                def _json_to_df(data, columns):
                    rows = json.loads(data) if isinstance(data, str) else (data or [])
                    if not rows:
                        return pd.DataFrame(columns=columns)
                    return pd.DataFrame(rows)

                curr_stores_df = _json_to_df(curr_detail.get('top_stores_json', '[]'), ['Store', 'Total Revenue'])
                prev_stores_df = _json_to_df(prev_detail.get('top_stores_json', '[]') if prev_detail else '[]', ['Store', 'Total Revenue'])

                # Build minimal DataFrames compatible with calculate_churn
                curr_df = curr_stores_df.rename(columns={'Store': 'Particulars', 'Total Revenue': 'Sales_Value'}) \
                    if not curr_stores_df.empty else pd.DataFrame(columns=['Particulars', 'Sales_Value'])
                prev_df = prev_stores_df.rename(columns={'Store': 'Particulars', 'Total Revenue': 'Sales_Value'}) \
                    if not prev_stores_df.empty else pd.DataFrame(columns=['Particulars', 'Sales_Value'])

                churn = calculate_churn(curr_df, prev_df)
                ds.save_store_churn(report_id, brand_name, churn)
            except Exception:
                pass
    except Exception:
        pass


def _queue_catalog_candidates(df, source_filename=None, report_id=None):
    """Register unknown brands/SKUs discovered in an imported dataset."""
    try:
        ds.sync_catalog_from_history()
        return ds.register_catalog_candidates(
            df,
            source_filename=source_filename,
            source_report_id=report_id,
        )
    except Exception:
        return {'queued_brands': 0, 'queued_skus': 0}


def _coerce_int(value):
    try:
        if value in (None, '', False):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _current_copilot_context(overrides=None):
    overrides = overrides or {}
    view_args = request.view_args or {}
    endpoint = str(overrides.get('endpoint') or request.endpoint or '')
    path = str(overrides.get('path') or request.path or '')
    report_id = (
        _coerce_int(overrides.get('report_id')) or
        request.args.get('report_id', type=int) or
        _coerce_int(view_args.get('report_id'))
    )
    batch_id = (
        _coerce_int(overrides.get('batch_id')) or
        request.args.get('batch_id', type=int)
    )
    brand_name = (
        str(
            overrides.get('brand_name') or
            overrides.get('brand') or
            request.args.get('brand') or
            view_args.get('brand_name') or
            ''
        ).strip() or None
    )
    retailer_code = (
        str(
            overrides.get('retailer_code') or
            view_args.get('retailer_code') or
            ''
        ).strip() or None
    )
    token = str(overrides.get('token') or view_args.get('token') or '').strip() or None
    if token and not brand_name:
        brand_info = ds.get_brand_by_token(token)
        brand_name = brand_info['brand_name'] if brand_info else None

    report = ds.get_report(report_id) if report_id else None
    if not report and endpoint in {
        'dashboard', 'brands', 'brand_detail', 'copilot_dashboard',
        'agent_actions_page', 'activity_intelligence', 'forecasting',
    }:
        report = ds.get_latest_report()
        report_id = report['id'] if report else None
    elif not report:
        report = ds.get_latest_report()

    page_label_map = {
        'index': 'Home',
        'dashboard': 'Dashboard',
        'brands': 'Brand Partners',
        'brand_detail': 'Brand 360',
        'brand_portal': 'Brand Portal',
        'retailers': 'Retailers',
        'retailer_detail': 'Retailer Intelligence',
        'retailer_report_page': 'Retailer Report',
        'retailer_group_detail': 'Retailer Groups',
        'retailer_group_report_page': 'Retailer Group Report',
        'forecasting': 'Forecasting',
        'activity_intelligence': 'Activity',
        'store_360': 'Store 360',
        'catalog': 'Catalog',
        'catalog_brand_detail': 'Catalog Brand',
        'copilot_dashboard': 'Copilot',
        'agent_actions_page': 'Agent Actions',
        'database_page': 'Database',
    }

    return {
        'endpoint': endpoint,
        'path': path,
        'page_label': page_label_map.get(endpoint, 'Workspace'),
        'report_id': report_id or (report['id'] if report else None),
        'report_label': report['month_label'] if report else None,
        'report_type': report.get('report_type') if report else None,
        'batch_id': batch_id,
        'brand_name': brand_name,
        'retailer_code': retailer_code,
    }


def _agent_action_link(action):
    subject_type = action.get('subject_type')
    subject_key = action.get('subject_key')
    report_id = action.get('report_id')
    if subject_type == 'brand' and subject_key:
        if report_id:
            return url_for('brand_detail', brand_name=subject_key, report_id=report_id)
        return url_for('brand_detail', brand_name=subject_key)
    if subject_type in {'store', 'retailer'} and subject_key:
        return url_for('retailer_detail', retailer_code=subject_key)
    if subject_type == 'catalog':
        return url_for('catalog')
    if subject_type == 'activity_batch':
        return url_for('activity_intelligence', report_id=report_id) if report_id else url_for('activity_intelligence')
    if report_id:
        return url_for('dashboard', report_id=report_id)
    return url_for('copilot_dashboard')


def _serialize_agent_action_for_ui(action):
    return {
        'id': action.get('id'),
        'title': action.get('title'),
        'reason': action.get('reason'),
        'priority': action.get('priority'),
        'status': action.get('status'),
        'agent_type': action.get('agent_type'),
        'subject_type': action.get('subject_type'),
        'subject_key': action.get('subject_key'),
        'report_id': action.get('report_id'),
        'payload': action.get('proposed_payload') or {},
        'link': _agent_action_link(action),
    }


def _action_priority_rank(priority):
    return {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}.get(str(priority or '').lower(), 4)


def _copilot_actions_for_context(context, limit=6):
    pending_actions = ds.list_agent_actions(status='pending', limit=120)
    brand_name = context.get('brand_name')
    retailer_code = context.get('retailer_code')
    report_id = context.get('report_id')
    endpoint = context.get('endpoint')
    scored = []
    for action in pending_actions:
        score = 0
        subject_key = str(action.get('subject_key') or '')
        if report_id and action.get('report_id') == report_id:
            score += 2
        if brand_name and ds.analytics_brand_name(subject_key) == ds.analytics_brand_name(brand_name):
            score += 6
        if retailer_code and subject_key.lower() == retailer_code.lower():
            score += 6
        if endpoint == 'activity_intelligence' and action.get('agent_type') == 'Activity Agent':
            score += 3
        if endpoint == 'forecasting' and action.get('agent_type') == 'Forecast Agent':
            score += 3
        if endpoint in {'catalog', 'catalog_brand_detail'} and action.get('agent_type') == 'Data Quality Agent':
            score += 3
        if endpoint in {'dashboard', 'brands'} and action.get('report_id') == report_id:
            score += 1
        if score <= 0 and not brand_name and not retailer_code and action.get('report_id') == report_id:
            score = 1
        scored.append((score, _action_priority_rank(action.get('priority')), action))

    scored.sort(key=lambda item: (-(item[0]), item[1], -(item[2].get('id') or 0)))
    selected = [item[2] for item in scored if item[0] > 0][:limit]
    if len(selected) < limit:
        used_ids = {item.get('id') for item in selected}
        for _, _, action in scored:
            if action.get('id') in used_ids:
                continue
            selected.append(action)
            if len(selected) >= limit:
                break
    return [_serialize_agent_action_for_ui(action) for action in selected[:limit]]


def _copilot_prompt_suggestions(context):
    brand_name = context.get('brand_name')
    retailer_code = context.get('retailer_code')
    endpoint = context.get('endpoint')
    if brand_name:
        return [
            f"Summarize {brand_name} right now.",
            f"What should we do next for {brand_name}?",
            f"Explain {brand_name} growth outlook.",
            f"Draft a partner update for {brand_name}.",
        ]
    if retailer_code:
        return [
            "Summarize this store's latest issues.",
            "Which brands have the strongest opportunity here?",
            "What should the field team do next at this store?",
            "List unresolved store risks.",
        ]
    if endpoint == 'forecasting':
        return [
            "Which brands need attention first from forecasting?",
            "Explain the weakest forecast confidence items.",
            "Which brands are growing by supermarket count?",
            "What do the next actions look like from this forecast set?",
        ]
    if endpoint == 'activity_intelligence':
        return [
            "Summarize the latest activity issues.",
            "Which field issues need immediate action?",
            "Which stores show the strongest opportunities?",
            "What should the field team prioritize next?",
        ]
    return [
        "What needs attention this week?",
        "What changed across the business?",
        "Which brands are at risk right now?",
        "Draft the next best actions for the team.",
    ]


def _copilot_welcome_text(context):
    brand_name = context.get('brand_name')
    retailer_code = context.get('retailer_code')
    if brand_name:
        return f"Watching {brand_name} across sales, forecast, activity, and pending actions."
    if retailer_code:
        return f"Tracking store {retailer_code} across visits, issues, and brand mentions."
    return "Connected to reports, forecasts, activity, alerts, catalog decisions, and agent memory."


def _refresh_copilot_state(report_id=None, reason='system_update', brand_name=None,
                           retailer_code=None, batch_id=None, source='system'):
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    if report:
        report_id = report['id']
        build_default_agent_actions(ds, report)
    activity_summary = ds.get_activity_summary(
        batch_id=batch_id,
        report_id=report_id,
        brand_name=brand_name,
    )
    pending_count = len(ds.list_agent_actions(status='pending', limit=100))
    memory_parts = [f"State refresh triggered by {reason}."]
    if report:
        memory_parts.append(
            f"Current report is {report['month_label']} ({report.get('report_type') or 'custom'})."
        )
    if brand_name and report_id:
        kpis = ds.get_brand_kpis_single(report_id, brand_name)
        if kpis:
            memory_parts.append(
                f"{brand_name}: revenue ₦{float(kpis.get('total_revenue', 0)):,.2f}, "
                f"{int(kpis.get('num_stores', 0))} stores, repeat {float(kpis.get('repeat_pct', 0)):.1f}%."
            )
    if retailer_code:
        store_summary = ds.get_store_activity_summary(retailer_code)
        if store_summary.get('store'):
            memory_parts.append(
                f"Store {store_summary['store'].get('retailer_name') or retailer_code}: "
                f"{int(store_summary.get('visit_count', 0))} visits, "
                f"{int(store_summary.get('issue_count', 0))} issues."
            )
    if activity_summary.get('totals'):
        memory_parts.append(
            f"Activity snapshot: {int(activity_summary['totals'].get('visits', 0))} visits, "
            f"{int(activity_summary['totals'].get('issues', 0))} issues, "
            f"{pending_count} pending actions."
        )
    scope_type = 'system'
    scope_key = 'global'
    if brand_name:
        scope_type = 'brand'
        scope_key = ds.analytics_brand_name(brand_name)
    elif retailer_code:
        scope_type = 'store'
        scope_key = retailer_code
    elif batch_id:
        scope_type = 'activity_batch'
        scope_key = str(batch_id)
    elif report_id:
        scope_type = 'report'
        scope_key = str(report_id)
    ds.save_agent_memory(
        scope_type=scope_type,
        scope_key=scope_key,
        memory_text=' '.join(memory_parts),
        memory_kind='state_refresh',
        confidence=0.72,
        source=source,
    )


@app.context_processor
def inject_copilot_context():
    return {'copilot_context': _current_copilot_context()}


@app.template_global()
def clean_url_for(endpoint, **values):
    clean_values = {
        key: value for key, value in values.items()
        if value not in (None, "", [], {}, ())
    }
    return url_for(endpoint, **clean_values)


def _review_catalog_item(item_id, action, note=None, target_brand_id=None, target_sku_id=None):
    """Approve, map, or reject one catalog review item."""
    item = ds.get_catalog_review_item(item_id)
    if not item:
        raise ValueError('Catalog review item not found.')
    if item.get('status') != 'pending':
        raise ValueError('Catalog review item has already been processed.')

    action = (action or '').strip().lower()
    if action == 'reject':
        ds.update_catalog_review_status(item_id, 'rejected', note)
        return item

    if item['entity_type'] == 'brand':
        suggested_brand_name = item.get('suggested_match_name') or item.get('brand_candidate')
        if action == 'approve':
            brand = ds.ensure_brand_master(item['canonical_candidate'] or item['raw_name'])
            if not brand:
                raise ValueError('Could not create brand master record.')
            ds.add_brand_alias(brand['id'], item['raw_name'])
            ds.update_catalog_review_status(item_id, 'approved', note)
            return brand
        if action in ('map', 'accept_alias'):
            if not target_brand_id and suggested_brand_name:
                suggested = ds.resolve_brand_master(suggested_brand_name)
                target_brand_id = suggested['id'] if suggested else None
            brand = ds.get_brand_master(int(target_brand_id or 0))
            if not brand:
                raise ValueError('Select a valid existing brand.')
            ds.add_brand_alias(brand['id'], item['raw_name'])
            ds.update_catalog_review_status(item_id, 'merged', note or f"Mapped to {brand['canonical_name']}")
            return brand
        if action == 'keep_separate':
            brand = ds.ensure_brand_master(item['canonical_candidate'] or item['raw_name'])
            if not brand:
                raise ValueError('Could not create brand master record.')
            if suggested_brand_name:
                ds.mark_catalog_distinct('brand', item['raw_name'], suggested_brand_name, note=note)
            ds.update_catalog_review_status(
                item_id,
                'distinct',
                note or (f"Kept separate from {suggested_brand_name}" if suggested_brand_name else 'Kept separate')
            )
            return brand
        raise ValueError('Unsupported brand review action.')

    if item['entity_type'] == 'sku':
        brand = None
        if target_brand_id:
            brand = ds.get_brand_master(int(target_brand_id))
        if not brand and item.get('brand_candidate'):
            brand = ds.resolve_brand_master(item['brand_candidate'])
        if not brand:
            raise ValueError('Select or approve the parent brand first.')

        suggested_sku_name = item.get('suggested_match_name')
        if action == 'approve':
            sku = ds.ensure_sku_master(brand['id'], item['canonical_candidate'] or item['raw_name'])
            if not sku:
                raise ValueError('Could not create SKU master record.')
            ds.add_sku_alias(sku['id'], brand['id'], item['raw_name'])
            ds.update_catalog_review_status(item_id, 'approved', note)
            return sku
        if action in ('map', 'accept_alias'):
            if not target_sku_id and suggested_sku_name:
                suggested = ds.resolve_sku_master(brand['id'], suggested_sku_name)
                target_sku_id = suggested['id'] if suggested else None
            sku = ds.get_sku_master(int(target_sku_id or 0))
            if not sku or sku['brand_id'] != brand['id']:
                raise ValueError('Select a valid existing SKU for that brand.')
            ds.add_sku_alias(sku['id'], brand['id'], item['raw_name'])
            ds.update_catalog_review_status(item_id, 'merged', note or f"Mapped to {sku['sku_name']}")
            return sku
        if action == 'keep_separate':
            sku = ds.ensure_sku_master(brand['id'], item['canonical_candidate'] or item['raw_name'])
            if not sku:
                raise ValueError('Could not create SKU master record.')
            if suggested_sku_name:
                ds.mark_catalog_distinct('sku', item['raw_name'], suggested_sku_name, brand_scope=brand['canonical_name'], note=note)
            ds.update_catalog_review_status(
                item_id,
                'distinct',
                note or (f"Kept separate from {suggested_sku_name}" if suggested_sku_name else 'Kept separate')
            )
            return sku
        raise ValueError('Unsupported SKU review action.')

    raise ValueError('Unsupported catalog entity type.')


def _merge_depletions_by_brand(brand_kpis_rows):
    """Collapse obvious duplicate brand variants into one depletion summary."""
    merged = {}
    for row in brand_kpis_rows or []:
        brand_name = ds.analytics_brand_name(row.get('brand_name', ''))
        depletion = stock_depletion_date(row)
        existing = merged.get(brand_name)
        if not existing or depletion.get('days_remaining', 10**9) < existing.get('days_remaining', 10**9):
            merged[brand_name] = depletion
    return merged


def _try_backfill_brand_detail_json(report: dict, brand_name: str) -> dict | None:
    """
    Rebuild detailed KPI data for older seeded reports when brand_detail_json is missing.
    Uses the bundled combined workbook if available, then persists the detail JSON.
    """
    existing = ds.get_brand_detail_json(report['id'], brand_name)
    if existing:
        return existing

    hist_path = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')
    if not os.path.isfile(hist_path):
        return None

    try:
        combined_df = load_and_clean(hist_path)
        df_filtered = filter_by_date(combined_df, report['start_date'], report['end_date'])
        brand_data = split_by_brand(df_filtered)
        brand_df = brand_data.get(brand_name)
        if brand_df is None or brand_df.empty:
            return None
        detail_kpis = calculate_kpis(brand_df)
        ds.save_brand_detail_json(report['id'], brand_name, detail_kpis)
        return ds.get_brand_detail_json(report['id'], brand_name)
    except Exception:
        return None


def _deployment_metadata():
    """Expose minimal runtime metadata for deployment verification."""
    return {
        'git_branch': os.environ.get('RAILWAY_GIT_BRANCH'),
        'git_commit_sha': os.environ.get('RAILWAY_GIT_COMMIT_SHA'),
        'git_commit_message': os.environ.get('RAILWAY_GIT_COMMIT_MESSAGE'),
        'railway_environment': os.environ.get('RAILWAY_ENVIRONMENT_NAME'),
        'railway_project_id': os.environ.get('RAILWAY_PROJECT_ID'),
        'railway_service_id': os.environ.get('RAILWAY_SERVICE_ID'),
    }


@app.route('/__version')
def version():
    """Lightweight runtime endpoint to confirm what Railway actually deployed."""
    return jsonify({
        'service': 'dala-reporting',
        'timestamp_utc': datetime.utcnow().isoformat() + 'Z',
        **_deployment_metadata(),
    })


# ── Import page ──────────────────────────────────────────────────────────────

@app.route('/import')
def import_page():
    alert_count = ds.get_unacknowledged_count()
    latest      = ds.get_latest_report()
    known_brands = set(ds.get_all_brands_in_db())
    return render_template('portal/import.html',
                           alert_count=alert_count,
                           latest=latest,
                           known_brands=list(known_brands),
                           reports=ds.get_all_reports())


# ── Preview API ───────────────────────────────────────────────────────────────

@app.route('/api/preview', methods=['POST'])
def api_preview():
    """
    Instant file analysis — parses the Tally export and returns a full data
    profile without generating any reports. Returns JSON in < 3 seconds.
    """
    file = request.files.get('tally_file')
    if not file or file.filename == '':
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400

    try:
        file_bytes = io.BytesIO(file.read())
        df = load_and_clean(file_bytes)
    except Exception as exc:
        return jsonify({'success': False, 'error': str(exc)}), 422

    # ── Basic stats ───────────────────────────────────────────────────────────
    row_count = len(df)
    
    # For large historical files, limit preview to most recent 3 months
    # to avoid timeouts while still giving useful preview
    is_large_file = row_count > 10000
    if is_large_file:
        date_max_all = df['Date'].max()
        date_min_preview = date_max_all - pd.Timedelta(days=90)
        preview_df = df[df['Date'] >= date_min_preview].copy()
    else:
        preview_df = df
    
    date_min  = preview_df['Date'].min()
    date_max  = preview_df['Date'].max()
    file_size_kb = round(file.content_length / 1024, 1) if file.content_length else 0

    # ── Vch type breakdown ────────────────────────────────────────────────────
    vch_counts = preview_df['Vch Type'].value_counts().to_dict()
    sales_df = preview_df[preview_df['Vch Type'] == 'Sales']

    # ── Brand analysis ────────────────────────────────────────────────────────
    known_brands = set(ds.get_all_brands_in_db())
    brand_stats  = []

    brand_revenue = sales_df.groupby('Brand Partner')['Sales_Value'].sum().sort_values(ascending=False)
    brand_qty     = sales_df.groupby('Brand Partner')['Quantity'].sum()
    brand_stores  = sales_df.groupby('Brand Partner')['Particulars'].nunique()
    brand_skus    = sales_df.groupby('Brand Partner')['SKUs'].nunique()
    brand_days    = sales_df.groupby('Brand Partner')['Date'].nunique()

    max_rev = float(brand_revenue.max()) if not brand_revenue.empty else 1

    for brand in brand_revenue.index:
        rev = float(brand_revenue[brand])
        brand_stats.append({
            'name':       brand,
            'revenue':    rev,
            'revenue_pct': round(rev / max_rev * 100, 1),
            'qty':        float(brand_qty.get(brand, 0)),
            'stores':     int(brand_stores.get(brand, 0)),
            'skus':       int(brand_skus.get(brand, 0)),
            'days':       int(brand_days.get(brand, 0)),
            'is_new':     brand not in known_brands,
        })

    # Brands in df but with no Sales rows
    all_brands_in_file = set(df['Brand Partner'].unique())
    active_brands      = set(brand_revenue.index)
    zero_sales_brands  = sorted(all_brands_in_file - active_brands)

    # Brands in DB but absent from this file
    missing_brands = sorted(known_brands - all_brands_in_file) if known_brands else []

    # ── Date coverage ─────────────────────────────────────────────────────────
    if not sales_df.empty:
        date_range = pd.date_range(date_min, date_max)
        days_with_data = set(sales_df['Date'].dt.normalize().unique())
        coverage = []
        for d in date_range:
            daily_rev = float(sales_df[sales_df['Date'].dt.normalize() == d]['Sales_Value'].sum())
            coverage.append({
                'date':    d.strftime('%b %d'),
                'weekday': d.strftime('%a'),
                'has_data': d in days_with_data,
                'revenue': daily_rev,
            })
        days_total  = len(date_range)
        days_active = len(days_with_data)
        coverage_pct = round(days_active / days_total * 100) if days_total else 0
    else:
        coverage, days_total, days_active, coverage_pct = [], 0, 0, 0

    # ── Top stores + products ─────────────────────────────────────────────────
    top_stores = (
        sales_df.groupby('Particulars')['Sales_Value'].sum()
        .sort_values(ascending=False).head(10)
    )
    top_stores_list = [{'name': k, 'revenue': round(float(v), 0)}
                       for k, v in top_stores.items()]

    top_products = (
        sales_df.groupby('SKUs')['Sales_Value'].sum()
        .sort_values(ascending=False).head(10)
    )
    top_products_list = [{'name': k, 'revenue': round(float(v), 0)}
                         for k, v in top_products.items()]

    # ── Data quality score ────────────────────────────────────────────────────
    issues  = []
    penalty = 0

    null_dates = int(df['Date'].isna().sum())
    if null_dates:
        issues.append({'level': 'warning', 'msg': f'{null_dates} rows with unparseable dates were dropped.'})
        penalty += 5

    zero_val_rows = int((sales_df['Sales_Value'] == 0).sum())
    if zero_val_rows > 0:
        issues.append({'level': 'info', 'msg': f'{zero_val_rows} Sales rows have ₦0 value — may be returns or errors.'})
        penalty += min(zero_val_rows // 10, 10)

    if zero_sales_brands:
        issues.append({'level': 'info', 'msg': f'{len(zero_sales_brands)} brand(s) have no Sales transactions: {", ".join(zero_sales_brands[:3])}{"..." if len(zero_sales_brands) > 3 else ""}.'})
        penalty += 3

    if missing_brands:
        issues.append({'level': 'warning', 'msg': f'{len(missing_brands)} brand(s) from your history are absent in this file: {", ".join(missing_brands[:3])}{"..." if len(missing_brands) > 3 else ""}.'})
        penalty += len(missing_brands) * 2

    new_brands = [b for b in brand_stats if b['is_new']]
    if new_brands:
        issues.append({'level': 'info', 'msg': f'{len(new_brands)} new brand(s) detected (not in history): {", ".join(b["name"] for b in new_brands[:3])}.'})

    if coverage_pct < 60:
        issues.append({'level': 'warning', 'msg': f'Only {coverage_pct}% of days in the date range have sales data — check for data gaps.'})
        penalty += 10

    # Spike detection: any single day revenue > 3x the mean daily
    if not sales_df.empty:
        daily_rev = sales_df.groupby('Date')['Sales_Value'].sum()
        mean_d = daily_rev.mean()
        std_d  = daily_rev.std()
        if std_d and std_d > 0:
            spikes = daily_rev[daily_rev > mean_d + 3 * std_d]
            if not spikes.empty:
                issues.append({'level': 'warning', 'msg': f'Revenue spike detected on {spikes.index[0].strftime("%b %d")} (₦{spikes.iloc[0]:,.0f}) — {round(spikes.iloc[0]/mean_d,1)}x the daily average.'})

    quality_score = max(100 - penalty, 40)

    # ── Inventory data presence ───────────────────────────────────────────────
    has_inventory  = 'Available Inventory' in vch_counts
    has_pickup     = 'Inventory Pickup by Dala' in vch_counts
    has_supply     = 'Inventory Supplied by Brands' in vch_counts

    # ── vs last report comparison ─────────────────────────────────────────────
    vs_last = None
    latest  = ds.get_latest_report()
    if latest:
        total_rev_preview = float(sales_df['Sales_Value'].sum())
        rev_change = round((total_rev_preview - latest['total_revenue']) / max(latest['total_revenue'], 1) * 100, 1)
        vs_last = {
            'month_label': latest['month_label'],
            'prev_revenue': latest['total_revenue'],
            'new_revenue':  total_rev_preview,
            'rev_change':   rev_change,
            'prev_brands':  latest['brand_count'],
            'new_brands':   len(brand_stats),
            'brand_change': len(brand_stats) - latest['brand_count'],
        }

    return jsonify({
        'success':        True,
        'file_name':      file.filename,
        'row_count':      row_count,
        'date_min':       date_min.strftime('%Y-%m-%d'),
        'date_max':       date_max.strftime('%Y-%m-%d'),
        'date_min_fmt':   date_min.strftime('%d %b %Y'),
        'date_max_fmt':   date_max.strftime('%d %b %Y'),
        'brand_count':    len(brand_stats),
        'active_brand_count': len(brand_stats),
        'workbook_brand_count': len(all_brands_in_file),
        'brand_stats':    brand_stats,
        'zero_sales_brands': zero_sales_brands,
        'missing_brands': missing_brands,
        'new_brand_count': len(new_brands),
        'total_stores':   int(sales_df['Particulars'].nunique()),
        'total_skus':     int(sales_df['SKUs'].nunique()),
        'total_revenue':  float(sales_df['Sales_Value'].sum()),
        'total_qty':      float(sales_df['Quantity'].sum()),
        'vch_counts':     vch_counts,
        'has_inventory':  has_inventory,
        'has_pickup':     has_pickup,
        'has_supply':     has_supply,
        'coverage':       coverage,
        'days_total':     days_total,
        'days_active':    days_active,
        'coverage_pct':   coverage_pct,
        'top_stores':     top_stores_list,
        'top_products':   top_products_list,
        'quality_score':  quality_score,
        'issues':         issues,
        'vs_last':        vs_last,
    })


# ── Async generate ────────────────────────────────────────────────────────────

def _run_generation(job_id, file_bytes, start_date, end_date, selected_brands, filename, report_type=None, options=None):
    """Queued background import. Persists report data first and can defer heavy extras."""
    options = options or {}

    def _upd(**kw):
        ds.update_job(job_id, **kw)

    def _append_error(message):
        job_obj = ds.get_job(job_id) or {}
        errs = job_obj.get('errors', [])
        errs.append(message)
        _upd(errors=errs)

    try:
        _upd(status='running', current_brand='Validating workbook...', progress=2)
        df_all = load_and_clean(io.BytesIO(file_bytes))
        df_ranged = filter_by_date(df_all, start_date, end_date)
        if df_ranged.empty:
            raise ValueError(f'No data between {start_date} and {end_date}.')

        workbook_brands = sorted(set(df_ranged['Brand Partner'].dropna().astype(str).str.strip()))
        brand_data = split_by_brand(df_ranged)
        active_brands = list(brand_data.keys())
        zero_sales_brands = sorted(set(workbook_brands) - set(active_brands))
        filtered_out_brands = []
        if selected_brands:
            filtered_out_brands = sorted(set(active_brands) - set(selected_brands))
            brand_data = {b: df for b, df in brand_data.items() if b in selected_brands}
        if not brand_data:
            raise ValueError('No sales data found after filtering the selected brands.')

        brands = list(brand_data.keys())
        catalog_df = (
            pd.concat(list(brand_data.values()), ignore_index=True)
            if brand_data else df_ranged.head(0).copy()
        )
        _upd(total=len(brands), current_brand='Calculating KPIs...', progress=8)

        all_kpis = {}
        for brand_name in brands:
            all_kpis[brand_name] = calculate_kpis(brand_data[brand_name])

        total_portfolio_revenue = sum(k['total_revenue'] for k in all_kpis.values())
        portfolio_avg_revenue = total_portfolio_revenue / max(len(brands), 1)
        for brand_name in brands:
            all_kpis[brand_name]['perf_score'] = calculate_perf_score(all_kpis[brand_name], portfolio_avg_revenue)

        all_stores = set()
        for kpis in all_kpis.values():
            if kpis.get('top_stores') is not None and not kpis['top_stores'].empty:
                all_stores.update(kpis['top_stores']['Store'].tolist())

        total_qty_sum = sum(k['total_qty'] for k in all_kpis.values())
        _upd(current_brand='Saving report to history...', progress=18)
        existing_report = ds.get_report_by_date_range(start_date, end_date)
        brand_payloads = []
        for brand_name in brands:
            kpis = all_kpis[brand_name]
            perf_score = kpis.get('perf_score', {})
            share = round(kpis['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)
            brand_payloads.append({
                'brand_name': brand_name,
                'kpis': kpis,
                'perf_score': perf_score,
                'portfolio_share_pct': share,
            })
        report_id = ds.persist_report_bundle(
            start_date=start_date,
            end_date=end_date,
            xls_filename=filename,
            total_revenue=total_portfolio_revenue,
            total_qty=total_qty_sum,
            total_stores=len(all_stores),
            report_type=report_type,
            brand_payloads=brand_payloads,
            workbook_brand_count=len(workbook_brands),
            active_brand_count=len(active_brands),
            selected_brand_count=len(brands),
            zero_sales_brands=zero_sales_brands,
            filtered_out_brands=filtered_out_brands,
            replace_report_id=existing_report['id'] if existing_report else None,
        )
        report_meta = ds.get_report(report_id) or {}
        import_audit = ds.get_report_import_audit(report_id) or {}
        source_meta = _archive_report_source(file_bytes, report_id, filename)
        ds.save_agent_memory(
            scope_type='report',
            scope_key=str(report_id),
            memory_text=(
                f"Imported {filename} for {start_date} to {end_date}. "
                f"Workbook brands: {import_audit.get('workbook_brand_count', len(workbook_brands))}; "
                f"active brands: {import_audit.get('active_brand_count', len(active_brands))}; "
                f"persisted brands: {import_audit.get('persisted_brand_count', len(brands))}."
            ),
            memory_kind='import_audit',
            confidence=0.98,
            source='report_import',
            memory_layer='workspace',
            subject_type='report',
            subject_key=str(report_id),
            related_report_id=report_id,
            tags=['import', 'audit', 'source-workbook'],
            metadata={
                'file_name': filename,
                'start_date': start_date,
                'end_date': end_date,
                'workbook_brand_count': import_audit.get('workbook_brand_count', len(workbook_brands)),
                'active_brand_count': import_audit.get('active_brand_count', len(active_brands)),
                'persisted_brand_count': import_audit.get('persisted_brand_count', len(brands)),
                'zero_sales_brands': import_audit.get('zero_sales_brands', zero_sales_brands),
                'source_file_path': source_meta['path'],
                'source_file_size_bytes': source_meta['size_bytes'],
                'source_file_sha1': source_meta['sha1'],
            },
        )
        _upd(report_id=report_id, progress=24)
        _queue_catalog_candidates(catalog_df, source_filename=filename, report_id=report_id)

        _upd(current_brand='Persisting brand metrics...', progress=28)
        defer_secondary_refresh = options.get('import_mode') == 'fast'
        if not defer_secondary_refresh:
            for brand_name in brands:
                kpis = all_kpis[brand_name]
                history = ds.get_brand_history(brand_name, limit=3)
                check_and_save_alerts(report_id, brand_name, kpis, portfolio_avg_revenue, history[1:], ds)

        if not defer_secondary_refresh:
            run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
            _compute_and_save_churn(report_id)

            _upd(current_brand='Building reorder trends...', progress=48)
            for brand_name in brands:
                _attach_reorder_trend(
                    brand_name=brand_name,
                    kpis=all_kpis[brand_name],
                    report_type=report_type or report_meta.get('report_type'),
                    cutoff_date=end_date,
                )
        else:
            _upd(current_brand='Scheduling secondary refresh...', progress=48)

        for brand_name in brands:
            ds.get_or_create_token(brand_name)

        ai_narratives = {}
        if options.get('generate_narratives') and gemini_available():
            _upd(current_brand='Generating narratives...', progress=58)
            for brand_name in brands:
                try:
                    history = ds.get_brand_history(brand_name, limit=6)
                    text, _ = generate_brand_narrative(brand_name, all_kpis[brand_name], history, portfolio_avg_revenue)
                    if text:
                        ai_narratives[brand_name] = text
                        ds.save_narrative(report_id, brand_name, text)
                except Exception:
                    pass
            try:
                portfolio_text = generate_portfolio_narrative(all_kpis, report_meta)
                if portfolio_text:
                    ds.save_narrative(report_id, '__portfolio__', portfolio_text)
            except Exception:
                pass

        sheets_urls = {}
        if options.get('sync_sheets'):
            try:
                from modules.sheets import push_brand_to_sheets, sheets_available
                if sheets_available():
                    _upd(current_brand='Syncing external sheets...', progress=66)
                    for brand_name in brands:
                        try:
                            url = push_brand_to_sheets(
                                brand_name=brand_name,
                                brand_df=brand_data[brand_name],
                                start_date=start_date,
                                end_date=end_date,
                            )
                            if url:
                                sheets_urls[brand_name] = url
                                ds.log_activity('sheets_sync', 'Auto-synced to Google Sheets', brand_name, report_id)
                        except Exception:
                            pass
            except Exception:
                pass

        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        month_tag = start_dt.strftime('%b%Y')
        brands_done = [
            {'brand': brand_name, 'pdf': False, 'html': False, 'error': None, 'deferred': not options.get('generate_artifacts')}
            for brand_name in brands
        ]
        portfolio_filename = None

        def try_generate_pdf(brand_name, pdf_path, kpis, max_retries=2):
            for attempt in range(max_retries):
                try:
                    report_context = _brand_report_context(brand_name, cutoff_date=end_date)
                    coach = _report_summary_payload(brand_name, kpis, ds.get_report(report_id))
                    activity_report = _build_brand_activity_report(brand_name, report_id, kpis)
                    result_path = generate_pdf_html(
                        output_path=pdf_path,
                        brand_name=brand_name,
                        kpis=kpis,
                        start_date=start_date,
                        end_date=end_date,
                        portfolio_avg_revenue=portfolio_avg_revenue,
                        total_portfolio_revenue=total_portfolio_revenue,
                        report_type=report_type or report_meta.get('report_type'),
                        month_label=report_meta.get('month_label'),
                        ai_narrative=ai_narratives.get(brand_name),
                        sheets_url=sheets_urls.get(brand_name),
                        growth_outlook=report_context.get('growth_outlook'),
                        gmv_window=report_context.get('gmv_window'),
                        coach=coach,
                        activity_report=activity_report,
                    )
                    is_pdf = result_path.endswith('.pdf') and os.path.exists(result_path)
                    is_html = result_path.endswith('.html') and os.path.exists(result_path)
                    if is_pdf:
                        return True, True, None
                    if is_html:
                        return True, False, None
                    return False, False, 'File not created'
                except Exception as exc:
                    if attempt >= max_retries - 1:
                        return False, False, str(exc)
            return False, False, 'Max retries exceeded'

        def try_generate_html(brand_name, html_path, kpis, max_retries=2):
            for attempt in range(max_retries):
                try:
                    report_context = _brand_report_context(brand_name, cutoff_date=end_date)
                    coach = _report_summary_payload(brand_name, kpis, ds.get_report(report_id))
                    activity_report = _build_brand_activity_report(brand_name, report_id, kpis)
                    generate_html(
                        output_path=html_path,
                        brand_name=brand_name,
                        kpis=kpis,
                        start_date=start_date,
                        end_date=end_date,
                        portfolio_avg_revenue=portfolio_avg_revenue,
                        total_portfolio_revenue=total_portfolio_revenue,
                        report_type=report_type or report_meta.get('report_type'),
                        month_label=report_meta.get('month_label'),
                        growth_outlook=report_context.get('growth_outlook'),
                        gmv_window=report_context.get('gmv_window'),
                        coach=coach,
                        activity_report=activity_report,
                    )
                    return True, None
                except Exception as exc:
                    if attempt >= max_retries - 1:
                        return False, str(exc)
            return False, 'Max retries exceeded'

        if options.get('generate_artifacts'):
            _upd(current_brand='Generating downloadable reports...', progress=72)
            brands_done = []
            for index, brand_name in enumerate(brands):
                _upd(current_brand=f'Building report for {brand_name}')
                safe = _safe_name(brand_name)
                pdf_path = os.path.join(PDF_DIR, f"{safe}_Report_{month_tag}.pdf")
                html_path = os.path.join(HTML_DIR, f"{safe}_Report_{month_tag}.html")
                kpis = all_kpis[brand_name]
                brand_result = {'brand': brand_name, 'pdf': False, 'html': False, 'error': None, 'deferred': False}

                pdf_success, is_actually_pdf, pdf_error = try_generate_pdf(brand_name, pdf_path, kpis)
                brand_result['pdf'] = pdf_success and is_actually_pdf
                brand_result['html'] = pdf_success
                if pdf_error:
                    brand_result['error'] = f'PDF: {pdf_error}'

                if not pdf_success:
                    html_success, html_error = try_generate_html(brand_name, html_path, kpis)
                    brand_result['html'] = html_success
                    if html_error:
                        brand_result['error'] = (brand_result['error'] or '') + f' HTML: {html_error}'

                if (brand_result['pdf'] or brand_result['html']) and brand_result['error']:
                    brand_result['error'] = None

                brands_done.append(brand_result)
                stage_progress = 72 + round(((index + 1) / max(len(brands), 1)) * 18)
                _upd(brands_done=brands_done, progress=min(stage_progress, 90))

            _upd(current_brand='Preparing portfolio dashboard', progress=92)
            portfolio_path = os.path.join(HTML_DIR, f"PORTFOLIO_Dashboard_{month_tag}.html")
            try:
                generate_portfolio_html(
                    output_path=portfolio_path,
                    all_brand_kpis=all_kpis,
                    brand_data_raw=brand_data,
                    start_date=start_date,
                    end_date=end_date,
                    total_portfolio_revenue=total_portfolio_revenue,
                )
                portfolio_filename = os.path.basename(portfolio_path)
            except Exception as exc:
                _append_error(f'Portfolio: {exc}')
        else:
            _upd(brands_done=brands_done, current_brand='Reports saved. Downloads will render on demand.', progress=88)

        background_refresh_queued = False
        if defer_secondary_refresh:
            GENERATION_EXECUTOR.submit(
                _run_post_import_maintenance,
                report_id,
                brands,
                all_kpis,
                report_type,
                end_date,
                portfolio_avg_revenue,
                filename,
                options.get('refresh_copilot', True),
                options.get('launch_coach', True),
            )
            background_refresh_queued = True

        _upd(current_brand='Refreshing intelligence...', progress=94)
        if options.get('refresh_copilot') and not defer_secondary_refresh:
            _refresh_copilot_state(
                report_id=report_id,
                reason=f'report_generation:{filename}',
                source='report_generation',
            )

        coach_job_id = None
        if options.get('launch_coach') and not defer_secondary_refresh:
            coach_job_id = _launch_coach_refresh_job(
                mode='current',
                report_id=report_id,
                include_pairs=True,
                source='report_generation',
            )

        result_options = dict(options)
        result_options['background_refresh_queued'] = background_refresh_queued
        result_payload = _generation_result_payload(
            report_id=report_id,
            report_meta=report_meta,
            brands=brands,
            portfolio_filename=portfolio_filename,
            brands_done=brands_done,
            options=result_options,
            coach_job_id=coach_job_id,
            import_audit=import_audit,
        )
        _upd(
            progress=100,
            status='done',
            current_brand=None,
            portfolio_file=portfolio_filename,
            brands_done=brands_done,
            result_json=result_payload,
        )
    except Exception as exc:
        _upd(status='error', error_msg=str(exc), current_brand=None)


def _submit_generation_job(file_bytes, start_date, end_date, selected_brands, filename, report_type=None, options=None):
    options = options or {}
    job_id = uuid.uuid4().hex
    ds.create_job(job_id)
    ds.update_job(
        job_id,
        status='queued',
        progress=0,
        total=len(selected_brands) if selected_brands else 0,
        current_brand='Queued for processing...',
        result_json={'import_mode': options.get('import_mode', 'full')},
    )
    GENERATION_EXECUTOR.submit(
        _run_generation,
        job_id,
        file_bytes,
        start_date,
        end_date,
        selected_brands,
        filename,
        report_type,
        options,
    )
    return job_id


@app.route('/api/generate_async', methods=['POST'])
def generate_async():
    """Start background generation. Returns {job_id} immediately."""
    file = request.files.get('tally_file')
    start_date = request.form.get('start_date', '').strip()
    end_date = request.form.get('end_date', '').strip()
    selected_raw = request.form.get('selected_brands', '')
    selected_brands = [b.strip() for b in selected_raw.split(',') if b.strip()] if selected_raw else []
    report_type = request.form.get('report_type', '').strip() or None
    options = _parse_generation_options(request.form)

    if not file or not start_date or not end_date:
        return jsonify({'success': False, 'error': 'Missing file or dates'}), 400

    file_bytes = file.read()
    job_id = _submit_generation_job(
        file_bytes=file_bytes,
        start_date=start_date,
        end_date=end_date,
        selected_brands=selected_brands,
        filename=file.filename,
        report_type=report_type,
        options=options,
    )
    return jsonify({'success': True, 'job_id': job_id, 'import_mode': options.get('import_mode', 'full')})


@app.route('/api/generation_status/<job_id>')
def generation_status(job_id):
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    payload = dict(job)
    result_json = payload.get('result_json') or {}
    if isinstance(result_json, dict):
        payload.update(result_json)
    return jsonify({'success': True, **payload})


# ── Trends / Forecasting Dashboard ────────────────────────────────────────────

def _convert_json_native(obj):
    if isinstance(obj, dict):
        return {k: _convert_json_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_json_native(item) for item in obj]
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    return obj


_TRENDS_DF_CACHE = {
    'mtime': None,
    'df': None,
}


def _load_trends_dataframe():
    hist_path = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')
    if not os.path.exists(hist_path):
        raise FileNotFoundError('Historical data not available')
    current_mtime = os.path.getmtime(hist_path)
    cached_df = _TRENDS_DF_CACHE.get('df')
    if cached_df is not None and _TRENDS_DF_CACHE.get('mtime') == current_mtime:
        return cached_df

    df = pd.read_excel(hist_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Brand Partner Canonical'] = (
        df['Brand Partner']
        .fillna('')
        .astype(str)
        .map(canonicalize_brand_name)
    )
    _TRENDS_DF_CACHE['mtime'] = current_mtime
    _TRENDS_DF_CACHE['df'] = df
    return df


def _build_trends_map_payload(scoped_df, year, month, selected_store='', google_maps_key='', live_geocode=False, top_n=12):
    from modules.geocoding import geocode_stores_batch

    map_candidates = get_repeat_purchase_map_data(scoped_df, year, month, top_n=top_n)
    if selected_store:
        map_candidates = [row for row in map_candidates if row.get('store_name') == selected_store]

    coords = {}
    if map_candidates:
        coords = geocode_stores_batch(
            [row['store_name'] for row in map_candidates],
            google_maps_key,
            cache_only=not live_geocode,
        )

    mapped_rows = []
    for row in map_candidates:
        item = dict(row)
        if item.get('store_name') in coords:
            item['latitude'], item['longitude'] = coords[item['store_name']]
        if item.get('latitude') and item.get('longitude'):
            mapped_rows.append(item)

    preview_rows = [
        {
            'store_name': str(row.get('store_name', '')),
            'visit_count': int(row.get('visit_count') or 0),
            'repeat_category': str(row.get('repeat_category') or ''),
            'total_revenue': float(row.get('total_revenue') or 0),
        }
        for row in map_candidates[:6]
    ]

    return {
        'candidates': map_candidates,
        'mapped_rows': mapped_rows,
        'preview_rows': preview_rows,
        'candidate_count': len(map_candidates),
        'mapped_count': len(mapped_rows),
        'missing_count': max(0, len(map_candidates) - len(mapped_rows)),
    }


@app.route('/trends')
def trends():
    """
    Historical trend analysis dashboard with MoM growth,
    color themes, and insights.
    """
    def render_trends_error(message, available_months=None, year=None, month=None,
                            scope='portfolio', scope_badge='Portfolio View',
                            scope_title='Portfolio Trend Analysis',
                            scope_subtitle='General view across all brand partners, stores, products, and reorder behaviour.',
                            reorder_kicker='Portfolio Repeat Purchase Trend',
                            available_brands=None, selected_brand='',
                            available_stores=None, selected_store='',
                            top_stores_title='🏪 Top Selling Supermarkets by Value',
                            secondary_panel_title='🗺️ Top Repeat Purchase Stores Map',
                            detail_title='📍 Store Repeat Purchase Details',
                            detail_rows=None, detail_empty_message='No repeat purchase detail available for this view yet.',
                            focus_note=''):
        now = datetime.now()
        selected_year = year or now.year
        selected_month = month or now.month

        return render_template(
            'portal/trends.html',
            error=message,
            metrics=None,
            insights={'working': [], 'not_working': [], 'next_steps': []},
            historical=[],
            historical_json='[]',
            top_stores=[],
            top_stores_json='[]',
            top_products=[],
            top_products_json='[]',
            map_data=[],
            map_preview_rows=[],
            map_api_url='',
            map_candidate_count=0,
            map_mapped_count=0,
            map_missing_count=0,
            scope=scope,
            scope_badge=scope_badge,
            scope_title=scope_title,
            scope_subtitle=scope_subtitle,
            reorder_kicker=reorder_kicker,
            available_brands=available_brands or [],
            selected_brand=selected_brand,
            available_stores=available_stores or [],
            selected_store=selected_store,
            top_stores_title=top_stores_title,
            secondary_panel_title=secondary_panel_title,
            detail_title=detail_title,
            detail_rows=detail_rows or [],
            detail_empty_message=detail_empty_message,
            focus_note=focus_note,
            color_scheme=get_color_scheme_for_month(selected_year, selected_month),
            available_months=available_months or [],
            current_year=selected_year,
            current_month=selected_month,
            alert_count=ds.get_unacknowledged_count(),
        )
    
    # Load historical data
    try:
        df = _load_trends_dataframe()
    except FileNotFoundError:
        return render_trends_error("Historical data not available")
    except Exception as e:
        return render_trends_error(f"Error loading data: {e}")

    scope = (request.args.get('scope') or 'portfolio').strip().lower()
    if scope not in {'portfolio', 'brand'}:
        scope = 'portfolio'

    sales_catalog_df = df[df['Vch Type'] == 'Sales'].copy()
    available_brands = sorted(
        {
            brand.strip()
            for brand in sales_catalog_df['Brand Partner Canonical'].dropna().tolist()
            if str(brand).strip()
        }
    )
    selected_brand = (request.args.get('brand') or '').strip()
    if scope == 'brand' and available_brands:
        if selected_brand not in available_brands:
            selected_brand = available_brands[0]
    else:
        selected_brand = ''

    if scope == 'brand' and selected_brand:
        scoped_df = df[df['Brand Partner Canonical'] == selected_brand].copy()
        scope_badge = 'Brand Partner View'
        scope_title = f'{selected_brand} Trend Analysis'
        scope_subtitle = (
            'Brand-partner view across all stores, products, sales momentum, and reorder behaviour.'
        )
        reorder_kicker = f'{selected_brand} Repeat Purchase Trend'
        top_stores_title = f'🏪 Top Selling Supermarkets for {selected_brand}'
        secondary_panel_title = f'🗺️ Repeat Purchase Stores Map for {selected_brand}'
        detail_title = f'📍 Store Repeat Purchase Details for {selected_brand}'
    else:
        scoped_df = df.copy()
        scope_badge = 'Portfolio View'
        scope_title = 'Portfolio Trend Analysis'
        scope_subtitle = (
            'General view across all brand partners, stores, products, and reorder behaviour.'
        )
        reorder_kicker = 'Portfolio Repeat Purchase Trend'
        top_stores_title = '🏪 Top Selling Supermarkets by Value'
        secondary_panel_title = '🗺️ Top Repeat Purchase Stores Map'
        detail_title = '📍 Store Repeat Purchase Details'

    scoped_sales_df = scoped_df[scoped_df['Vch Type'] == 'Sales'].copy()
    if scoped_sales_df.empty:
        return render_trends_error(
            'No sales data is available for this view yet.',
            scope=scope,
            scope_badge=scope_badge,
            scope_title=scope_title,
            scope_subtitle=scope_subtitle,
            reorder_kicker=reorder_kicker,
            available_brands=available_brands,
            selected_brand=selected_brand,
            top_stores_title=top_stores_title,
            secondary_panel_title=secondary_panel_title,
            detail_title=detail_title,
        )

    scoped_sales_df['YearMonth'] = scoped_sales_df['Date'].dt.to_period('M')
    available_ym = sorted(scoped_sales_df['YearMonth'].unique())
    available_months = [
        {'year': ym.year, 'month': ym.month, 'label': ym.strftime('%b %Y')}
        for ym in available_ym
    ]

    month_param = request.args.get('month', '')
    selected_period = None
    if month_param:
        try:
            req_year, req_month = map(int, month_param.split('-'))
            selected_period = pd.Period(year=req_year, month=req_month, freq='M')
        except Exception:
            selected_period = None
    if selected_period not in set(available_ym):
        selected_period = available_ym[-1]
    year, month = selected_period.year, selected_period.month

    historical = get_portfolio_monthly_trend(scoped_df)
    metrics = next(
        (row.copy() for row in historical if row.get('year') == year and row.get('month') == month),
        None,
    )
    if not metrics:
        metrics = get_monthly_metrics(scoped_df, year, month)
    if not metrics:
        return render_trends_error(
            'No data for selected month',
            available_months=available_months,
            year=year,
            month=month,
            scope=scope,
            scope_badge=scope_badge,
            scope_title=scope_title,
            scope_subtitle=scope_subtitle,
            reorder_kicker=reorder_kicker,
            available_brands=available_brands,
            selected_brand=selected_brand,
            top_stores_title=top_stores_title,
            secondary_panel_title=secondary_panel_title,
            detail_title=detail_title,
        )

    insights = generate_insights(historical)
    color_scheme = get_color_scheme_for_month(year, month)

    sales_df = scoped_sales_df[
        (scoped_sales_df['Date'].dt.year == year)
        & (scoped_sales_df['Date'].dt.month == month)
    ].copy()

    store_repeat_df = get_store_repeat_analysis(scoped_df, year, month)
    available_stores = sorted(
        {
            str(store).strip()
            for store in store_repeat_df.get('store_name', pd.Series(dtype=str)).dropna().tolist()
            if str(store).strip()
        }
    )
    selected_store = (request.args.get('store') or '').strip()
    if selected_store not in available_stores:
        selected_store = ''

    top_stores = []
    if not sales_df.empty:
        store_revenue = (
            sales_df.groupby('Particulars')['Sales_Value']
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        top_stores = [
            {'name': str(name), 'revenue': float(rev)}
            for name, rev in store_revenue.items()
        ]

    top_products = []
    if not sales_df.empty:
        product_revenue = (
            sales_df.groupby('SKUs')['Sales_Value']
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        top_products = [
            {'name': str(name), 'revenue': float(rev)}
            for name, rev in product_revenue.items()
        ]

    google_maps_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
    map_context = _build_trends_map_payload(
        scoped_df,
        year,
        month,
        selected_store=selected_store,
        google_maps_key=google_maps_key,
        live_geocode=False,
        top_n=20,
    )
    map_data = map_context['candidates']
    map_preview_rows = map_context['preview_rows']
    map_api_url = clean_url_for(
        'api_trends_map',
        scope=scope,
        brand=selected_brand if scope == 'brand' else None,
        month=f'{year:04d}-{month:02d}',
        store=selected_store or None,
    )

    detail_source = store_repeat_df.copy()
    if selected_store:
        detail_source = detail_source[detail_source['store_name'] == selected_store]
    else:
        detail_source = detail_source.head(15)

    detail_rows = [
        {
            'label': str(row['store_name']),
            'meta': f"{int(row['visit_count'])} visits",
            'value': float(row['total_revenue']),
            'badge': str(row['repeat_category']),
            'badge_class': (
                'badge-red' if row['repeat_category'] == 'Frequent (5+)'
                else 'badge-amber' if row['repeat_category'] == 'Regular (2-4)'
                else 'badge-muted'
            ),
        }
        for _, row in detail_source.iterrows()
    ]

    if selected_store:
        if scope == 'brand' and selected_brand:
            focus_note = (
                f"Store focus is set to {selected_store} inside {selected_brand}. "
                'The repeat-store map and detail list are narrowed to that supermarket.'
            )
            detail_empty_message = (
                f'{selected_store} did not record repeat purchases for {selected_brand} in this period.'
            )
        else:
            focus_note = (
                f"Store focus is set to {selected_store}. "
                'The repeat-store map and detail list are narrowed to that supermarket.'
            )
            detail_empty_message = (
                f'{selected_store} did not record repeat purchases in this period.'
            )
    elif scope == 'brand' and selected_brand:
        focus_note = (
            f'Showing all stores that bought {selected_brand} in the selected month. '
            'Use Store Focus when you want to zoom into one supermarket.'
        )
        detail_empty_message = f'No repeat-purchase store detail is available for {selected_brand} in this period.'
    else:
        focus_note = (
            'Showing the general portfolio view across all brand partners. '
            'Switch to a brand partner or apply Store Focus to drill down.'
        )
        detail_empty_message = 'No repeat-purchase store detail is available for this period.'
    
    historical_native = _convert_json_native(historical)
    top_stores_native = _convert_json_native(top_stores)
    top_products_native = _convert_json_native(top_products)
    
    return render_template('portal/trends.html',
                           metrics=metrics,
                           insights=insights,
                           historical=historical,
                           historical_json=json.dumps(historical_native),
                            top_stores=top_stores,
                            top_stores_json=json.dumps(top_stores_native),
                            top_products=top_products,
                            top_products_json=json.dumps(top_products_native),
                            map_data=map_data,
                            map_preview_rows=map_preview_rows,
                            map_api_url=map_api_url,
                            map_candidate_count=map_context['candidate_count'],
                            map_mapped_count=map_context['mapped_count'],
                            map_missing_count=map_context['missing_count'],
                            scope=scope,
                            scope_badge=scope_badge,
                            scope_title=scope_title,
                            scope_subtitle=scope_subtitle,
                            reorder_kicker=reorder_kicker,
                            available_brands=available_brands,
                            selected_brand=selected_brand,
                            available_stores=available_stores,
                            selected_store=selected_store,
                            top_stores_title=top_stores_title,
                            secondary_panel_title=secondary_panel_title,
                            detail_title=detail_title,
                            detail_rows=detail_rows,
                            detail_empty_message=detail_empty_message,
                            focus_note=focus_note,
                            color_scheme=color_scheme,
                            available_months=available_months,
                            current_year=year,
                            current_month=month,
                           alert_count=ds.get_unacknowledged_count())


@app.route('/api/trends/map')
def api_trends_map():
    try:
        df = _load_trends_dataframe()
    except FileNotFoundError:
        return jsonify({'success': False, 'message': 'Historical data not available', 'map_data': []}), 404
    except Exception as exc:
        return jsonify({'success': False, 'message': f'Error loading data: {exc}', 'map_data': []}), 500

    scope = (request.args.get('scope') or 'portfolio').strip().lower()
    if scope not in {'portfolio', 'brand'}:
        scope = 'portfolio'

    sales_catalog_df = df[df['Vch Type'] == 'Sales'].copy()
    available_brands = sorted(
        {
            brand.strip()
            for brand in sales_catalog_df['Brand Partner Canonical'].dropna().tolist()
            if str(brand).strip()
        }
    )
    selected_brand = (request.args.get('brand') or '').strip()
    if scope == 'brand':
        if selected_brand not in available_brands:
            return jsonify({'success': False, 'message': 'Unknown brand for trends map', 'map_data': []}), 404
        scoped_df = df[df['Brand Partner Canonical'] == selected_brand].copy()
    else:
        scoped_df = df.copy()
        selected_brand = ''

    scoped_sales_df = scoped_df[scoped_df['Vch Type'] == 'Sales'].copy()
    if scoped_sales_df.empty:
        return jsonify({'success': True, 'map_data': [], 'candidate_count': 0, 'mapped_count': 0, 'missing_count': 0})

    scoped_sales_df['YearMonth'] = scoped_sales_df['Date'].dt.to_period('M')
    available_ym = sorted(scoped_sales_df['YearMonth'].unique())
    month_param = request.args.get('month', '')
    selected_period = None
    if month_param:
        try:
            req_year, req_month = map(int, month_param.split('-'))
            selected_period = pd.Period(year=req_year, month=req_month, freq='M')
        except Exception:
            selected_period = None
    if selected_period not in set(available_ym):
        selected_period = available_ym[-1]
    year, month = selected_period.year, selected_period.month

    store_repeat_df = get_store_repeat_analysis(scoped_df, year, month)
    available_stores = sorted(
        {
            str(store).strip()
            for store in store_repeat_df.get('store_name', pd.Series(dtype=str)).dropna().tolist()
            if str(store).strip()
        }
    )
    selected_store = (request.args.get('store') or '').strip()
    if selected_store not in available_stores:
        selected_store = ''

    map_context = _build_trends_map_payload(
        scoped_df,
        year,
        month,
        selected_store=selected_store,
        google_maps_key=os.environ.get('GOOGLE_MAPS_API_KEY', ''),
        live_geocode=True,
        top_n=20,
    )

    return jsonify({
        'success': True,
        'scope': scope,
        'brand': selected_brand,
        'month': f'{year:04d}-{month:02d}',
        'map_data': _convert_json_native(map_context['mapped_rows']),
        'preview_rows': _convert_json_native(map_context['preview_rows']),
        'candidate_count': map_context['candidate_count'],
        'mapped_count': map_context['mapped_count'],
        'missing_count': map_context['missing_count'],
    })


# ── How It Works (Public Documentation) ───────────────────────────────────────

@app.route('/how-it-works')
def how_it_works():
    """Public documentation page explaining the system."""
    return render_template('portal/docs.html', 
                           alert_count=ds.get_unacknowledged_count())


# ── Admin: Retailer Count ─────────────────────────────────────────────────────

@app.route('/admin/retailers')
def admin_retailers():
    return redirect(url_for('retailers'))


@app.route('/retailers')
def retailers():
    alert_count = ds.get_unacknowledged_count()
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    if not coach_history_available():
        return render_template(
            'portal/retailers.html',
            alert_count=alert_count,
            retailer_rows=[],
            featured={},
            period_label='',
            available_months=[],
            selected_month=month_value or '',
            report=ds.get_report(report_id) if report_id else ds.get_latest_report(),
            reports=ds.get_all_reports(),
            error='Historical sales workbook is not available.',
        )

    dataset = build_retailer_index(ds, report_id=report_id, month_value=month_value)
    retailer_rows = _decorate_retailer_rows(dataset.get('rows', []))
    group_dataset = build_retailer_group_index(ds, report_id=report_id, month_value=month_value)
    retailer_group_rows = _decorate_retailer_group_rows(group_dataset.get('rows', []))
    summary = _build_retailer_summary(retailer_rows)
    group_summary = _build_retailer_group_summary(retailer_group_rows)
    chart_rows = retailer_rows[:10]
    table_rows = retailer_rows[:40]
    group_rows = retailer_group_rows[:16]

    featured = {
        'risk': [row for row in retailer_rows if (row.get('revenue_mom') or 0) <= -12][:5],
        'growth': [row for row in retailer_rows if (row.get('revenue_mom') or 0) >= 15][:5],
        'opportunity': sorted(
            retailer_rows,
            key=lambda item: (-(item.get('active_brands') or 0), item.get('repeat_rate') or 0, -(item.get('total_revenue') or 0))
        )[:5],
    }
    selected_month = month_value or ''
    return render_template(
        'portal/retailers.html',
        alert_count=alert_count,
        retailer_rows=table_rows,
        retailer_group_rows=group_rows,
        chart_rows=chart_rows,
        full_row_count=len(retailer_rows),
        summary=summary,
        group_summary=group_summary,
        featured=featured,
        period_label=dataset.get('period_label'),
        period_start=dataset.get('period_start'),
        period_end=dataset.get('period_end'),
        available_months=dataset.get('available_months', []),
        group_choices=group_dataset.get('group_choices', []),
        selected_month=selected_month,
        report=ds.get_report(dataset.get('report_id')) if dataset.get('report_id') else (ds.get_report(report_id) if report_id else ds.get_latest_report()),
        reports=ds.get_all_reports(),
        error=None,
    )


# ── Login / Logout ────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    error = None
    if not ADMIN_PASSWORD:
        # No password configured — no auth needed, redirect to home
        return redirect(url_for('index'))
    if request.method == 'POST':
        pw = request.form.get('password', '')
        if pw == ADMIN_PASSWORD:
            session['admin_authenticated'] = True
            next_url = request.args.get('next') or url_for('dashboard')
            return redirect(next_url)
        error = 'Incorrect password.'
    return render_template('portal/login.html', error=error)


@app.route('/logout')
def logout():
    session.pop('admin_authenticated', None)
    return redirect(url_for('login_page'))


# ── Home / Upload ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    latest  = ds.get_latest_report()
    reports = ds.get_all_reports()
    alert_count = ds.get_unacknowledged_count()
    return render_template('portal/home.html',
                           latest=latest,
                           reports=reports,
                           alert_count=alert_count)


# ── Generate ──────────────────────────────────────────────────────────────────

@app.route('/generate', methods=['POST'])
def generate():
    file       = request.files.get('tally_file')
    start_date = request.form.get('start_date', '').strip()
    end_date   = request.form.get('end_date', '').strip()
    report_type = request.form.get('report_type', '').strip() or None

    if not file or file.filename == '':
        return jsonify({'success': False, 'error': 'No file uploaded.'}), 400
    if not start_date or not end_date:
        return jsonify({'success': False, 'error': 'Please select both dates.'}), 400
    if start_date > end_date:
        return jsonify({'success': False, 'error': 'Start date must be before end date.'}), 400

    try:
        file_bytes = io.BytesIO(file.read())
        df_all    = load_and_clean(file_bytes)
        df_ranged = filter_by_date(df_all, start_date, end_date)
    except Exception as exc:
        return jsonify({'success': False, 'error': f'File processing failed: {exc}'}), 422

    if df_ranged.empty:
        return jsonify({'success': False,
                        'error': f'No data between {start_date} and {end_date}.'}), 422

    workbook_brands = sorted(set(df_ranged['Brand Partner'].dropna().astype(str).str.strip()))
    brand_data = split_by_brand(df_ranged)
    if not brand_data:
        return jsonify({'success': False, 'error': 'No sales data found.'}), 422

    brands = list(brand_data.keys())
    active_brands = list(brands)
    zero_sales_brands = sorted(set(workbook_brands) - set(active_brands))
    catalog_df = (
        pd.concat(list(brand_data.values()), ignore_index=True)
        if brand_data else df_ranged.head(0).copy()
    )

    # Portfolio aggregates
    all_kpis = {}
    for b in brands:
        all_kpis[b] = calculate_kpis(brand_data[b])

    total_portfolio_revenue = sum(k['total_revenue'] for k in all_kpis.values())
    portfolio_avg_revenue   = total_portfolio_revenue / max(len(brands), 1)

    # Save report to DB — upsert: reuse existing row for same date range
    all_stores = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    total_qty_sum = sum(k['total_qty'] for k in all_kpis.values())
    existing_report = ds.get_report_by_date_range(start_date, end_date)
    brand_payloads = []
    for brand_name in brands:
        kpis = all_kpis[brand_name]
        perf = calculate_perf_score(kpis, portfolio_avg_revenue)
        kpis['perf_score'] = perf
        portfolio_share = round(kpis['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)
        brand_payloads.append({
            'brand_name': brand_name,
            'kpis': kpis,
            'perf_score': perf,
            'portfolio_share_pct': portfolio_share,
        })
    report_id = ds.persist_report_bundle(
        start_date=start_date,
        end_date=end_date,
        xls_filename=file.filename,
        total_revenue=total_portfolio_revenue,
        total_qty=total_qty_sum,
        total_stores=len(all_stores),
        report_type=report_type,
        brand_payloads=brand_payloads,
        workbook_brand_count=len(workbook_brands),
        active_brand_count=len(active_brands),
        selected_brand_count=len(brands),
        zero_sales_brands=zero_sales_brands,
        filtered_out_brands=[],
        replace_report_id=existing_report['id'] if existing_report else None,
    )
    _queue_catalog_candidates(catalog_df, source_filename=file.filename, report_id=report_id)

    # Generate files
    ok_pdf = ok_html = 0
    errors = []
    month_tag = datetime.strptime(start_date, '%Y-%m-%d').strftime('%b%Y')
    report_meta = ds.get_report(report_id) or {}

    for brand_name in brands:
        kpis = all_kpis[brand_name]
        safe = _safe_name(brand_name)
        pdf_path  = os.path.join(PDF_DIR,  f"{safe}_Report_{month_tag}.pdf")
        html_path = os.path.join(HTML_DIR, f"{safe}_Report_{month_tag}.html")

        # Alerts
        history = ds.get_brand_history(brand_name, limit=3)
        check_and_save_alerts(report_id, brand_name, kpis,
                              portfolio_avg_revenue, history[1:], ds)
        report_context = _brand_report_context(brand_name, cutoff_date=end_date)
        coach = _report_summary_payload(brand_name, kpis, ds.get_report(report_id))

        # PDF
        try:
            generate_pdf_html(
                output_path=pdf_path,
                brand_name=brand_name,
                kpis=kpis,
                start_date=start_date,
                end_date=end_date,
                portfolio_avg_revenue=portfolio_avg_revenue,
                total_portfolio_revenue=total_portfolio_revenue,
                report_type=report_type or report_meta.get('report_type'),
                month_label=report_meta.get('month_label'),
                growth_outlook=report_context.get('growth_outlook'),
                gmv_window=report_context.get('gmv_window'),
                coach=coach,
                activity_report=_build_brand_activity_report(brand_name, report_id, kpis),
            )
            ok_pdf += 1
        except Exception as e:
            errors.append({'brand': brand_name, 'type': 'PDF', 'error': str(e)})

        # HTML
        try:
            generate_html(
                output_path=html_path,
                brand_name=brand_name,
                kpis=kpis,
                start_date=start_date,
                end_date=end_date,
                portfolio_avg_revenue=portfolio_avg_revenue,
                total_portfolio_revenue=total_portfolio_revenue,
                report_type=report_type or report_meta.get('report_type'),
                month_label=report_meta.get('month_label'),
                growth_outlook=report_context.get('growth_outlook'),
                gmv_window=report_context.get('gmv_window'),
                coach=coach,
                activity_report=_build_brand_activity_report(brand_name, report_id, kpis),
            )
            ok_html += 1
        except Exception as e:
            errors.append({'brand': brand_name, 'type': 'HTML', 'error': str(e)})

    # Portfolio dashboard
    portfolio_path = os.path.join(HTML_DIR, f"PORTFOLIO_Dashboard_{month_tag}.html")
    try:
        generate_portfolio_html(
            output_path=portfolio_path,
            all_brand_kpis=all_kpis,
            brand_data_raw=brand_data,
            start_date=start_date,
            end_date=end_date,
            total_portfolio_revenue=total_portfolio_revenue,
        )
    except Exception as e:
        errors.append({'brand': 'PORTFOLIO', 'type': 'Dashboard', 'error': str(e)})

    # Portfolio-level alerts
    run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
    _refresh_copilot_state(
        report_id=report_id,
        reason=f'sync_generate:{file.filename}',
        source='report_generation',
    )
    coach_job_id = _launch_coach_refresh_job(
        mode='current',
        report_id=report_id,
        include_pairs=True,
        source='sync_generate',
    )

    import_audit = ds.get_report_import_audit(report_id) or {}
    return jsonify({
        'success':    True,
        'report_id':  report_id,
        'pdf_count':  ok_pdf,
        'html_count': ok_html,
        'brands':     len(brands),
        'workbook_brand_count': int(import_audit.get('workbook_brand_count') or len(workbook_brands)),
        'active_brand_count': int(import_audit.get('active_brand_count') or len(active_brands)),
        'persisted_brand_count': int(import_audit.get('persisted_brand_count') or len(ds.get_all_brand_kpis(report_id))),
        'zero_sales_brands': list(import_audit.get('zero_sales_brands') or zero_sales_brands),
        'missing_brands': list(import_audit.get('missing_brands') or []),
        'import_warnings': list(import_audit.get('warnings') or []),
        'errors':     errors,
        'portfolio_dashboard': f"/download/html/{os.path.basename(portfolio_path)}",
        'coach_job_id': coach_job_id,
    })


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route('/dashboard')
def dashboard():
    latest = ds.get_latest_report()
    if not latest:
        return redirect(url_for('index'))
    report_id = request.args.get('report_id', latest['id'], type=int)
    report    = ds.get_report(report_id)
    brand_kpis = ds.get_all_brand_kpis(report_id)
    alerts    = ds.get_alerts(report_id)
    alert_count = ds.get_unacknowledged_count()

    # Portfolio dashboard HTML link
    start_dt = datetime.strptime(report['start_date'], '%Y-%m-%d')
    month_tag = start_dt.strftime('%b%Y')
    portfolio_file = f"PORTFOLIO_Dashboard_{month_tag}.html"
    portfolio_exists = os.path.isfile(os.path.join(HTML_DIR, portfolio_file))

    # YoY comparison
    yoy_prev_report, yoy_prev_revenue, yoy_prev_qty, yoy_prev_stores = ds.get_portfolio_yoy(report_id)
    yoy_brand_kpis = ds.get_yoy_kpis(report_id)
    yoy_rev_pct = None
    if yoy_prev_revenue and report and report.get('total_revenue'):
        yoy_rev_pct = round((report['total_revenue'] - yoy_prev_revenue) / yoy_prev_revenue * 100, 1)

    # Churn summary
    churn_summary = ds.get_churn_summary(report_id)
    total_churned = sum(v.get('churned', 0) for v in churn_summary.values())

    # Revenue concentration risk
    conc_warning = None
    if brand_kpis and report and report.get('total_revenue', 0) > 0:
        top2_rev = sum(b['total_revenue'] for b in brand_kpis[:2])
        top2_pct = round(top2_rev / report['total_revenue'] * 100, 1)
        if top2_pct >= 70:
            top2_names = ' & '.join(b['brand_name'] for b in brand_kpis[:2])
            conc_warning = f"Top 2 brands ({top2_names}) account for {top2_pct}% of portfolio revenue."

    return render_template('portal/dashboard.html',
                           report=report,
                           brand_kpis=brand_kpis,
                           reports=ds.get_all_reports(),
                           alerts=alerts[:5],
                           alert_count=alert_count,
                           portfolio_file=portfolio_file if portfolio_exists else None,
                           yoy_prev_report=yoy_prev_report,
                           yoy_prev_revenue=yoy_prev_revenue,
                           yoy_prev_qty=yoy_prev_qty,
                           yoy_prev_stores=yoy_prev_stores,
                           yoy_rev_pct=yoy_rev_pct,
                           yoy_brand_kpis=yoy_brand_kpis,
                           conc_warning=conc_warning,
                           churn_summary=churn_summary,
                           total_churned=total_churned)


# ── Brands list ───────────────────────────────────────────────────────────────

@app.route('/brands')
def brands():
    latest = ds.get_latest_report()
    report_id = request.args.get('report_id', latest['id'] if latest else None, type=int)
    brand_kpis = ds.get_all_brand_kpis(report_id) if report_id else []
    alert_count = ds.get_unacknowledged_count()
    all_brand_names = ds.get_all_brands_in_db()
    tokens = {t['brand_name']: t for t in ds.get_all_tokens()}

    # Attach forecast label to each brand — one batch query instead of N per-brand queries
    all_trends = ds.get_all_brands_revenue_trends(limit=6)
    forecasts = {}
    for b in all_brand_names:
        hist_oldest_first = list(reversed(all_trends.get(b, [])))
        label = growth_label(hist_oldest_first)
        forecasts[b] = {'growth_label': label, 'growth_color': growth_color(label)}

    return render_template('portal/brands.html',
                           brand_kpis=brand_kpis,
                           report_id=report_id,
                           report=ds.get_report(report_id) if report_id else None,
                           reports=ds.get_all_reports(),
                           tokens=tokens,
                           forecasts=forecasts,
                           alert_count=alert_count)


# ── Brand detail ──────────────────────────────────────────────────────────────

@app.route('/brand/<path:brand_name>')
@app.route('/brand-360/<path:brand_name>')
def brand_detail(brand_name):
    latest = ds.get_latest_report()
    report_id = request.args.get('report_id', latest['id'] if latest else None, type=int)
    alert_count = ds.get_unacknowledged_count()
    focus = (request.args.get('focus') or 'overview').strip().lower()
    if focus not in {'overview', 'forecast', 'activity'}:
        focus = 'overview'

    # Latest KPIs for this brand
    kpis = ds.get_brand_kpis_single(report_id, brand_name) if report_id else None
    report = ds.get_report(report_id) if report_id else None

    # Historical trend
    history = ds.get_brand_history(brand_name, limit=12)
    forecast_history = list(reversed(ds.get_brand_history(brand_name, limit=36)))
    hist_oldest = list(reversed(history))

    # Forecast
    canonical_brand = ds.analytics_brand_name(brand_name)
    forecast = build_brand_forecasts({brand_name: forecast_history}).get(canonical_brand, {})

    # Daily sales
    daily = ds.get_daily_sales(report_id, brand_name) if report_id else []

    # Depletion
    depletion = stock_depletion_date(
        {'total_closing_stock': kpis.get('closing_stock_total', 0) if kpis else 0,
         'stock_days_cover':    kpis.get('stock_days_cover', 0) if kpis else 0},
    ) if kpis else {}

    # Token
    token = ds.get_or_create_token(brand_name)

    # YoY for this brand
    yoy_brand_kpis = ds.get_yoy_kpis(report_id) if report_id else {}
    yoy_kpi = yoy_brand_kpis.get(brand_name)
    yoy_rev_pct = None
    if yoy_kpi and yoy_kpi.get('total_revenue') and kpis and kpis.get('total_revenue'):
        yoy_rev_pct = round((kpis['total_revenue'] - yoy_kpi['total_revenue']) / yoy_kpi['total_revenue'] * 100, 1)

    # Store churn
    churn_data = ds.get_store_churn(report_id, brand_name) if report_id else []
    churned_stores = [c for c in churn_data if c['churn_type'] == 'churned']
    new_stores = [c for c in churn_data if c['churn_type'] == 'new']
    brand_activity = ds.get_activity_brand_summary(brand_name, limit=8)

    trend_month_value = None
    if report and report.get('start_date'):
        try:
            trend_month_value = datetime.strptime(report['start_date'], '%Y-%m-%d').strftime('%Y-%m')
        except Exception:
            trend_month_value = None
    brand_coach_data = build_brand_coach_data(
        ds,
        brand_name,
        report_id=report_id,
        month_value=trend_month_value,
    ) if coach_history_available() else {'snapshot': {}, 'top_risks': [], 'top_opportunities': [], 'activity_mismatches': []}
    coach = _coach_summary_for_snapshot(brand_coach_data.get('snapshot'), persist=True) if brand_coach_data.get('snapshot') else {'summary': {'headline': 'Coach summary unavailable', 'summary': 'No historical context is available for this brand yet.', 'recommended_actions': [], 'used_gemini': False}, 'signals': []}

    return render_template('portal/brand_detail.html',
                           brand_name=brand_name,
                           kpis=kpis,
                           history=history,
                           hist_oldest=hist_oldest,
                           forecast=forecast,
                           daily=daily,
                           depletion=depletion,
                           token=token,
                           reports=ds.get_all_reports(),
                           report_id=report_id,
                           report=report,
                           alert_count=alert_count,
                           yoy_kpi=yoy_kpi,
                           yoy_rev_pct=yoy_rev_pct,
                           churned_stores=churned_stores,
                           new_stores=new_stores,
                           brand_activity=brand_activity,
                           coach=coach,
                           brand_coach_data=brand_coach_data,
                           focus=focus,
                           trend_month_value=trend_month_value)


# ── History ───────────────────────────────────────────────────────────────────

@app.route('/history')
def history():
    reports = ds.get_all_reports()
    alert_count = ds.get_unacknowledged_count()
    trend = ds.get_portfolio_monthly_trend(limit=24)
    return render_template('portal/history.html',
                           reports=reports,
                           trend=trend,
                           alert_count=alert_count)


# ── Compare ───────────────────────────────────────────────────────────────────

@app.route('/compare')
def compare():
    latest = ds.get_latest_report()
    report_id = request.args.get('report_id', latest['id'] if latest else None, type=int)
    brand_a = request.args.get('brand_a', '')
    brand_b = request.args.get('brand_b', '')
    alert_count = ds.get_unacknowledged_count()
    all_brands = ds.get_all_brands_in_db()
    comparison = {}
    if brand_a and brand_b and report_id:
        comparison = ds.compare_brands(brand_a, brand_b, report_id)
    return render_template('portal/compare.html',
                           all_brands=all_brands,
                           brand_a=brand_a,
                           brand_b=brand_b,
                           report_id=report_id,
                           comparison=comparison,
                           reports=ds.get_all_reports(),
                           alert_count=alert_count)


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.route('/alerts')
def alerts_view():
    alerts = ds.get_alerts()
    alert_count = ds.get_unacknowledged_count()
    return render_template('portal/alerts.html',
                           alerts=alerts,
                           alert_count=alert_count,
                           reports=ds.get_all_reports())


# ── Settings ──────────────────────────────────────────────────────────────────

@app.route('/settings')
def settings():
    ds.sync_catalog_from_history()
    tokens = ds.get_all_tokens()
    alert_count = ds.get_unacknowledged_count()
    smtp_ok  = bool(os.environ.get('SMTP_USER') and os.environ.get('SMTP_PASSWORD'))
    twilio_ok = bool(os.environ.get('TWILIO_ACCOUNT_SID') and os.environ.get('TWILIO_AUTH_TOKEN'))
    catalog_summary = ds.get_catalog_summary()
    pending_reviews = ds.get_catalog_review_queue(limit=8)
    return render_template('portal/settings.html',
                           tokens=tokens,
                           catalog_summary=catalog_summary,
                           pending_reviews=pending_reviews,
                           alert_count=alert_count,
                           smtp_configured=smtp_ok,
                           twilio_configured=twilio_ok,
                           reports=ds.get_all_reports())


@app.route('/catalog')
def catalog():
    ds.sync_catalog_from_history()
    alert_count = ds.get_unacknowledged_count()
    status = request.args.get('status', 'active')
    review_status = request.args.get('review_status', 'pending')
    brands_master = ds.get_all_brand_master(status=status)
    brand_options = ds.get_all_brand_master(status='all')
    review_queue = ds.get_catalog_review_queue(status=review_status, limit=300)
    catalog_summary = ds.get_catalog_summary()
    sku_counts = {}
    brand_skus_map = {}
    for brand in brand_options:
        skus = ds.get_brand_skus(brand['id'], status='all')
        sku_counts[brand['id']] = len(skus)
        brand_skus_map[str(brand['id'])] = [
            {'id': sku['id'], 'name': sku['sku_name']}
            for sku in skus
        ]
    return render_template(
        'portal/catalog.html',
        brands_master=brands_master,
        brand_options=brand_options,
        review_queue=review_queue,
        catalog_summary=catalog_summary,
        sku_counts=sku_counts,
        brand_skus_map=brand_skus_map,
        status=status,
        review_status=review_status,
        alert_count=alert_count,
    )


@app.route('/catalog/brand/<slug>')
def catalog_brand_detail(slug):
    ds.sync_catalog_from_history()
    brand = ds.get_brand_master_by_slug(slug)
    if not brand:
        abort(404)
    aliases = ds.get_brand_aliases(brand['id'])
    skus = ds.get_brand_skus(brand['id'], status='all')
    sku_aliases = ds.get_sku_aliases(brand['id'])
    review_queue = [
        item for item in ds.get_catalog_review_queue(status='pending', limit=300)
        if item.get('brand_candidate') == brand['canonical_name']
    ]
    token = next((t for t in ds.get_all_tokens() if t['brand_name'] == brand['canonical_name']), None)
    latest_kpis = ds.get_brand_history(brand['canonical_name'], limit=1)
    return render_template(
        'portal/catalog_brand_detail.html',
        brand=brand,
        aliases=aliases,
        skus=skus,
        sku_aliases=sku_aliases,
        review_queue=review_queue,
        token=token,
        latest_kpis=latest_kpis[0] if latest_kpis else None,
        alert_count=ds.get_unacknowledged_count(),
    )


# ── Brand partner portal (token-auth) ────────────────────────────────────────

@app.route('/portal/<token>')
def brand_portal(token):
    brand_info = ds.get_brand_by_token(token)
    if not brand_info:
        abort(404)

    brand_name = brand_info['brand_name']
    latest = ds.get_latest_report()
    report_id = latest['id'] if latest else None

    kpis    = ds.get_brand_kpis_single(report_id, brand_name) if report_id else None
    history = ds.get_brand_history(brand_name, limit=12)
    forecast_history = list(reversed(ds.get_brand_history(brand_name, limit=36)))
    hist_oldest = list(reversed(history))
    forecast = build_brand_forecasts({brand_name: forecast_history}).get(ds.analytics_brand_name(brand_name), {})
    daily   = ds.get_daily_sales(report_id, brand_name) if report_id else []

    # PDF link
    safe = _safe_name(brand_name)
    pdf_files = [f for f in os.listdir(PDF_DIR) if f.startswith(safe) and f.endswith('.pdf')] \
                if os.path.isdir(PDF_DIR) else []
    pdf_file = pdf_files[0] if pdf_files else None

    # Targets
    report_obj = ds.get_report(report_id) if report_id else None
    month_label = report_obj['month_label'] if report_obj else None
    target = ds.get_target(brand_name, month_label) if month_label else None

    # Portfolio rank
    portfolio_rank = None
    portfolio_total = None
    if report_id and kpis:
        all_kpis = ds.get_all_brand_kpis(report_id)
        all_sorted = sorted(all_kpis, key=lambda x: x.get('total_revenue', 0), reverse=True)
        portfolio_total = len(all_sorted)
        for i, b in enumerate(all_sorted, 1):
            if b['brand_name'] == brand_name:
                portfolio_rank = i
                break

    # Activity log for this brand (last 5 entries)
    activity_log = ds.get_activity_log(brand_name=brand_name, limit=5)
    brand_activity = ds.get_activity_brand_summary(brand_name, limit=6)

    return render_template('portal/brand_portal.html',
                           brand_name=brand_name,
                           brand_info=brand_info,
                           kpis=kpis,
                           history=history,
                           forecast=forecast,
                           daily=daily,
                           pdf_file=pdf_file,
                           report=report_obj,
                           target=target,
                           portfolio_rank=portfolio_rank,
                           portfolio_total=portfolio_total,
                           activity_log=activity_log,
                           brand_activity=brand_activity)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/compute_churn/<int:report_id>', methods=['POST'])
def api_compute_churn(report_id):
    """On-demand churn computation for an existing report."""
    report = ds.get_report(report_id)
    if not report:
        return jsonify({'error': 'Report not found'}), 404
    try:
        _compute_and_save_churn(report_id)
        summary = ds.get_churn_summary(report_id)
        total_churned = sum(v.get('churned', 0) for v in summary.values())
        return jsonify({'success': True, 'brands_processed': len(summary), 'total_churned': total_churned})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/acknowledge', methods=['POST'])
def acknowledge():
    alert_id = request.json.get('alert_id')
    if not alert_id:
        return jsonify({'success': False}), 400
    ds.acknowledge_alert(alert_id)
    _refresh_copilot_state(reason=f'alert_acknowledged:{alert_id}', source='alert_update')
    return jsonify({'success': True, 'remaining': ds.get_unacknowledged_count()})


@app.route('/api/update_contact', methods=['POST'])
def update_contact():
    data       = request.json or {}
    brand_name = data.get('brand_name', '')
    email      = data.get('email', '') or None
    whatsapp   = data.get('whatsapp', '') or None
    if not brand_name:
        return jsonify({'success': False, 'error': 'brand_name required'}), 400
    ds.get_or_create_token(brand_name)  # ensure row exists
    ds.update_brand_contact(brand_name, email=email, whatsapp=whatsapp)
    _refresh_copilot_state(brand_name=brand_name, reason='contact_update', source='brand_update')
    return jsonify({'success': True})


@app.route('/api/regenerate_token', methods=['POST'])
def regenerate_token():
    brand_name = (request.json or {}).get('brand_name', '')
    if not brand_name:
        return jsonify({'success': False}), 400
    token = ds.regenerate_token(brand_name)
    portal_url = url_for('brand_portal', token=token, _external=True)
    return jsonify({'success': True, 'token': token, 'portal_url': portal_url})


@app.route('/api/catalog/resync', methods=['POST'])
def api_catalog_resync():
    ds.sync_catalog_from_history()
    _refresh_copilot_state(reason='catalog_resync', source='catalog_update')
    return jsonify({'success': True, 'summary': ds.get_catalog_summary()})


@app.route('/api/catalog/brand', methods=['POST'])
def api_catalog_brand():
    data = request.get_json(silent=True) or {}
    brand_name = (data.get('brand_name') or '').strip()
    if not brand_name:
        return jsonify({'success': False, 'error': 'brand_name is required'}), 400

    brand_id = data.get('brand_id')
    if brand_id:
        brand = ds.update_brand_master(
            int(brand_id),
            brand_name=brand_name,
            status=(data.get('status') or 'active').strip(),
            category=(data.get('category') or '').strip() or None,
            start_date=(data.get('start_date') or '').strip() or None,
            default_email=(data.get('default_email') or '').strip() or None,
            default_whatsapp=(data.get('default_whatsapp') or '').strip() or None,
            notes=(data.get('notes') or '').strip() or None,
        )
    else:
        brand = ds.ensure_brand_master(
            brand_name,
            status=(data.get('status') or 'active').strip(),
            category=(data.get('category') or '').strip() or None,
            start_date=(data.get('start_date') or '').strip() or None,
            email=(data.get('default_email') or '').strip() or None,
            whatsapp=(data.get('default_whatsapp') or '').strip() or None,
            notes=(data.get('notes') or '').strip() or None,
        )
    _refresh_copilot_state(
        brand_name=brand.get('canonical_name') if brand else brand_name,
        reason='catalog_brand_upsert',
        source='catalog_update',
    )
    return jsonify({'success': True, 'brand': brand})


@app.route('/api/catalog/sku', methods=['POST'])
def api_catalog_sku():
    data = request.get_json(silent=True) or {}
    brand_id = int(data.get('brand_id') or 0)
    sku_name = (data.get('sku_name') or '').strip()
    if not brand_id or not sku_name:
        return jsonify({'success': False, 'error': 'brand_id and sku_name are required'}), 400
    sku = ds.ensure_sku_master(
        brand_id,
        sku_name,
        sku_code=(data.get('sku_code') or '').strip() or None,
        pack_size=(data.get('pack_size') or '').strip() or None,
        unit_type=(data.get('unit_type') or '').strip() or None,
        status=(data.get('status') or 'active').strip(),
        launch_date=(data.get('launch_date') or '').strip() or None,
        notes=(data.get('notes') or '').strip() or None,
    )
    brand = ds.get_brand_master(brand_id)
    _refresh_copilot_state(
        brand_name=brand.get('canonical_name') if brand else None,
        reason='catalog_sku_upsert',
        source='catalog_update',
    )
    return jsonify({'success': True, 'sku': sku})


@app.route('/api/catalog/review/<int:item_id>', methods=['POST'])
def api_catalog_review(item_id):
    data = request.get_json(silent=True) or {}
    item = ds.get_catalog_review_item(item_id)
    try:
        result = _review_catalog_item(
            item_id=item_id,
            action=data.get('action'),
            note=(data.get('note') or '').strip() or None,
            target_brand_id=data.get('target_brand_id'),
            target_sku_id=data.get('target_sku_id'),
        )
    except Exception as exc:
        return jsonify({'success': False, 'error': str(exc)}), 400
    _refresh_copilot_state(
        brand_name=item.get('brand_candidate') if item else None,
        reason=f'catalog_review:{data.get("action") or "unknown"}',
        source='catalog_update',
    )
    return jsonify({'success': True, 'result': result, 'summary': ds.get_catalog_summary()})


@app.route('/api/deliver', methods=['POST'])
def deliver():
    data     = request.json or {}
    channel  = data.get('channel', 'email')   # 'email' | 'whatsapp' | 'both'
    brand_names = data.get('brands', [])       # empty = all brands with contacts

    from modules.delivery import send_bulk_reports, send_bulk_whatsapp, smtp_configured, twilio_configured

    tokens = ds.get_all_tokens()
    if brand_names:
        tokens = [t for t in tokens if t['brand_name'] in brand_names]

    latest = ds.get_latest_report()
    month_label = latest['month_label'] if latest else 'Latest'
    results = []

    if channel in ('email', 'both'):
        if not smtp_configured():
            return jsonify({'success': False, 'error': 'SMTP not configured'}), 400
        email_results = send_bulk_reports(tokens, month_label, PDF_DIR,
                                          base_url=request.host_url.rstrip('/'), ds=ds)
        results.extend(email_results)

    if channel in ('whatsapp', 'both'):
        if not twilio_configured():
            return jsonify({'success': False, 'error': 'Twilio not configured'}), 400
        wa_results = send_bulk_whatsapp(tokens, month_label, ds=ds)
        results.extend(wa_results)

    return jsonify({'success': True, 'results': results})


@app.route('/api/reports')
def api_reports():
    return jsonify(ds.get_all_reports())


@app.route('/api/brand_history/<path:brand_name>')
def api_brand_history(brand_name):
    history = ds.get_brand_history(brand_name, limit=12)
    return jsonify(list(reversed(history)))


# ── File downloads ────────────────────────────────────────────────────────────

@app.route('/download/pdf/<path:filename>')
def download_pdf(filename):
    path = os.path.join(PDF_DIR, filename)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=filename,
                     mimetype='application/pdf')


@app.route('/download/html/<path:filename>')
def download_html(filename):
    path = os.path.join(HTML_DIR, filename)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, mimetype='text/html')


# ── Generate PDF/HTML from DB data ────────────────────────────────────────────

def _reconstruct_kpis_from_db(report_id: int, brand_name: str) -> dict:
    """
    Reconstruct a kpis dict from stored DB data for PDF/HTML generation.
    Loads detailed DataFrames from brand_detail_json table when available.
    """
    import json as _json

    bk = ds.get_brand_kpis_single(report_id, brand_name)
    if not bk:
        return None

    # Rebuild daily_sales DataFrame
    daily_rows = ds.get_daily_sales(report_id, brand_name)
    if daily_rows:
        daily_df = pd.DataFrame(daily_rows)[['date', 'revenue', 'qty']]
        daily_df = daily_df.rename(columns={'date': 'Date', 'revenue': 'Revenue', 'qty': 'Quantity'})
        daily_df['Date'] = pd.to_datetime(daily_df['Date'])
    else:
        daily_df = pd.DataFrame(columns=['Date', 'Revenue', 'Quantity'])

    # Load detailed DataFrames from JSON store
    def _load_df(json_str, columns):
        try:
            if json_str and json_str != '[]':
                records = _json.loads(json_str)
                if records:
                    frame = pd.DataFrame(records)
                    if 'Date' in frame.columns and not frame.empty:
                        try:
                            if pd.api.types.is_numeric_dtype(frame['Date']):
                                frame['Date'] = pd.to_datetime(frame['Date'], unit='ms', errors='coerce')
                            else:
                                frame['Date'] = pd.to_datetime(frame['Date'], errors='coerce')
                        except Exception:
                            frame['Date'] = pd.to_datetime(frame['Date'], errors='coerce')
                    return frame
        except Exception:
            pass
        return pd.DataFrame(columns=columns)

    detail = ds.get_brand_detail_json(report_id, brand_name)
    if not detail:
        report = ds.get_report(report_id)
        if report:
            detail = _try_backfill_brand_detail_json(report, brand_name)
    detail = detail or {}

    top_stores_df    = _load_df(detail.get('top_stores_json'),    ['Store', 'Revenue'])
    product_value_df = _load_df(detail.get('product_value_json'), ['SKU', 'Revenue'])
    product_qty_df   = _load_df(detail.get('product_qty_json'),   ['SKU', 'Quantity'])
    closing_stock_df = _load_df(detail.get('closing_stock_json'), ['SKU', 'Closing Stock (Cartons)'])
    pickup_df        = _load_df(detail.get('pickup_json'),        ['SKU', 'Qty Picked Up', 'Value'])
    supply_df        = _load_df(detail.get('supply_json'),        ['SKU', 'Qty Supplied', 'Value'])
    reorder_df       = _load_df(detail.get('reorder_json'),       [])
    heatmap_df       = _load_df(detail.get('heatmap_json'),       ['Store', 'Date', 'Orders'])

    if not product_qty_df.empty:
        top_qty_label = str(product_qty_df.iloc[0].get('SKU') or '').strip()
        if top_qty_label and looks_like_store_label(top_qty_label) and not looks_like_sku_label(top_qty_label):
            product_qty_df = pd.DataFrame(columns=['SKU', 'Quantity'])

    # Derive top SKU from product_qty
    top_sku     = product_qty_df.iloc[0]['SKU']     if not product_qty_df.empty else '-'
    top_sku_qty = product_qty_df.iloc[0]['Quantity'] if not product_qty_df.empty else 0
    top_sku_value_name = product_value_df.iloc[0]['SKU'] if not product_value_df.empty else '-'
    top_sku_value = product_value_df.iloc[0]['Revenue'] if not product_value_df.empty else 0
    peak_date = bk.get('peak_date')
    peak_qty = 0
    if peak_date:
        try:
            peak_date = pd.to_datetime(peak_date)
            if pd.isna(peak_date):
                peak_date = None
        except Exception:
            peak_date = None
    if not daily_df.empty:
        try:
            peak_row = daily_df.loc[daily_df['Revenue'].idxmax()]
            if peak_date is None:
                peak_date = peak_row['Date']
            peak_qty = float(peak_row.get('Quantity', 0) or 0)
        except Exception:
            peak_qty = 0

    # Top-store percentage
    total_rev    = bk.get('total_revenue', 0) or 1
    top_store_pct = round(bk.get('top_store_revenue', 0) / total_rev * 100, 1)

    status    = bk.get('inv_health_status') or 'No Stock Data'
    color_map = {'Healthy Stock': 'green', 'Low Stock': 'amber', 'Overstocked': 'blue'}
    inv_color = color_map.get(status, 'gray')

    # ── Weekly sparkline percentages (derived from daily_df) ──────────────────
    if not daily_df.empty:
        min_date = daily_df['Date'].min()
        weekly_rev, weekly_qty = [], []
        for w in range(4):
            w_start = min_date + pd.Timedelta(days=w * 7)
            w_end   = w_start  + pd.Timedelta(days=6)
            mask    = (daily_df['Date'] >= w_start) & (daily_df['Date'] <= w_end)
            weekly_rev.append(float(daily_df.loc[mask, 'Revenue'].sum()))
            weekly_qty.append(float(daily_df.loc[mask, 'Quantity'].sum()))
        tot_r = sum(weekly_rev) or 1
        tot_q = sum(weekly_qty) or 1
        weekly_rev_pct = [round(v / tot_r * 100, 1) for v in weekly_rev]
        weekly_qty_pct = [round(v / tot_q * 100, 1) for v in weekly_qty]
    else:
        weekly_rev_pct = [0, 0, 0, 0]
        weekly_qty_pct = [0, 0, 0, 0]

    kpis = {
        # Scalars
        'total_revenue':         bk.get('total_revenue', 0),
        'gmv':                   bk.get('total_revenue', 0),
        'total_qty':             bk.get('total_qty', 0),
        'num_stores':            bk.get('num_stores', 0),
        'unique_skus':           bk.get('unique_skus', 0),
        'trading_days':          bk.get('trading_days', 0),
        'repeat_stores':         bk.get('repeat_stores', 0),
        'single_stores':         bk.get('single_stores', 0),
        'repeat_pct':            bk.get('repeat_pct', 0),
        'avg_revenue_per_store': bk.get('avg_revenue_per_store', 0),
        'closing_stock_total':   bk.get('closing_stock_total', 0),
        'total_closing_stock':   bk.get('closing_stock_total', 0),
        'stock_days_cover':      bk.get('stock_days_cover', 0),
        'inv_health_status':     status,
        'inv_health_color':      inv_color,
        'peak_date':             peak_date,
        'peak_revenue':          bk.get('peak_revenue', 0),
        'peak_qty':              peak_qty,
        'top_store_name':        bk.get('top_store_name') or '-',
        'top_store_revenue':     bk.get('top_store_revenue', 0),
        'top_store_pct':         top_store_pct,
        'wow_rev_change':        bk.get('wow_rev_change', 0),
        'wow_qty_change':        bk.get('wow_qty_change', 0),
        'weekly_rev_pct':        weekly_rev_pct,
        'weekly_qty_pct':        weekly_qty_pct,
        'top_sku':               top_sku,
        'top_sku_qty':           top_sku_qty,
        'top_sku_value_name':    top_sku_value_name,
        'top_sku_value':         top_sku_value,
        'total_pickup_qty':      pickup_df['Qty Picked Up'].sum()  if not pickup_df.empty else 0,
        'total_pickup_value':    pickup_df['Value'].sum()          if not pickup_df.empty else 0,
        'total_supplied_qty':    supply_df['Qty Supplied'].sum()   if not supply_df.empty else 0,
        'total_supplied_value':  supply_df['Value'].sum()          if not supply_df.empty else 0,
        # DataFrames
        'daily_sales':           daily_df,
        'top_stores':            top_stores_df,
        'product_qty':           product_qty_df,
        'product_value':         product_value_df,
        'closing_stock':         closing_stock_df,
        'reorder_analysis':      reorder_df,
        'store_heatmap_df':      heatmap_df,
        'pickup_summary':        pickup_df,
        'supply_summary':        supply_df,
    }
    return _attach_reorder_trend(
        brand_name=brand_name,
        kpis=kpis,
        report_type=bk.get('report_type'),
        cutoff_date=bk.get('end_date'),
    )


def _build_brand_report_pdf_bytes(report_id: int, brand_name: str,
                                  report: dict | None = None,
                                  kpis: dict | None = None,
                                  all_brand_kpis: list | None = None) -> bytes:
    """Build PDF bytes using the premium print-optimised report template."""
    report = report or ds.get_report(report_id)
    if not report:
        raise ValueError(f'Report {report_id} not found')

    kpis = kpis or _reconstruct_kpis_from_db(report_id, brand_name)
    if not kpis:
        raise ValueError(f'KPI data not found for {brand_name}')

    all_brand_kpis = all_brand_kpis or ds.get_all_brand_kpis(report_id)
    total_portfolio = sum(b['total_revenue'] for b in all_brand_kpis)
    avg_portfolio   = total_portfolio / max(len(all_brand_kpis), 1)
    report_context = _brand_report_context(brand_name, cutoff_date=report.get('end_date'))
    coach = _report_summary_payload(brand_name, kpis, report)
    activity_report = _build_brand_activity_report(brand_name, report_id, kpis)

    # Use the premium 2-page print template (report_template.html)
    html = render_pdf_report_html(
        brand_name=brand_name,
        kpis=kpis,
        start_date=report['start_date'],
        end_date=report['end_date'],
        portfolio_avg_revenue=avg_portfolio,
        total_portfolio_revenue=total_portfolio,
        report_type=report.get('report_type'),
        month_label=report.get('month_label'),
        growth_outlook=report_context.get('growth_outlook'),
        gmv_window=report_context.get('gmv_window'),
        coach=coach,
        activity_report=activity_report,
    )
    return render_pdf_bytes(html)


def _build_retailer_report_pdf_bytes(retailer_code: str, report_id: int | None = None,
                                     month_value: str | None = None) -> tuple[bytes, dict]:
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        raise ValueError(f'Retailer data not found for {retailer_code}')
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    html = render_retailer_pdf_report_html(detail)
    return render_pdf_bytes(html), detail


def _build_retailer_group_report_pdf_bytes(group_slug: str, report_id: int | None = None,
                                           month_value: str | None = None) -> tuple[bytes, dict]:
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        raise ValueError(f'Retailer group data not found for {group_slug}')
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    detail['scope_label'] = 'Retailer Group Intelligence Report'
    html = render_retailer_pdf_report_html(detail)
    return render_pdf_bytes(html), detail


_REPORT_DEPENDENCY_PATHS = [
    os.path.join(os.path.dirname(__file__), 'app.py'),
    os.path.join(os.path.dirname(__file__), 'templates', 'report_template.html'),
    os.path.join(os.path.dirname(__file__), 'templates', 'report_interactive.html'),
    os.path.join(os.path.dirname(__file__), 'modules', 'pdf_generator_html.py'),
    os.path.join(os.path.dirname(__file__), 'modules', 'html_generator.py'),
    os.path.join(os.path.dirname(__file__), 'modules', 'charts_html.py'),
    os.path.join(os.path.dirname(__file__), 'modules', 'predictor.py'),
]


def _report_render_signature_mtime() -> float:
    mtimes = []
    for path in _REPORT_DEPENDENCY_PATHS:
        if os.path.isfile(path):
            mtimes.append(os.path.getmtime(path))
    return max(mtimes) if mtimes else 0


@app.route('/api/report_pdf/<int:report_id>/<path:brand_name>')
def api_report_pdf(report_id, brand_name):
    """Serve a PDF report — from disk if fresh, otherwise build with the current template."""
    report = ds.get_report(report_id)
    if not report:
        abort(404)

    safe      = _safe_name(brand_name)
    month_tag = datetime.strptime(report['start_date'], '%Y-%m-%d').strftime('%b%Y')
    fname     = f"{safe}_Report_{month_tag}.pdf"
    disk_path = os.path.join(PDF_DIR, fname)

    render_signature_mtime = _report_render_signature_mtime()

    # Fast path: serve pre-generated PDF only if it was built after the current print pipeline
    if os.path.isfile(disk_path) and os.path.getmtime(disk_path) >= render_signature_mtime:
        return send_file(disk_path, as_attachment=True, download_name=fname,
                         mimetype='application/pdf')

    # Slow path: reconstruct and generate
    kpis = _reconstruct_kpis_from_db(report_id, brand_name)
    if not kpis:
        abort(404)

    all_bk = ds.get_all_brand_kpis(report_id)
    try:
        pdf_bytes = _build_brand_report_pdf_bytes(
            report_id=report_id, brand_name=brand_name,
            report=report, kpis=kpis, all_brand_kpis=all_bk,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Cache to disk so next request is instant
    try:
        with open(disk_path, 'wb') as fh:
            fh.write(pdf_bytes)
        return send_file(disk_path, as_attachment=True, download_name=fname,
                         mimetype='application/pdf')
    except Exception:
        return send_file(io.BytesIO(pdf_bytes), as_attachment=True,
                         download_name=fname, mimetype='application/pdf')


@app.route('/api/report_pdf_bulk/<int:report_id>', methods=['POST'])
def api_report_pdf_bulk(report_id):
    """ZIP PDFs for all/selected brands — serves pre-generated files instantly, generates missing ones in parallel."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    report = ds.get_report(report_id)
    if not report:
        abort(404)

    all_bk          = ds.get_all_brand_kpis(report_id)
    brands_in_order = [b['brand_name'] for b in all_bk]
    payload         = request.get_json(silent=True) or {}
    requested_brands = payload.get('brands') or []

    if requested_brands:
        requested_set = set(requested_brands)
        brands = [b for b in brands_in_order if b in requested_set]
    else:
        brands = brands_in_order

    if not brands:
        return jsonify({'error': 'No brands selected for bulk download.'}), 400

    month_tag = datetime.strptime(report['start_date'], '%Y-%m-%d').strftime('%b%Y')
    zip_name  = (f"Selected_Brand_Reports_{month_tag}.zip" if requested_brands
                 else f"All_Brand_Reports_{month_tag}.zip")

    render_signature_mtime = _report_render_signature_mtime()

    def _get_pdf(brand_name):
        safe      = _safe_name(brand_name)
        disk_path = os.path.join(PDF_DIR, f"{safe}_Report_{month_tag}.pdf")
        # Serve from disk only if it matches the current render pipeline.
        if os.path.isfile(disk_path) and os.path.getmtime(disk_path) >= render_signature_mtime:
            with open(disk_path, 'rb') as fh:
                return brand_name, fh.read(), None
        try:
            pdf_bytes = _build_brand_report_pdf_bytes(
                report_id=report_id, brand_name=brand_name,
                report=report, all_brand_kpis=all_bk,
            )
            # Cache to disk for future requests
            try:
                with open(disk_path, 'wb') as fh:
                    fh.write(pdf_bytes)
            except Exception:
                pass
            return brand_name, pdf_bytes, None
        except Exception as exc:
            return brand_name, None, str(exc)

    # Parallel: disk reads + KPI reconstruction run concurrently;
    # Playwright rendering is serialised internally by _browser_lock
    results   = {}
    failures  = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_get_pdf, b): b for b in brands}
        for fut in as_completed(futures):
            brand, pdf_bytes, err = fut.result()
            if err:
                failures.append(f'{brand}: {err}')
            else:
                results[brand] = pdf_bytes

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for brand_name in brands:          # preserve original order
            if brand_name in results:
                safe = _safe_name(brand_name)
                archive.writestr(f'{safe}_Report_{month_tag}.pdf', results[brand_name])
        if failures:
            archive.writestr('DOWNLOAD_ERRORS.txt',
                             'Some reports could not be generated.\n\n' + '\n'.join(failures))

    zip_buffer.seek(0)
    return send_file(zip_buffer, as_attachment=True, download_name=zip_name,
                     mimetype='application/zip')


@app.route('/api/portfolio_pdf/<int:report_id>')
def api_portfolio_pdf(report_id):
    """Convert the pre-generated portfolio HTML into a PDF and stream it."""
    report = ds.get_report(report_id)
    if not report:
        abort(404)
    month_tag = datetime.strptime(report['start_date'], '%Y-%m-%d').strftime('%b%Y')
    html_path = os.path.join(HTML_DIR, f"PORTFOLIO_Dashboard_{month_tag}.html")
    if not os.path.isfile(html_path):
        return jsonify({'error': 'Portfolio HTML not yet generated. Run a full report generation first.'}), 404
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    try:
        pdf_bytes = render_pdf_bytes(prepare_interactive_html_for_pdf(html_content))
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    fname = f"PORTFOLIO_ExecutiveSummary_{month_tag}.pdf"
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True, download_name=fname, mimetype='application/pdf')


@app.route('/api/report_html/<int:report_id>/<path:brand_name>')
def api_report_html(report_id, brand_name):
    """Generate and stream the interactive HTML report for a single brand from DB data."""
    report = ds.get_report(report_id)
    if not report:
        abort(404)
    kpis = _reconstruct_kpis_from_db(report_id, brand_name)
    if not kpis:
        abort(404)

    all_bk          = ds.get_all_brand_kpis(report_id)
    total_portfolio = sum(b['total_revenue'] for b in all_bk)
    avg_portfolio   = total_portfolio / max(len(all_bk), 1)
    report_context = _brand_report_context(brand_name, cutoff_date=report.get('end_date'))
    activity_report = _build_brand_activity_report(brand_name, report_id, kpis)

    try:
        html_content = render_html_report(
            brand_name=brand_name,
            kpis=kpis,
            start_date=report['start_date'],
            end_date=report['end_date'],
            portfolio_avg_revenue=avg_portfolio,
            total_portfolio_revenue=total_portfolio,
            report_type=report.get('report_type'),
            month_label=report.get('month_label'),
            growth_outlook=report_context.get('growth_outlook'),
            gmv_window=report_context.get('gmv_window'),
            coach=_report_summary_payload(brand_name, kpis, report),
            activity_report=activity_report,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return Response(html_content, mimetype='text/html')


@app.route('/retailer/<path:retailer_code>/report')
def retailer_report_page(retailer_code):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    html_content = render_retailer_html_report(detail)
    return Response(html_content, mimetype='text/html')


@app.route('/retailer-groups/<path:group_slug>/report')
def retailer_group_report_page(group_slug):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    detail['scope_label'] = 'Retailer Group Intelligence Report'
    return Response(render_retailer_html_report(detail), mimetype='text/html')


@app.route('/api/retailers/<path:retailer_code>/report_html')
def api_retailer_report_html(retailer_code):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    html = render_retailer_html_report(detail)
    if request.args.get('download', type=int) == 1:
        month_tag = detail.get('period_label') or 'Retailer'
        safe = _safe_name(detail.get('retailer_name') or retailer_code)
        return Response(
            html,
            mimetype='text/html',
            headers={'Content-Disposition': f'attachment; filename="{safe}_Retailer_Report_{month_tag.replace(" ", "_")}.html"'},
        )
    return Response(html, mimetype='text/html')


@app.route('/api/retailer-groups/<path:group_slug>/report_html')
def api_retailer_group_report_html(group_slug):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    detail['coach'] = coach
    detail['scope_label'] = 'Retailer Group Intelligence Report'
    html = render_retailer_html_report(detail)
    if request.args.get('download', type=int) == 1:
        month_tag = detail.get('period_label') or 'Retailer_Group'
        safe = _safe_name(detail.get('retailer_name') or group_slug)
        return Response(
            html,
            mimetype='text/html',
            headers={'Content-Disposition': f'attachment; filename="{safe}_Retailer_Group_Report_{month_tag.replace(" ", "_")}.html"'},
        )
    return Response(html, mimetype='text/html')


@app.route('/api/retailers/<path:retailer_code>/report_pdf')
def api_retailer_report_pdf(retailer_code):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    pdf_bytes, detail = _build_retailer_report_pdf_bytes(retailer_code, report_id=report_id, month_value=month_value)
    month_tag = detail.get('period_label') or 'Retailer'
    safe = _safe_name(detail.get('retailer_name') or retailer_code)
    filename = f"{safe}_Retailer_Report_{month_tag.replace(' ', '_')}.pdf"
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True, download_name=filename, mimetype='application/pdf')


@app.route('/api/retailer-groups/<path:group_slug>/report_pdf')
def api_retailer_group_report_pdf(group_slug):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    try:
        pdf_bytes, detail = _build_retailer_group_report_pdf_bytes(group_slug, report_id=report_id, month_value=month_value)
    except ValueError:
        abort(404)
    month_tag = detail.get('period_label') or 'Retailer_Group'
    safe = _safe_name(detail.get('retailer_name') or group_slug)
    filename = f"{safe}_Retailer_Group_Report_{month_tag.replace(' ', '_')}.pdf"
    return send_file(io.BytesIO(pdf_bytes), as_attachment=True, download_name=filename, mimetype='application/pdf')


@app.route('/files')
def list_files():
    pdfs  = sorted(f for f in os.listdir(PDF_DIR)  if f.endswith('.pdf')) if os.path.isdir(PDF_DIR) else []
    htmls = sorted(f for f in os.listdir(HTML_DIR) if f.endswith('.html')) if os.path.isdir(HTML_DIR) else []
    return jsonify({'pdfs': pdfs, 'htmls': htmls})


# ── Forecasting Dashboard ─────────────────────────────────────────────────────

@app.route('/forecasting')
def forecasting():
    import re as _re
    import json as _json

    def _forecast_anchor(value):
        return _re.sub(r'[^a-z0-9]+', '-', str(value or '').lower()).strip('-') or 'brand'

    alert_count = ds.get_unacknowledged_count()
    report      = ds.get_latest_report()
    all_brands  = ds.get_all_brands_in_db()
    if not all_brands:
        return render_template('portal/forecasting.html', forecasts={},
                               forecasts_json='{}', depletions={},
                               growing_count=0, declining_count=0,
                               stable_count=0, stock_warning_count=0,
                               report=None, alert_count=alert_count,
                               forecast_groups={}, horizon_sections={}, horizon_nav=[])

    brand_histories = {b: list(reversed(ds.get_brand_history(b, limit=36)))
                       for b in all_brands}
    forecasts = build_brand_forecasts(brand_histories)

    # Latest KPIs for stock depletion
    latest_report = ds.get_latest_report()
    depletions = {}
    if latest_report:
        depletions = _merge_depletions_by_brand(ds.get_all_brand_kpis(latest_report['id']))

    growing_count  = sum(1 for f in forecasts.values() if f['growth_label'] == 'Growing')
    declining_count= sum(1 for f in forecasts.values() if f['growth_label'] == 'Declining')
    stable_count   = sum(1 for f in forecasts.values() if f['growth_label'] == 'Stable')
    stock_warning_count = sum(1 for d in depletions.values()
                              if d.get('urgency') in ('critical', 'warning'))
    eligible_3m_count = sum(1 for f in forecasts.values() if f.get('horizons', {}).get('3m', {}).get('eligible'))
    eligible_6m_count = sum(1 for f in forecasts.values() if f.get('horizons', {}).get('6m', {}).get('eligible'))
    eligible_12m_count = sum(1 for f in forecasts.values() if f.get('horizons', {}).get('12m', {}).get('eligible'))

    sorted_forecast_items = sorted(forecasts.items(), key=lambda item: item[0].lower())
    forecast_groups = {
        'Growing': [],
        'Stable': [],
        'Declining': [],
        'Insufficient Data': [],
    }
    horizon_sections = {
        '3m': {'label': '3-Month Analysis', 'months': 3, 'items': []},
        '6m': {'label': '6-Month Analysis', 'months': 6, 'items': []},
        '12m': {'label': '1-Year Analysis', 'months': 12, 'items': []},
    }

    ordered_forecasts = {}
    for brand, fc in sorted_forecast_items:
        anchor = _forecast_anchor(brand)
        item = {
            'brand': brand,
            'anchor': anchor,
            'forecast': fc,
            'depletion': depletions.get(brand, {}),
        }
        forecast_groups.setdefault(fc.get('growth_label') or 'Insufficient Data', []).append(item)
        ordered_forecasts[brand] = fc

        for key in ('3m', '6m', '12m'):
            hz = (fc.get('horizons') or {}).get(key, {})
            if hz.get('eligible'):
                horizon_sections[key]['items'].append({
                    'brand': brand,
                    'anchor': anchor,
                    'forecast': fc,
                    'horizon': hz,
                    'depletion': depletions.get(brand, {}),
                })

    horizon_nav = [
        {'key': '3m', 'label': '3-Month Analysis', 'count': len(horizon_sections['3m']['items'])},
        {'key': '6m', 'label': '6-Month Analysis', 'count': len(horizon_sections['6m']['items'])},
        {'key': '12m', 'label': '1-Year Analysis', 'count': len(horizon_sections['12m']['items'])},
    ]

    return render_template('portal/forecasting.html',
                           forecasts=ordered_forecasts, forecasts_json=_json.dumps(ordered_forecasts),
                           depletions=depletions, report=report,
                           growing_count=growing_count, declining_count=declining_count,
                           stable_count=stable_count, stock_warning_count=stock_warning_count,
                           eligible_3m_count=eligible_3m_count,
                           eligible_6m_count=eligible_6m_count,
                           eligible_12m_count=eligible_12m_count,
                           alert_count=alert_count,
                           forecast_groups=forecast_groups,
                           horizon_sections=horizon_sections,
                           horizon_nav=horizon_nav)


# ── Brand Leaderboard ─────────────────────────────────────────────────────────

@app.route('/leaderboard')
def leaderboard():
    alert_count = ds.get_unacknowledged_count()
    report = ds.get_latest_report()
    if not report:
        return render_template('portal/leaderboard.html', leaderboard=[],
                               report=None, alert_count=alert_count)
    lb = ds.get_leaderboard(report['id'])
    return render_template('portal/leaderboard.html', leaderboard=lb,
                           report=report, alert_count=alert_count)


# ── SKU Analytics ─────────────────────────────────────────────────────────────

@app.route('/sku-analytics')
def sku_analytics():
    import json as _json
    alert_count = ds.get_unacknowledged_count()
    report = ds.get_latest_report()
    if not report:
        return render_template('portal/sku_analytics.html', sku_data=[], sku_json='[]',
                               brand_sku_counts_json='[]', brands=[], report=None,
                               alert_count=alert_count)

    import json as _json
    brand_kpis_rows = ds.get_all_brand_kpis(report['id'])
    brand_filter = request.args.get('brand', '')

    # Build real SKU data from brand_detail_json (product_value_json + product_qty_json)
    sku_data = []
    brand_sku_counts = []
    for bk in brand_kpis_rows:
        bname = bk['brand_name']
        brand_sku_counts.append({'brand': bname, 'sku_count': bk['unique_skus']})
        if brand_filter and bname != brand_filter:
            continue
        detail = ds.get_brand_detail_json(report['id'], bname)
        if not detail:
            continue
        try:
            val_rows = _json.loads(detail.get('product_value_json', '[]') or '[]')
            qty_rows = _json.loads(detail.get('product_qty_json', '[]') or '[]')
            qty_map = {}
            for qr in qty_rows:
                sku_key = qr.get('SKUs') or qr.get('Product') or next(iter(qr), None)
                qty_val = qr.get('Quantity') or qr.get('Qty') or (list(qr.values())[1] if len(qr) > 1 else 0)
                if sku_key:
                    qty_map[str(sku_key)] = float(qty_val or 0)
            for vr in val_rows:
                sku_name = vr.get('SKUs') or vr.get('Product') or next(iter(vr), None)
                rev = vr.get('Sales_Value') or vr.get('Revenue') or (list(vr.values())[1] if len(vr) > 1 else 0)
                if not sku_name:
                    continue
                qty = qty_map.get(str(sku_name), 0)
                avg_price = round(float(rev) / qty, 2) if qty > 0 else 0
                sku_data.append({
                    'name': str(sku_name),
                    'brand': bname,
                    'revenue': round(float(rev or 0), 0),
                    'qty': round(float(qty or 0), 1),
                    'avg_price': avg_price,
                })
        except Exception:
            pass

    sku_data.sort(key=lambda x: x['revenue'], reverse=True)
    brands = [bk['brand_name'] for bk in brand_kpis_rows]

    return render_template('portal/sku_analytics.html',
                           sku_data=sku_data,
                           sku_json=_json.dumps(sku_data),
                           brand_sku_counts_json=_json.dumps(brand_sku_counts),
                           brands=brands, report=report, alert_count=alert_count,
                           brand_filter=brand_filter)


# ── Target Setting ────────────────────────────────────────────────────────────

@app.route('/targets')
def targets():
    alert_count = ds.get_unacknowledged_count()
    report = ds.get_latest_report()

    # Month selector
    all_reports = ds.get_all_reports()
    available_months = [r['month_label'] for r in all_reports]
    selected_month = request.args.get('month', available_months[0] if available_months else '')

    # Current KPIs for selected month
    brands_data = []
    if report:
        for bk in ds.get_all_brand_kpis(report['id']):
            brands_data.append(bk)

    # Get targets for selected month
    targets_list = ds.get_all_targets(selected_month)
    targets_map  = {t['brand_name']: t for t in targets_list}

    on_track_count  = sum(1 for b in brands_data
                          if targets_map.get(b['brand_name'], {}).get('target_revenue', 0) > 0
                          and b['total_revenue'] / targets_map[b['brand_name']]['target_revenue'] >= 0.8)
    at_risk_count   = sum(1 for b in brands_data
                          if targets_map.get(b['brand_name'], {}).get('target_revenue', 0) > 0
                          and 0.5 <= b['total_revenue'] / targets_map[b['brand_name']]['target_revenue'] < 0.8)
    off_track_count = sum(1 for b in brands_data
                          if targets_map.get(b['brand_name'], {}).get('target_revenue', 0) > 0
                          and b['total_revenue'] / targets_map[b['brand_name']]['target_revenue'] < 0.5)
    no_target_count = sum(1 for b in brands_data
                          if not targets_map.get(b['brand_name'], {}).get('target_revenue', 0))

    return render_template('portal/targets.html',
                           brands=brands_data, targets=targets_map,
                           available_months=available_months,
                           selected_month=selected_month, report=report,
                           on_track_count=on_track_count, at_risk_count=at_risk_count,
                           off_track_count=off_track_count, no_target_count=no_target_count,
                           alert_count=alert_count)


@app.route('/api/set_target', methods=['POST'])
def api_set_target():
    brand_name    = request.form.get('brand_name', '').strip()
    month_label   = request.form.get('month_label', '').strip()
    target_revenue= float(request.form.get('target_revenue', 0) or 0)
    if not brand_name or not month_label:
        return redirect(url_for('targets'))
    ds.set_target(brand_name, month_label, target_revenue=target_revenue)
    ds.log_activity('target_set', f'Target ₦{target_revenue:,.0f} for {brand_name} ({month_label})', brand_name)
    _refresh_copilot_state(
        brand_name=brand_name,
        reason=f'target_update:{month_label}',
        source='target_update',
    )
    return redirect(url_for('targets', month=month_label))


# ── Alert Rules ───────────────────────────────────────────────────────────────

@app.route('/alert-rules')
def alert_rules_view():
    alert_count = ds.get_unacknowledged_count()
    rules       = ds.get_alert_rules(active_only=False)
    all_brands  = ds.get_all_brands_in_db()
    return render_template('portal/alert_rules.html', rules=rules,
                           all_brands=all_brands, alert_count=alert_count)


@app.route('/api/save_alert_rule', methods=['POST'])
def api_save_alert_rule():
    ds.save_alert_rule(
        rule_name   = request.form.get('rule_name', '').strip(),
        brand_filter= request.form.get('brand_filter', 'all'),
        metric      = request.form.get('metric', ''),
        operator    = request.form.get('operator', 'lt'),
        threshold   = float(request.form.get('threshold', 0) or 0),
        severity    = request.form.get('severity', 'medium'),
    )
    ds.log_activity('alert_rule_created', request.form.get('rule_name', ''))
    _refresh_copilot_state(reason='alert_rule_created', source='alert_rule')
    return redirect(url_for('alert_rules_view'))


@app.route('/api/toggle_alert_rule', methods=['POST'])
def api_toggle_alert_rule():
    rule_id = int(request.form.get('rule_id', 0))
    active  = int(request.form.get('active', 1))
    ds.toggle_alert_rule(rule_id, active)
    _refresh_copilot_state(reason=f'alert_rule_toggle:{rule_id}', source='alert_rule')
    return redirect(url_for('alert_rules_view'))


@app.route('/api/delete_alert_rule', methods=['POST'])
def api_delete_alert_rule():
    rule_id = int(request.form.get('rule_id', 0))
    ds.delete_alert_rule(rule_id)
    _refresh_copilot_state(reason=f'alert_rule_delete:{rule_id}', source='alert_rule')
    return redirect(url_for('alert_rules_view'))


# ── Google Sheets Sync ────────────────────────────────────────────────────────

@app.route('/api/sync_sheets/<path:brand_name>', methods=['POST'])
def api_sync_sheets(brand_name):
    try:
        from modules.sheets import push_brand_to_sheets
        report = ds.get_latest_report()
        if not report:
            return jsonify({'success': False, 'error': 'No report data found'}), 404
        bk = ds.get_brand_kpis_single(report['id'], brand_name)
        if not bk:
            return jsonify({'success': False, 'error': 'Brand not in latest report'}), 404
        url = push_brand_to_sheets(brand_name, report['start_date'], report['end_date'])
        ds.log_activity('sheets_sync', f'Synced to Google Sheets', brand_name, report['id'])
        return jsonify({'success': True, 'url': url})
    except Exception as exc:
        return jsonify({'success': False, 'error': str(exc)}), 500


# ── REST API v1 ───────────────────────────────────────────────────────────────

@app.route('/api/v1/brands')
def api_v1_brands():
    """REST: list all brands with latest KPIs."""
    report = ds.get_latest_report()
    if not report:
        return jsonify({'brands': [], 'report': None})
    kpis = ds.get_all_brand_kpis(report['id'])
    # Strip non-serialisable fields
    clean = []
    for k in kpis:
        clean.append({
            'brand_name': k['brand_name'],
            'total_revenue': k['total_revenue'],
            'total_qty': k['total_qty'],
            'num_stores': k['num_stores'],
            'unique_skus': k['unique_skus'],
            'repeat_pct': k['repeat_pct'],
            'perf_grade': k['perf_grade'],
            'perf_score': k['perf_score'],
            'stock_days_cover': k['stock_days_cover'],
        })
    return jsonify({'brands': clean, 'report': {'id': report['id'], 'month_label': report['month_label']}})


@app.route('/api/v1/kpis/<path:brand_name>')
def api_v1_kpis(brand_name):
    """REST: full KPI history for a brand."""
    history = ds.get_brand_history(brand_name, limit=24)
    return jsonify({'brand': brand_name, 'history': history})


@app.route('/api/v1/alerts')
def api_v1_alerts():
    """REST: unacknowledged alerts."""
    alerts = ds.get_alerts(unacknowledged_only=True)
    return jsonify({'alerts': alerts, 'count': len(alerts)})


@app.route('/api/v1/portfolio')
def api_v1_portfolio():
    """REST: portfolio monthly trend."""
    trend = ds.get_portfolio_monthly_trend(limit=24)
    return jsonify({'trend': trend})


# ── Data Export ───────────────────────────────────────────────────────────────

@app.route('/api/export/brands')
def api_export_brands():
    """Export brand KPIs as CSV or JSON."""
    fmt    = request.args.get('format', 'csv')
    report_id = request.args.get('report_id', type=int)
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    if not report:
        abort(404)
    kpis = ds.get_all_brand_kpis(report['id'])
    if fmt == 'json':
        return jsonify(kpis)
    # CSV
    import csv, io as _io
    buf = _io.StringIO()
    if kpis:
        writer = csv.DictWriter(buf, fieldnames=kpis[0].keys())
        writer.writeheader()
        money_fields = {
            'total_revenue', 'avg_revenue_per_store', 'closing_stock_total',
            'stock_days_cover', 'portfolio_share_pct', 'wow_rev_change',
            'wow_qty_change', 'peak_revenue', 'top_store_revenue',
        }
        for row in kpis:
            sanitized = {}
            for key, value in row.items():
                if key in money_fields and isinstance(value, (int, float)):
                    sanitized[key] = _money_csv_2dp(value)
                else:
                    sanitized[key] = value
            writer.writerow(sanitized)
    buf.seek(0)
    return app.response_class(buf.getvalue(), mimetype='text/csv',
                               headers={'Content-Disposition':
                                        f'attachment; filename=brands_{report["month_label"]}.csv'})


@app.route('/api/export/skus')
def api_export_skus():
    """Export SKU summary as CSV."""
    report = ds.get_latest_report()
    if not report:
        abort(404)
    kpis = ds.get_all_brand_kpis(report['id'])
    import csv, io as _io
    buf = _io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['Brand', 'Unique SKUs', 'Total Revenue', 'Revenue per SKU'])
    for bk in kpis:
        rev_per = (bk['total_revenue'] / bk['unique_skus']) if bk['unique_skus'] else 0
        writer.writerow([bk['brand_name'], bk['unique_skus'],
                         _money_csv_2dp(bk['total_revenue']), _money_csv_2dp(rev_per)])
    buf.seek(0)
    return app.response_class(buf.getvalue(), mimetype='text/csv',
                               headers={'Content-Disposition':
                                        f'attachment; filename=skus_{report["month_label"]}.csv'})


@app.route('/api/export/alerts')
def api_export_alerts():
    """Export alerts as CSV."""
    alerts = ds.get_alerts()
    import csv, io as _io
    buf = _io.StringIO()
    if alerts:
        writer = csv.DictWriter(buf, fieldnames=['brand_name', 'alert_type', 'severity',
                                                  'message', 'created_at', 'acknowledged'])
        writer.writeheader()
        for a in alerts:
            writer.writerow({k: a.get(k, '') for k in writer.fieldnames})
    buf.seek(0)
    return app.response_class(buf.getvalue(), mimetype='text/csv',
                               headers={'Content-Disposition': 'attachment; filename=alerts.csv'})


# ── Activity Log API ──────────────────────────────────────────────────────────

@app.route('/activity-intelligence')
def activity_intelligence():
    alert_count = ds.get_unacknowledged_count()
    latest_report = ds.get_latest_report()
    report_id = request.args.get('report_id', latest_report['id'] if latest_report else None, type=int)
    batch_id = request.args.get('batch_id', type=int)
    brand_name = (request.args.get('brand') or '').strip() or None
    store_code = (request.args.get('store') or '').strip() or None

    if store_code:
        store_summary = ds.get_retailer_activity_summary(store_code, report_id=report_id)
        issue_counts = Counter((item.get('issue_type') or 'unspecified') for item in (store_summary.get('issues') or []))
        salesperson_counts = Counter((item.get('salesman_name') or 'Unknown') for item in (store_summary.get('visits') or []))
        top_issues = [
            {'issue_type': issue_type, 'count': count}
            for issue_type, count in issue_counts.most_common(8)
        ]
        top_salespeople = [
            {'salesman_name': name, 'visits': count}
            for name, count in salesperson_counts.most_common(8)
        ]
        summary = {
            'totals': {
                'events': int((store_summary.get('totals') or {}).get('events') or 0),
                'visits': int(store_summary.get('visit_count') or 0),
                'issues': int(store_summary.get('issue_count') or 0),
                'stores': 1 if store_summary.get('store') or store_code else 0,
                'salespeople': int((store_summary.get('totals') or {}).get('salespeople') or 0),
                'active_days': int((store_summary.get('totals') or {}).get('active_days') or 0),
                'opportunities': 0,
            },
            'recent_issues': store_summary.get('issues') or [],
            'recent_visits': store_summary.get('visits') or [],
            'top_issues': top_issues,
            'top_brands': store_summary.get('brands') or [],
            'top_salespeople': top_salespeople,
            'store': store_summary.get('store'),
            'store_name': (store_summary.get('store') or {}).get('retailer_name') or store_code,
            'store_view': True,
        }
    else:
        summary = ds.get_activity_summary(batch_id=batch_id, report_id=report_id, brand_name=brand_name)

    batches = ds.get_activity_batches(limit=30)
    available_stores = ds.list_activity_retailers(report_id=report_id, limit=500)
    return render_template(
        'portal/activity_intelligence.html',
        alert_count=alert_count,
        report=ds.get_report(report_id) if report_id else None,
        reports=ds.get_all_reports(),
        batches=batches,
        selected_batch_id=batch_id,
        selected_brand=brand_name,
        selected_store=store_code,
        available_stores=available_stores,
        summary=summary,
    )


@app.route('/retailer/<path:retailer_code>')
def retailer_detail(retailer_code):
    alert_count = ds.get_unacknowledged_count()
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    if not coach_history_available():
        abort(404)
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    return render_template(
        'portal/store_360.html',
        alert_count=alert_count,
        retailer_code=retailer_code,
        detail=detail,
        coach=coach,
        report=ds.get_report(detail.get('report_id')) if detail.get('report_id') else (ds.get_report(report_id) if report_id else ds.get_latest_report()),
        reports=ds.get_all_reports(),
        selected_month=month_value or '',
    )


@app.route('/retailer-groups/<path:group_slug>')
def retailer_group_detail(group_slug):
    alert_count = ds.get_unacknowledged_count()
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    if not coach_history_available():
        abort(404)
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        abort(404)
    coach = _coach_summary_for_snapshot(detail, persist=True)
    return render_template(
        'portal/retailer_group.html',
        alert_count=alert_count,
        group_slug=group_slug,
        detail=detail,
        coach=coach,
        report=ds.get_report(detail.get('report_id')) if detail.get('report_id') else (ds.get_report(report_id) if report_id else ds.get_latest_report()),
        reports=ds.get_all_reports(),
        selected_month=month_value or '',
    )


@app.route('/store-360/<path:retailer_code>')
def store_360(retailer_code):
    return redirect(url_for('retailer_detail', retailer_code=retailer_code, **request.args.to_dict()))


@app.route('/copilot')
def copilot_dashboard():
    alert_count = ds.get_unacknowledged_count()
    report = ds.get_latest_report()
    if report:
        build_default_agent_actions(ds, report)
    pending_actions = ds.list_agent_actions(status='pending', limit=25)
    summary = ds.get_activity_summary(report_id=report['id']) if report else {'totals': {}}
    return render_template(
        'portal/copilot.html',
        alert_count=alert_count,
        report=report,
        pending_actions=pending_actions,
        activity_summary=summary,
    )


@app.route('/agent-actions')
def agent_actions_page():
    alert_count = ds.get_unacknowledged_count()
    report = ds.get_latest_report()
    if report:
        build_default_agent_actions(ds, report)
    status = request.args.get('status', 'pending')
    actions = ds.list_agent_actions(status=status, limit=100)
    return render_template(
        'portal/agent_actions.html',
        alert_count=alert_count,
        report=report,
        actions=actions,
        selected_status=status,
    )


def _build_copilot_state_payload(context):
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    if report:
        build_default_agent_actions(ds, report)
    pending_total = len(ds.list_agent_actions(status='pending', limit=100))
    payload = {
        'context': context,
        'welcome': _copilot_welcome_text(context),
        'prompts': _copilot_prompt_suggestions(context),
        'actions': _copilot_actions_for_context(context, limit=6),
        'pending_total': pending_total,
        'execution_states': ['thinking', 'planning', 'executing', 'waiting', 'job_running', 'completed', 'failed'],
        'report': {
            'id': report['id'],
            'month_label': report['month_label'],
            'report_type': report.get('report_type'),
        } if report else None,
        'activity_totals': {},
        'memory_preview': [],
        'schedule_preview': ds.list_assistant_jobs(limit=6),
        'pending_actions': ds.list_agent_actions(status='pending', limit=12),
    }
    subject_type = 'system'
    subject_key = 'global'
    if context.get('brand_name'):
        subject_type = 'brand'
        subject_key = context['brand_name']
    elif context.get('retailer_code'):
        subject_type = 'store'
        subject_key = context['retailer_code']
    elif report:
        subject_type = 'report'
        subject_key = str(report['id'])
    payload['memory_preview'] = ds.list_agent_memories(
        limit=6,
        subject_type=subject_type,
        subject_key=subject_key,
    )
    if report:
        activity_summary = ds.get_activity_summary(
            batch_id=context.get('batch_id'),
            report_id=report['id'],
            brand_name=context.get('brand_name'),
        )
        payload['activity_totals'] = activity_summary.get('totals', {})
    if context.get('brand_name') and report:
        brand_kpis = ds.get_brand_kpis_single(report['id'], context['brand_name'])
        payload['brand_snapshot'] = {
            'brand_name': context['brand_name'],
            'revenue': float(brand_kpis.get('total_revenue', 0)) if brand_kpis else 0,
            'stores': int(brand_kpis.get('num_stores', 0)) if brand_kpis else 0,
            'repeat_pct': float(brand_kpis.get('repeat_pct', 0)) if brand_kpis else 0,
        } if brand_kpis else None
    if context.get('retailer_code'):
        store_summary = ds.get_store_activity_summary(context['retailer_code'])
        if store_summary.get('store'):
            payload['store_snapshot'] = {
                'retailer_name': store_summary['store'].get('retailer_name'),
                'visit_count': int(store_summary.get('visit_count', 0)),
                'issue_count': int(store_summary.get('issue_count', 0)),
                'brand_mentions': int(store_summary.get('brand_mentions', 0)),
            }
    return payload


@app.route('/api/activity')
def api_activity():
    limit = min(int(request.args.get('limit', 50)), 200)
    return jsonify(ds.get_activity_log(limit=limit))


@app.route('/api/activity/import', methods=['POST'])
def api_activity_import():
    uploaded = request.files.get('file') or request.files.get('activity_file')
    if not uploaded or uploaded.filename == '':
        return jsonify({'success': False, 'error': 'No activity file uploaded.'}), 400

    file_bytes = uploaded.read()
    if not file_bytes:
        return jsonify({'success': False, 'error': 'The uploaded activity file is empty.'}), 400

    filename = uploaded.filename or 'activity_upload'
    explicit_report_id = request.form.get('report_id', type=int)
    job_id = uuid.uuid4().hex
    ds.create_job(job_id)
    ds.update_job(job_id, progress=3, current_brand='Preparing activity import')

    def _run():
        report_id = explicit_report_id
        try:
            ds.update_job(job_id, progress=8, current_brand='Reading activity file')
            df, meta = load_activity_dataframe(file_bytes)

            ds.update_job(
                job_id,
                progress=18,
                total=len(df),
                current_brand='Matching activity dates to a report',
            )

            if not report_id and not df.empty:
                inferred = ds.find_report_covering_range(df['activity_date'].min(), df['activity_date'].max())
                report_id = inferred['id'] if inferred else None

            def _progress(progress_value, message):
                ds.update_job(
                    job_id,
                    progress=max(18, min(int(progress_value), 95)),
                    current_brand=message,
                    report_id=report_id,
                )

            payload = build_activity_payload(
                df,
                ds=ds,
                source_filename=filename,
                report_id=report_id,
                progress_cb=_progress,
            )

            ds.update_job(job_id, progress=92, current_brand='Saving activity summary', report_id=report_id)
            batch_id = ds.save_activity_import(
                payload,
                source_filename=filename,
                source_type=meta.get('source_type'),
                report_id=report_id,
            )

            linked_report = ds.get_report(report_id) if report_id else None
            if linked_report:
                build_default_agent_actions(ds, linked_report)

            top_brands = (payload.get('summary') or {}).get('top_brands') or []
            primary_brand = top_brands[0].get('brand_name') if top_brands else None

            ds.update_job(
                job_id,
                progress=97,
                current_brand='Refreshing Activity summaries',
                report_id=report_id,
                result_json={
                    'batch_id': batch_id,
                    'report_id': report_id,
                    'summary': payload.get('summary', {}),
                },
            )

            _refresh_copilot_state(
                report_id=report_id,
                batch_id=batch_id,
                brand_name=primary_brand,
                reason=f'activity_import:{filename}',
                source='activity_import',
            )
            coach_job_id = _launch_coach_refresh_job(
                mode='current',
                report_id=report_id,
                include_pairs=True,
                source='activity_import',
            ) if report_id else None

            ds.update_job(
                job_id,
                status='done',
                progress=100,
                current_brand='Activity import complete',
                report_id=report_id,
                result_json={
                    'batch_id': batch_id,
                    'report_id': report_id,
                    'summary': payload.get('summary', {}),
                    'coach_job_id': coach_job_id,
                },
            )
        except Exception as exc:
            ds.update_job(
                job_id,
                status='error',
                current_brand='Activity import failed',
                report_id=report_id,
                error_msg=str(exc),
            )

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/activity/summary')
def api_activity_summary():
    batch_id = request.args.get('batch_id', type=int)
    report_id = request.args.get('report_id', type=int)
    brand_name = (request.args.get('brand_name') or '').strip() or None
    return jsonify(ds.get_activity_summary(batch_id=batch_id, report_id=report_id, brand_name=brand_name))


@app.route('/api/activity/job/<job_id>')
def api_activity_job(job_id):
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


@app.route('/api/activity/brand/<path:brand_name>')
def api_activity_brand_summary(brand_name):
    return jsonify(ds.get_activity_brand_summary(brand_name))


@app.route('/api/activity/store/<path:retailer_code>')
def api_activity_store_summary(retailer_code):
    return jsonify(ds.get_store_activity_summary(retailer_code))


@app.route('/api/retailers')
def api_retailers():
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    dataset = build_retailer_index(ds, report_id=report_id, month_value=month_value)
    dataset['rows'] = _decorate_retailer_rows(dataset.get('rows', []))
    return jsonify({'success': True, **dataset})


@app.route('/api/retailer-groups')
def api_retailer_groups():
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    dataset = build_retailer_group_index(ds, report_id=report_id, month_value=month_value)
    dataset['rows'] = _decorate_retailer_group_rows(dataset.get('rows', []))
    return jsonify({'success': True, **dataset})


@app.route('/api/retailers/<path:retailer_code>')
def api_retailer_detail(retailer_code):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        return jsonify({'success': False, 'error': 'Retailer not found'}), 404
    coach = _coach_summary_for_snapshot(detail, persist=True)
    return jsonify({'success': True, 'detail': detail, 'coach': coach})


@app.route('/api/retailer-groups/<path:group_slug>')
def api_retailer_group_detail(group_slug):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        return jsonify({'success': False, 'error': 'Retailer group not found'}), 404
    coach = _coach_summary_for_snapshot(detail, persist=True)
    return jsonify({'success': True, 'detail': detail, 'coach': coach})


@app.route('/api/retailers/<path:retailer_code>/brands')
def api_retailer_brands(retailer_code):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_detail(ds, retailer_code, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        return jsonify({'success': False, 'error': 'Retailer not found'}), 404
    return jsonify({'success': True, 'brands': detail.get('brand_rows', [])})


@app.route('/api/retailer-groups/<path:group_slug>/branches')
def api_retailer_group_branches(group_slug):
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    detail = build_retailer_group_detail(ds, group_slug, report_id=report_id, month_value=month_value)
    if not detail.get('period_start'):
        return jsonify({'success': False, 'error': 'Retailer group not found'}), 404
    return jsonify({'success': True, 'branches': detail.get('branch_rows', [])})


@app.route('/api/coach/signals')
def api_coach_signals():
    scope_type = (request.args.get('scope_type') or '').strip() or None
    scope_key = (request.args.get('scope_key') or '').strip() or None
    status = (request.args.get('status') or 'open').strip().lower()
    signal_type = (request.args.get('signal_type') or '').strip() or None
    signals = ds.list_coach_signals(scope_type=scope_type, scope_key=scope_key, status=status, signal_type=signal_type, limit=min(int(request.args.get('limit', 25)), 100))
    return jsonify({'success': True, 'signals': signals})


@app.route('/api/coach/entity')
def api_coach_entity():
    scope_type = (request.args.get('scope_type') or 'portfolio').strip().lower()
    scope_key = (request.args.get('scope_key') or '').strip() or None
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    retailer_code = (request.args.get('retailer_code') or '').strip() or None
    if scope_type == 'retailer' and not scope_key:
        scope_key = retailer_code
    snapshot = build_scope_snapshot(ds, scope_type, scope_key=scope_key, report_id=report_id, month_value=month_value, retailer_code=retailer_code, persist=True)
    coach = _coach_summary_for_snapshot(snapshot, persist=True)
    return jsonify({'success': True, 'snapshot': snapshot, 'coach': coach})


@app.route('/api/coach/summary')
def api_coach_summary():
    scope_type = (request.args.get('scope_type') or 'portfolio').strip().lower()
    scope_key = (request.args.get('scope_key') or '').strip() or None
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    retailer_code = (request.args.get('retailer_code') or '').strip() or None
    if scope_type == 'retailer' and not scope_key:
        scope_key = retailer_code
    snapshot = build_scope_snapshot(ds, scope_type, scope_key=scope_key, report_id=report_id, month_value=month_value, retailer_code=retailer_code, persist=True)
    coach = _coach_summary_for_snapshot(snapshot, persist=True)
    return jsonify({'success': True, 'summary': coach.get('summary'), 'signals': coach.get('signals', []), 'action_items': coach.get('action_items', []), 'recommendation_items': coach.get('recommendation_items', []), 'snapshot_meta': {
        'scope_type': snapshot.get('scope_type'),
        'scope_key': snapshot.get('scope_key'),
        'period_label': snapshot.get('period_label'),
        'report_id': snapshot.get('report_id'),
    }})


@app.route('/api/coach/thresholds')
def api_coach_thresholds():
    return jsonify({'success': True, 'thresholds': get_signal_thresholds()})


@app.route('/api/coach/thresholds/validate')
def api_coach_thresholds_validate():
    report_id = request.args.get('report_id', type=int)
    month_value = (request.args.get('month') or '').strip() or None
    include_pairs = str(request.args.get('include_pairs', 'false')).lower() in {'1', 'true', 'yes'}
    sample_limit = min(max(request.args.get('sample_limit', 5, type=int), 1), 15)
    result = validate_signal_quality(
        ds,
        report_id=report_id,
        month_value=month_value,
        include_pairs=include_pairs,
        sample_limit=sample_limit,
    )
    return jsonify({'success': True, **result})


@app.route('/api/coach/backfill', methods=['POST'])
def api_coach_backfill():
    payload = request.get_json(silent=True) or request.form or {}
    mode = str(payload.get('mode') or 'current').strip().lower()
    include_pairs = str(payload.get('include_pairs', 'true')).lower() in {'1', 'true', 'yes'}
    try:
        report_id = int(payload.get('report_id')) if payload.get('report_id') not in (None, '') else None
    except Exception:
        report_id = None
    month_value = (payload.get('month') or '').strip() or None
    monthly_count = min(max(int(payload.get('monthly_count') or 6), 1), 24)
    weekly_count = min(max(int(payload.get('weekly_count') or 4), 0), 24)
    job_id = _launch_coach_refresh_job(
        mode=mode,
        report_id=report_id,
        month_value=month_value,
        monthly_count=monthly_count,
        weekly_count=weekly_count,
        include_pairs=include_pairs,
        source='manual_api',
    )
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/coach/job/<job_id>')
def api_coach_job(job_id):
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    return jsonify({'success': True, **job})


@app.route('/api/coach/pin', methods=['POST'])
def api_coach_pin():
    payload = request.get_json(silent=True) or request.form or {}
    scope_type = str(payload.get('scope_type') or 'portfolio').strip().lower()
    scope_key = str(payload.get('scope_key') or 'global').strip()
    try:
        report_id = int(payload.get('report_id')) if payload.get('report_id') not in (None, '') else None
    except Exception:
        report_id = None
    month_value = (payload.get('month') or '').strip() or None
    snapshot = build_scope_snapshot(
        ds,
        scope_type,
        scope_key=scope_key if scope_type != 'portfolio' else 'global',
        report_id=report_id,
        month_value=month_value,
        retailer_code=scope_key if scope_type == 'brand_retailer' else None,
        persist=True,
    )
    coach = _coach_summary_for_snapshot(snapshot, persist=True)
    memory_id = ds.save_agent_memory(
        scope_type=scope_type,
        scope_key=scope_key,
        memory_text=f"{coach.get('summary', {}).get('headline')}: {coach.get('summary', {}).get('summary')}",
        memory_kind='coach_summary',
        confidence=0.86,
        source='coach_pin',
        memory_layer='workspace',
        tags=['coach', scope_type, 'pinned'],
        related_report_id=snapshot.get('report_id'),
        related_brand=scope_key if scope_type == 'brand' else None,
        pinned=True,
        metadata={
            'signals': [signal.get('signal_type') for signal in coach.get('signals', [])[:6]],
            'period_label': snapshot.get('period_label'),
        },
    )
    return jsonify({'success': True, 'memory': ds.get_agent_memory(memory_id)})


@app.route('/api/coach/recommendations/outcome', methods=['POST'])
def api_coach_recommendation_outcome():
    payload = request.get_json(silent=True) or request.form or {}
    scope_type = str(payload.get('scope_type') or 'brand').strip().lower()
    scope_key = str(payload.get('scope_key') or '').strip()
    recommendation_key = str(payload.get('recommendation_key') or '').strip()
    recommendation_label = str(payload.get('recommendation_label') or recommendation_key).strip()
    outcome_type = str(payload.get('outcome_type') or '').strip().lower()
    note = str(payload.get('note') or '').strip() or None
    if not scope_key or not recommendation_key or not outcome_type:
        return jsonify({'success': False, 'error': 'scope_key, recommendation_key, and outcome_type are required.'}), 400
    try:
        report_id = int(payload.get('report_id')) if payload.get('report_id') not in (None, '') else None
    except Exception:
        report_id = None
    try:
        outcome_value = float(payload.get('outcome_value')) if payload.get('outcome_value') not in (None, '') else None
    except Exception:
        outcome_value = None
    brand_name = scope_key if scope_type == 'brand' else None
    ds.save_recommendation_outcome(
        brand_name=brand_name,
        recommendation_key=recommendation_key,
        recommendation_label=recommendation_label,
        outcome_type=outcome_type,
        outcome_value=outcome_value,
        note=note,
        report_id=report_id,
        scope_type=scope_type,
        scope_key=scope_key,
        actor='portal_user',
        metadata={'source': 'coach_ui'},
    )
    return jsonify({
        'success': True,
        'scores': ds.get_recommendation_outcome_scores(
            brand_name=brand_name,
            scope_type=scope_type,
            scope_key=scope_key,
        ).get(recommendation_key, {})
    })


@app.route('/api/coach/run-due', methods=['POST'])
def api_coach_run_due():
    now = datetime.now().isoformat(timespec='seconds')
    ran = []
    skipped = []
    for job in ds.list_assistant_jobs(status='active', limit=250):
        if job.get('job_type') != 'coach_refresh':
            continue
        next_run = str(job.get('next_run') or '').strip()
        if not next_run or next_run > now:
            skipped.append(job.get('id'))
            continue
        payload = job.get('payload') or {}
        result = execute_admin_request(
            ds,
            'run_schedule_now',
            arguments={'schedule_id': job.get('id')},
            report=ds.get_report(payload.get('report_id')) if payload.get('report_id') else ds.get_latest_report(),
        )
        ran.append({'schedule_id': job.get('id'), 'status': result.get('status'), 'message': result.get('message')})
    return jsonify({'success': True, 'ran': ran, 'skipped': skipped})


@app.route('/api/copilot/state')
def api_copilot_state():
    context = _current_copilot_context(request.args.to_dict())
    return jsonify({'success': True, **_build_copilot_state_payload(context)})


@app.route('/api/copilot/query', methods=['POST'])
def api_copilot_query():
    payload = request.get_json(silent=True) or request.form or {}
    question = str(payload.get('question') or '').strip()
    if not question:
        return jsonify({'success': False, 'error': 'question is required'}), 400
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    if report:
        build_default_agent_actions(ds, report)
    result = answer_admin_query(
        ds,
        question,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        page_context=context,
        confirmation_token=payload.get('confirmation_token'),
        operator_mode=bool(payload.get('operator_mode', True)),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({
        'success': True,
        **result,
        **_build_copilot_state_payload(context),
    })


@app.route('/api/copilot/execute', methods=['POST'])
def api_copilot_execute():
    payload = request.get_json(silent=True) or request.form or {}
    tool_name = str(payload.get('tool_name') or '').strip()
    if not tool_name:
        return jsonify({'success': False, 'error': 'tool_name is required'}), 400
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    result = execute_admin_request(
        ds,
        tool_name,
        arguments=payload.get('arguments') or {},
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        confirmation_token=payload.get('confirmation_token'),
        operator_mode=bool(payload.get('operator_mode', True)),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({
        'success': True,
        **result,
        **_build_copilot_state_payload(context),
    })


@app.route('/api/copilot/memory')
def api_copilot_memory():
    limit = min(int(request.args.get('limit', 25)), 100)
    pinned_raw = request.args.get('pinned')
    pinned = None if pinned_raw in (None, '', 'all') else pinned_raw.lower() in {'1', 'true', 'yes'}
    memories = ds.list_agent_memories(
        limit=limit,
        memory_layer=request.args.get('memory_layer') or None,
        pinned=pinned,
        subject_type=request.args.get('subject_type') or None,
        subject_key=request.args.get('subject_key') or None,
        query=request.args.get('query') or None,
    )
    return jsonify({'success': True, 'items': memories})


@app.route('/api/copilot/memory/pin', methods=['POST'])
def api_copilot_memory_pin():
    payload = request.get_json(silent=True) or request.form or {}
    memory_id = int(payload.get('memory_id') or 0)
    if not memory_id:
        return jsonify({'success': False, 'error': 'memory_id is required'}), 400
    memory = ds.pin_agent_memory(memory_id, pinned=bool(payload.get('pinned', True)))
    return jsonify({'success': True, 'memory': memory})


@app.route('/api/copilot/connectors')
def api_copilot_connectors():
    context = _current_copilot_context(request.args.to_dict())
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    result = execute_admin_request(
        ds,
        'list_connectors',
        arguments={},
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/copilot/connectors/<connector>/run', methods=['POST'])
def api_copilot_connector_run(connector):
    payload = request.get_json(silent=True) or request.form or {}
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    arguments = dict(payload.get('arguments') or {})
    arguments['connector'] = connector
    if payload.get('action') and 'action' not in arguments:
        arguments['action'] = payload.get('action')
    result = execute_admin_request(
        ds,
        'run_connector',
        arguments=arguments,
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        confirmation_token=payload.get('confirmation_token'),
        operator_mode=bool(payload.get('operator_mode', True)),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/copilot/schedules', methods=['GET', 'POST'])
def api_copilot_schedules():
    if request.method == 'GET':
        status = request.args.get('status', 'all')
        limit = min(int(request.args.get('limit', 25)), 100)
        return jsonify({'success': True, 'items': ds.list_assistant_jobs(status=status, limit=limit)})

    payload = request.get_json(silent=True) or request.form or {}
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    arguments = dict(payload.get('arguments') or {})
    for key in ('label', 'job_type', 'target', 'connector', 'cadence', 'next_run', 'status'):
        if payload.get(key) is not None and key not in arguments:
            arguments[key] = payload.get(key)
    if payload.get('payload') is not None and 'payload' not in arguments:
        arguments['payload'] = payload.get('payload')
    result = execute_admin_request(
        ds,
        'create_schedule',
        arguments=arguments,
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/copilot/schedules/<int:schedule_id>/pause', methods=['POST'])
def api_copilot_schedule_pause(schedule_id):
    payload = request.get_json(silent=True) or request.form or {}
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    result = execute_admin_request(
        ds,
        'pause_schedule',
        arguments={'schedule_id': schedule_id},
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/copilot/schedules/<int:schedule_id>/resume', methods=['POST'])
def api_copilot_schedule_resume(schedule_id):
    payload = request.get_json(silent=True) or request.form or {}
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    result = execute_admin_request(
        ds,
        'resume_schedule',
        arguments={'schedule_id': schedule_id},
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/copilot/schedules/<int:schedule_id>/run-now', methods=['POST'])
def api_copilot_schedule_run_now(schedule_id):
    payload = request.get_json(silent=True) or request.form or {}
    context = _current_copilot_context(payload.get('page_context') or payload)
    report_id = context.get('report_id')
    report = ds.get_report(report_id) if report_id else ds.get_latest_report()
    result = execute_admin_request(
        ds,
        'run_schedule_now',
        arguments={'schedule_id': schedule_id},
        page_context=context,
        report=report,
        brand_name=context.get('brand_name'),
        retailer_code=context.get('retailer_code'),
        batch_id=context.get('batch_id'),
        confirmation_token=payload.get('confirmation_token'),
        operator_mode=bool(payload.get('operator_mode', True)),
        idempotency_key=payload.get('idempotency_key'),
    )
    return jsonify({'success': True, **result})


@app.route('/api/agent-actions')
def api_agent_actions():
    status = request.args.get('status', 'pending')
    report = ds.get_latest_report()
    if report:
        build_default_agent_actions(ds, report)
    return jsonify(ds.list_agent_actions(status=status, limit=100))


@app.route('/api/agent-actions/<int:action_id>/approve', methods=['POST'])
def api_agent_action_approve(action_id):
    note = (request.get_json(silent=True) or {}).get('note')
    action = ds.update_agent_action_status(action_id, 'approved', actor='admin', note=note)
    if not action:
        return jsonify({'success': False, 'error': 'Action not found'}), 404
    _refresh_copilot_state(
        report_id=action.get('report_id'),
        brand_name=action.get('subject_key') if action.get('subject_type') == 'brand' else None,
        retailer_code=action.get('subject_key') if action.get('subject_type') == 'store' else None,
        reason=f'agent_action_approved:{action_id}',
        source='agent_feedback',
    )
    return jsonify({'success': True, 'action': action})


@app.route('/api/agent-actions/<int:action_id>/reject', methods=['POST'])
def api_agent_action_reject(action_id):
    note = (request.get_json(silent=True) or {}).get('note')
    action = ds.update_agent_action_status(action_id, 'rejected', actor='admin', note=note)
    if not action:
        return jsonify({'success': False, 'error': 'Action not found'}), 404
    _refresh_copilot_state(
        report_id=action.get('report_id'),
        brand_name=action.get('subject_key') if action.get('subject_type') == 'brand' else None,
        retailer_code=action.get('subject_key') if action.get('subject_type') == 'store' else None,
        reason=f'agent_action_rejected:{action_id}',
        source='agent_feedback',
    )
    return jsonify({'success': True, 'action': action})


# ── AI Narrative API ───────────────────────────────────────────────────────────

@app.route('/api/narrative/<path:brand_name>')
def api_narrative_brand(brand_name):
    """
    GET: Return cached AI narrative for a brand (from latest report).
    POST ?regenerate=1: Force regenerate a fresh narrative from Gemini.
    """
    if not gemini_available():
        return jsonify({'success': False, 'error': 'GEMINI_API_KEY not configured'}), 503

    report = ds.get_latest_report()
    if not report:
        return jsonify({'success': False, 'error': 'No report data'}), 404

    report_id  = report['id']
    regenerate = request.args.get('regenerate') == '1'

    # Try cache first
    if not regenerate:
        cached = ds.get_narrative(report_id, brand_name)
        if cached:
            return jsonify({'success': True, 'narrative': cached, 'cached': True})

    # Generate fresh
    kpis = ds.get_brand_kpis_single(report_id, brand_name)
    if not kpis:
        return jsonify({'success': False, 'error': 'Brand not found in latest report'}), 404

    total_portfolio_revenue = sum(
        b['total_revenue'] for b in ds.get_all_brand_kpis(report_id)
    )
    portfolio_avg = total_portfolio_revenue / max(
        len(ds.get_all_brand_kpis(report_id)), 1
    )
    history = ds.get_brand_history(brand_name, limit=6)

    try:
        narrative, _ = generate_brand_narrative(brand_name, kpis, history, portfolio_avg)
        if narrative:
            ds.save_narrative(report_id, brand_name, narrative)
            ds.log_activity('ai_narrative', f'Generated narrative for {brand_name}', brand_name, report_id)
        return jsonify({'success': True, 'narrative': narrative, 'cached': False})
    except Exception as exc:
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/recommendations/<path:brand_name>', methods=['GET', 'POST'])
def api_recommendations(brand_name):
    """
    GET: Return cached recommendations for a brand.
    POST: Regenerate recommendations on demand.
    """
    report = ds.get_latest_report()
    if not report:
        return jsonify({'success': False, 'error': 'No report data'}), 404

    report_id = report['id']
    regen = request.method == 'POST' or request.args.get('regenerate') == '1'
    cache_key = f'__rec__{brand_name}'

    if not regen:
        cached = ds.get_narrative(report_id, cache_key)
        if cached:
            return jsonify({'success': True, 'recommendations': cached, 'cached': True, 'source': 'cache'})
        return jsonify({
            'success': True,
            'recommendations': None,
            'cached': False,
            'needs_generation': True,
            'source': 'pending',
        })

    kpis = ds.get_brand_kpis_single(report_id, brand_name)
    if not kpis:
        return jsonify({'success': False, 'error': 'No KPIs found for this brand'}), 404

    churn_data = ds.get_store_churn(report_id, brand_name)
    all_kpis = ds.get_all_brand_kpis(report_id)
    portfolio_avg = sum(b['total_revenue'] for b in all_kpis) / max(len(all_kpis), 1)

    try:
        from modules.narrative_ai import generate_recommendations
        text, source = generate_recommendations(brand_name, kpis, churn_data, portfolio_avg)
        if text:
            ds.save_narrative(report_id, cache_key, text)
        return jsonify({'success': True, 'recommendations': text, 'cached': False, 'source': source})
    except Exception as exc:
        app.logger.exception('Recommendations generation failed for %s', brand_name)
        return jsonify({
            'success': False,
            'error': 'Recommended actions are temporarily unavailable. Try again shortly.',
        }), 503


@app.route('/api/narrative/portfolio')
def api_narrative_portfolio():
    """Return AI executive summary for the entire portfolio."""
    if not gemini_available():
        return jsonify({'success': False, 'error': 'GEMINI_API_KEY not configured'}), 503

    report = ds.get_latest_report()
    if not report:
        return jsonify({'success': False, 'error': 'No report data'}), 404

    report_id  = report['id']
    regenerate = request.args.get('regenerate') == '1'

    if not regenerate:
        cached = ds.get_narrative(report_id, '__portfolio__')
        if cached:
            return jsonify({'success': True, 'narrative': cached, 'cached': True})

    all_kpis_rows = ds.get_all_brand_kpis(report_id)
    all_kpis = {bk['brand_name']: bk for bk in all_kpis_rows}

    try:
        narrative = generate_portfolio_narrative(all_kpis, report)
        if narrative:
            ds.save_narrative(report_id, '__portfolio__', narrative)
        return jsonify({'success': True, 'narrative': narrative, 'cached': False})
    except Exception as exc:
        return jsonify({'success': False, 'error': str(exc)}), 500


# ── WhatsApp Conversational Bot ────────────────────────────────────────────────

@app.route('/webhook/whatsapp', methods=['POST'])
def whatsapp_webhook():
    """
    Twilio WhatsApp webhook — conversational bot.

    Commands (case-insensitive):
      REPORT           → Latest portfolio summary
      REPORT <brand>   → Brand-specific summary
      ALERTS           → Unacknowledged alerts count + top 3
      BRANDS           → List of all active brands
      HELP             → Command list

    Configure in Twilio Console → WhatsApp Sandbox → When a message comes in:
    https://your-railway-domain.railway.app/webhook/whatsapp
    """
    from urllib.parse import quote

    body    = request.form.get('Body', '').strip()
    sender  = request.form.get('From', '')
    cmd     = body.upper().strip()

    def twiml_reply(text):
        # Sanitize text to avoid XML injection
        safe = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        return app.response_class(
            f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<Response><Message>{safe}</Message></Response>',
            mimetype='text/xml'
        )

    import difflib

    def fuzzy_brand(query, all_brands):
        ql = query.lower()
        exact = next((b for b in all_brands if ql in b.lower()), None)
        if exact:
            return exact
        close = difflib.get_close_matches(query, all_brands, n=1, cutoff=0.5)
        return close[0] if close else None

    report = ds.get_latest_report()

    # ── HELP ──────────────────────────────────────────────────────────────────
    if cmd in ('HELP', 'HI', 'HELLO', 'START', '?'):
        return twiml_reply(
            "DALA Analytics Bot\n\n"
            "Commands:\n"
            "• STATUS [brand] — Brand KPIs + WoW\n"
            "• TOP [n] — Top N brands (default 5)\n"
            "• REPORT — Portfolio summary\n"
            "• REPORT [brand] — Brand summary\n"
            "• ALERTS — Active alerts\n"
            "• BRANDS — All brand partners\n"
            "• HELP — Show this menu\n\n"
            f"Latest data: {report['month_label'] if report else 'No data yet'}"
        )

    # ── STATUS <brand> ────────────────────────────────────────────────────────
    if cmd.startswith('STATUS'):
        if not report:
            return twiml_reply("No report data yet.")
        brand_query = body[6:].strip()
        all_brands = ds.get_all_brands_in_db()
        matched = fuzzy_brand(brand_query, all_brands) if brand_query else None
        if not matched:
            return twiml_reply(
                "Usage: STATUS [brand name]\nExample: STATUS Zayith\n"
                "Send BRANDS to see all brand names."
            )
        kpis = ds.get_brand_kpis_single(report['id'], matched)
        if not kpis:
            return twiml_reply(f"{matched} has no data in {report['month_label']}.")
        wow = kpis.get('wow_rev_change', 0)
        wow_str = (f"▲{wow}%" if wow > 0 else (f"▼{abs(wow)}%" if wow < 0 else "—")) + " WoW"
        lines = [
            f"{matched} — {report['month_label']}",
            f"Revenue: N{kpis.get('total_revenue',0):,.0f} ({wow_str})",
            f"Qty: {kpis.get('total_qty',0):,.1f} packs",
            f"Stores: {kpis.get('num_stores',0)} | Repeat: {kpis.get('repeat_pct',0):.1f}%",
            f"Avg Rev / Supermarket: N{kpis.get('avg_revenue_per_store',0):,.0f}",
            f"Stock Days: {kpis.get('stock_days_cover',0):.0f}",
        ]
        return twiml_reply('\n'.join(lines))

    # ── TOP [n] ───────────────────────────────────────────────────────────────
    if cmd.startswith('TOP'):
        if not report:
            return twiml_reply("No report data yet.")
        parts = cmd.split()
        n = 5
        if len(parts) > 1:
            try:
                n = min(int(parts[1]), 20)
            except ValueError:
                pass
        kpis_all = ds.get_all_brand_kpis(report['id'])
        top_brands = sorted(kpis_all, key=lambda x: x.get('total_revenue', 0), reverse=True)[:n]
        lines = [f"Top {n} Brands — {report['month_label']}:"]
        for i, b in enumerate(top_brands, 1):
            lines.append(f"{i}. {b['brand_name']}: N{b['total_revenue']:,.0f}")
        return twiml_reply('\n'.join(lines))

    # ── ALERTS ────────────────────────────────────────────────────────────────
    if cmd in ('ALERTS', 'ALERT'):
        alerts = ds.get_alerts(unacknowledged_only=True)
        count = len(alerts)
        if count == 0:
            return twiml_reply("No unacknowledged alerts.")
        lines = [f"Alerts ({count} active):"]
        for a in alerts[:4]:
            sev = a.get('severity', '').upper()
            lines.append(f"[{sev}] {a.get('brand_name','Portfolio')}: {a.get('message','')[:80]}")
        return twiml_reply('\n'.join(lines))

    # ── BRANDS ────────────────────────────────────────────────────────────────
    if cmd == 'BRANDS':
        brands_list = ds.get_all_brands_in_db()
        if not brands_list:
            return twiml_reply("No brand data yet. Import a report first.")
        return twiml_reply(
            f"Active Brands ({len(brands_list)}):\n" +
            '\n'.join(f"• {b}" for b in sorted(brands_list)[:20]) +
            ('\n...and more' if len(brands_list) > 20 else '')
        )

    # ── REPORT [brand] ────────────────────────────────────────────────────────
    if cmd.startswith('REPORT'):
        if not report:
            return twiml_reply("No report data yet. Please import data first.")

        brand_query = body[6:].strip()  # text after "REPORT"

        if not brand_query:
            # Portfolio summary
            kpis_all = ds.get_all_brand_kpis(report['id'])
            total_rev = sum(b['total_revenue'] for b in kpis_all)
            brand_count = len(kpis_all)
            top = max(kpis_all, key=lambda x: x['total_revenue'], default={})
            alert_count = ds.get_unacknowledged_count()

            lines = [
                f"DALA Portfolio — {report['month_label']}",
                f"Revenue: N{total_rev:,.0f}",
                f"Brands: {brand_count}",
                f"Top Performer: {top.get('brand_name', '-')} (N{top.get('total_revenue', 0):,.0f})",
                f"Alerts: {alert_count} unread",
                f"Type REPORT [brand name] for details.",
            ]
            return twiml_reply('\n'.join(lines))

        # Find brand (fuzzy match)
        all_brands = ds.get_all_brands_in_db()
        matched = fuzzy_brand(brand_query, all_brands)
        if not matched:
            return twiml_reply(
                f"Brand '{brand_query}' not found.\n"
                f"Try: BRANDS to see all brands."
            )

        kpis = ds.get_brand_kpis_single(report['id'], matched)
        if not kpis:
            return twiml_reply(f"{matched} has no data in the latest report.")

        rev    = kpis.get('total_revenue', 0)
        stores = kpis.get('num_stores', 0)
        repeat = kpis.get('repeat_pct', 0)
        stock  = kpis.get('stock_days_cover', 0)
        avg_store = kpis.get('avg_revenue_per_store', 0)

        lines = [
            f"{matched} — {report['month_label']}",
            f"Revenue: N{rev:,.0f}",
            f"Stores: {stores} | Repeat: {repeat:.1f}%",
            f"Avg Rev / Supermarket: N{avg_store:,.0f}",
            f"Stock Days: {stock:.0f}",
        ]

        # Attach AI narrative if available
        if gemini_available():
            cached_narrative = ds.get_narrative(report['id'], matched)
            if cached_narrative:
                # Trim to WhatsApp limit
                lines.append('')
                lines.append(cached_narrative[:300] + ('...' if len(cached_narrative) > 300 else ''))

        return twiml_reply('\n'.join(lines))

    # ── Unknown command ────────────────────────────────────────────────────────
    return twiml_reply(
        "I didn't understand that. Send HELP for a list of commands."
    )


# ── Database Layer (replaces Drive Sync page) ──────────────────────────────────

@app.route('/drive-sync')
def drive_sync_dashboard():
    """Legacy redirect — Drive Sync is now the Database page."""
    return redirect(url_for('database_page'))


def _build_drive_data():
    """Helper: return drive sync data dict for the database page template."""
    from modules.drive_sync import drive_available, DRIVE_FOLDERS, DriveSyncOrchestrator
    if not drive_available():
        return {
            'sync_status': 'no_credentials',
            'sync_stats':  {'total_imports': 0, 'total_errors': 0, 'files_tracked': 0},
            'folders':     DRIVE_FOLDERS,
            'drive_files': [],
            'drive_error': 'No Google credentials found.',
        }
    try:
        orch          = DriveSyncOrchestrator()
        stats         = orch.get_sync_summary()
        all_files_raw = orch.list_all_files()
        folder_errors = {f['folder_id']: f['list_error']
                         for f in all_files_raw if f.get('list_error')}
        all_files     = [f for f in all_files_raw if not f.get('list_error')]
        folders_data  = []
        for folder in DRIVE_FOLDERS:
            folder_files = [f for f in all_files if f['folder_id'] == folder['id']]
            folders_data.append({
                'name':       folder['name'],
                'id':         folder['id'],
                'files':      folder_files,
                'list_error': folder_errors.get(folder['id']),
            })
        return {
            'sync_status': 'active',
            'sync_stats':  {
                'total_imports':  stats['total_imports'],
                'total_errors':   stats['total_errors'],
                'files_tracked':  stats['total_files_tracked'],
            },
            'folders':     folders_data,
            'drive_files': all_files,
            'drive_error': None,
        }
    except Exception as e:
        from modules.drive_sync import DRIVE_FOLDERS
        return {
            'sync_status': 'error',
            'sync_stats':  {'total_imports': 0, 'total_errors': 0, 'files_tracked': 0},
            'folders':     DRIVE_FOLDERS,
            'drive_files': [],
            'drive_error': str(e),
        }


def _get_drive_bootstrap_data():
    """
    Cheap Drive tab bootstrap for /database.
    Avoids live Drive API calls during initial page render.
    """
    try:
        from modules.drive_sync import drive_available, DRIVE_FOLDERS
        folders = [
            {
                'name': folder['name'],
                'id': folder['id'],
                'files': [],
                'list_error': None,
            }
            for folder in DRIVE_FOLDERS
        ]
        if not drive_available():
            return {
                'sync_status': 'no_credentials',
                'sync_stats': {'total_imports': 0, 'total_errors': 0, 'files_tracked': 0},
                'folders': folders,
                'drive_files': [],
                'drive_error': 'No Google credentials found.',
                'drive_loaded': False,
            }
        return {
            'sync_status': 'loading',
            'sync_stats': {'total_imports': 0, 'total_errors': 0, 'files_tracked': 0},
            'folders': folders,
            'drive_files': [],
            'drive_error': None,
            'drive_loaded': False,
        }
    except Exception as e:
        return {
            'sync_status': 'error',
            'sync_stats': {'total_imports': 0, 'total_errors': 0, 'files_tracked': 0},
            'folders': [],
            'drive_files': [],
            'drive_error': str(e),
            'drive_loaded': False,
        }


@app.route('/database')
def database_page():
    """Unified database management page — upload, Drive Sync, and Google Sheets tabs."""
    alert_count     = ds.get_unacknowledged_count()
    db_health       = ds.get_db_health_stats()
    all_reports     = ds.get_all_reports()
    recent_activity = ds.get_activity_log(limit=20)

    # Sheets auth method
    try:
        from modules.sheets import sheets_auth_method
        sheets_auth = sheets_auth_method()
    except Exception:
        sheets_auth = None

    drive_data = _get_drive_bootstrap_data()

    return render_template(
        'portal/database.html',
        alert_count=alert_count,
        db_health=db_health,
        all_reports=all_reports,
        recent_activity=recent_activity,
        sheets_auth=sheets_auth,
        **drive_data,
    )


@app.route('/api/drive-sync/summary')
def api_drive_sync_summary():
    """Load Drive summary lazily so /database doesn't block on Google API calls."""
    return jsonify(_build_drive_data())


# ── DB Import API (upload → pipeline → DB, no PDFs) ────────────────────────────

@app.route('/api/db_import', methods=['POST'])
def api_db_import():
    """
    Import one or more Excel/CSV files directly into the database without generating PDFs.
    Supports both combined (Brand Partner column) and per-brand Tally wide-format files.
    """
    uploaded = request.files.getlist('files[]')
    if not uploaded:
        return jsonify({'success': False, 'error': 'No files provided'}), 400

    start_date  = request.form.get('start_date', '').strip() or None
    end_date    = request.form.get('end_date',   '').strip() or None
    format_hint = request.form.get('format_hint', 'auto')
    merge_mode  = request.form.get('merge_mode', 'replace')

    # Buffer all files in memory before the thread starts
    file_buffers = []
    for f in uploaded:
        buf = io.BytesIO(f.read())
        buf.name = f.filename
        file_buffers.append((f.filename, buf))

    job_id = uuid.uuid4().hex
    ds.create_job(job_id)

    def _run():
        try:
            from modules.ingestion import load_and_clean, load_brand_file, filter_by_date, split_by_brand
            from modules.kpi import calculate_kpis, calculate_perf_score
            from modules.alerts import check_and_save_alerts, run_portfolio_alerts
            from modules.drive_sync import (
                _run_pipeline_from_df, _extract_brand_from_filename, DateExtractor
            )
            import calendar as _cal

            ds.update_job(job_id, status='running', total=len(file_buffers),
                          current_brand='Loading files...')

            combined_dfs, brand_dfs, errors = [], [], []

            for fname, buf in file_buffers:
                buf.seek(0)
                try:
                    if format_hint in ('combined', 'auto'):
                        try:
                            df = load_and_clean(buf)
                            combined_dfs.append(df)
                            continue
                        except Exception:
                            if format_hint == 'combined':
                                raise
                    buf.seek(0)
                    brand_name = _extract_brand_from_filename(fname) or fname
                    df = load_brand_file(buf, brand_name)
                    if not df.empty:
                        brand_dfs.append(df)
                except Exception as e:
                    errors.append(f'{fname}: {e}')

            dfs = combined_dfs if combined_dfs else brand_dfs
            if not dfs:
                ds.update_job(job_id, status='error',
                              error_msg=f'No data loaded. Errors: {errors[:3]}')
                return

            import pandas as _pd
            combined = _pd.concat(dfs, ignore_index=True)

            # Determine date range
            if start_date and end_date:
                s, e = start_date, end_date
            else:
                s, e = DateExtractor.from_excel_content(combined)

            if not s or not e:
                ds.update_job(job_id, status='error',
                              error_msg='Could not determine date range. Please provide start/end dates.')
                return

            ds.update_job(job_id, current_brand=f'Importing {s} -> {e}...')

            if merge_mode == 'additive':
                # Per-brand additive merge
                from modules.ingestion import filter_by_date as _fbd, split_by_brand as _sbb
                df_filtered  = _fbd(combined, s, e)
                brand_data   = _sbb(df_filtered)
                brands       = list(brand_data.keys())
                all_kpis     = {b: calculate_kpis(brand_data[b]) for b in brands}
                total_rev    = sum(k['total_revenue'] for k in all_kpis.values())
                avg_rev      = total_rev / max(len(brands), 1)
                for b in brands:
                    all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], avg_rev)
                existing = ds.get_report_by_date_range(s, e)
                if existing:
                    report_id = existing['id']
                    for b in brands:
                        ds.clear_brand_from_report(report_id, b)
                else:
                    total_qty = sum(k['total_qty'] for k in all_kpis.values())
                    all_stores: set = set()
                    for k in all_kpis.values():
                        if k.get('top_stores') is not None and not k['top_stores'].empty:
                            all_stores.update(k['top_stores']['Store'].tolist())
                    report_id = ds.save_report(
                        start_date=s, end_date=e, xls_filename='db_import_additive',
                        total_revenue=total_rev, total_qty=total_qty,
                        total_stores=len(all_stores), brand_count=len(brands),
                    )
                _queue_catalog_candidates(df_filtered, source_filename='db_import_additive', report_id=report_id)
                for b in brands:
                    k = all_kpis[b]
                    share = round(k['total_revenue'] / max(total_rev, 1) * 100, 2)
                    ds.save_brand_kpis(report_id, b, k, k.get('perf_score', {}), share)
                    ds.save_brand_detail_json(report_id, b, k)
                    if not k['daily_sales'].empty:
                        ds.save_daily_sales(report_id, b, k['daily_sales'])
                    history = ds.get_brand_history(b, limit=3)
                    check_and_save_alerts(report_id, b, k, avg_rev, history[1:], ds)
                run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
                _compute_and_save_churn(report_id)
            else:
                result = _run_pipeline_from_df(combined, 'db_import_upload', s, e, ds)
                report_id = result.get('report_id')

            ds.log_activity('db_import_upload',
                            detail=f'{len(file_buffers)} file(s), {s} -> {e}, mode={merge_mode}')
            _refresh_copilot_state(
                report_id=report_id,
                reason=f'db_import_upload:{merge_mode}',
                source='db_import',
            )
            ds.update_job(job_id, status='done', progress=100,
                          current_brand=f'Complete — {s} to {e}',
                          report_id=report_id)
        except Exception as ex:
            ds.update_job(job_id, status='error', error_msg=str(ex))

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/db_import/job/<job_id>')
def api_db_import_job(job_id):
    """Poll status of a db_import job."""
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


@app.route('/api/database/report/<int:report_id>/delete', methods=['POST'])
def api_database_delete_report(report_id):
    """Admin: delete a full stored period and all derived rows."""
    report = ds.get_report(report_id)
    if not report:
        return jsonify({'success': False, 'error': 'Report not found'}), 404

    if not ds.delete_report(report_id):
        return jsonify({'success': False, 'error': 'Delete failed'}), 500

    ds.log_activity(
        'db_report_delete',
        detail=f"Deleted {report['month_label']} ({report['start_date']} to {report['end_date']})",
        report_id=None,
    )
    _refresh_copilot_state(reason=f'report_delete:{report_id}', source='database_update')
    return jsonify({'success': True})


@app.route('/api/database/report/<int:report_id>/remove_brand', methods=['POST'])
def api_database_remove_brand(report_id):
    """Admin: remove one brand from a stored period, then refresh rollups."""
    report = ds.get_report(report_id)
    if not report:
        return jsonify({'success': False, 'error': 'Report not found'}), 404

    payload = request.get_json(silent=True) or {}
    brand_name = str(payload.get('brand_name', '')).strip()
    if not brand_name:
        return jsonify({'success': False, 'error': 'Brand name is required'}), 400

    if not ds.get_brand_kpis_single(report_id, brand_name):
        return jsonify({'success': False, 'error': f'Brand not found in {report["month_label"]}'}), 404

    for member in ds._get_brand_family_names(brand_name):
        ds.clear_brand_from_report(report_id, member)
    ds.refresh_report_totals(report_id)
    ds.log_activity(
        'db_brand_remove',
        detail=f"Removed {ds.analytics_brand_name(brand_name)} from {report['month_label']}",
        brand_name=ds.analytics_brand_name(brand_name),
        report_id=report_id,
    )
    _refresh_copilot_state(
        report_id=report_id,
        brand_name=brand_name,
        reason=f'brand_remove:{report_id}',
        source='database_update',
    )
    return jsonify({'success': True})


@app.route('/api/db_import_sheet/preview', methods=['POST'])
def api_db_import_sheet_preview():
    """Preview a Google Sheet before importing — returns columns, row count, brands."""
    data = request.get_json(silent=True) or {}
    sheet_id = data.get('sheet_id', '').strip()
    tab_name = data.get('tab_name', '').strip() or None
    if not sheet_id:
        return jsonify({'success': False, 'error': 'sheet_id required'}), 400
    try:
        from modules.sheets import pull_sheet_as_df
        df = pull_sheet_as_df(sheet_id, tab_name)
        brands = sorted(df['Brand Partner'].dropna().unique().tolist()) if 'Brand Partner' in df.columns else []
        date_min = str(df['Date'].min().date()) if 'Date' in df.columns else ''
        date_max = str(df['Date'].max().date()) if 'Date' in df.columns else ''
        return jsonify({
            'success':    True,
            'columns':    df.columns.tolist(),
            'row_count':  len(df),
            'brands':     brands,
            'date_range': f'{date_min} to {date_max}' if date_min else 'unknown',
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/db_import_sheet', methods=['POST'])
def api_db_import_sheet():
    """Import a Google Sheet into the database."""
    data       = request.get_json(silent=True) or {}
    sheet_id   = data.get('sheet_id', '').strip()
    start_date = data.get('start_date', '').strip() or None
    end_date   = data.get('end_date',   '').strip() or None
    tab_name   = data.get('tab_name',   '').strip() or None
    merge_mode = data.get('merge_mode', 'replace')
    if not sheet_id:
        return jsonify({'success': False, 'error': 'sheet_id required'}), 400

    job_id = uuid.uuid4().hex
    ds.create_job(job_id)

    def _run():
        try:
            from modules.sheets import pull_sheet_as_df
            from modules.drive_sync import _run_pipeline_from_df, DateExtractor
            ds.update_job(job_id, status='running', current_brand='Fetching Google Sheet...')
            df = pull_sheet_as_df(sheet_id, tab_name)
            s, e = start_date, end_date
            if not s or not e:
                s, e = DateExtractor.from_excel_content(df)
            if not s or not e:
                ds.update_job(job_id, status='error',
                              error_msg='Could not determine date range.')
                return
            result = _run_pipeline_from_df(df, f'sheets:{sheet_id[:20]}', s, e, ds)
            ds.log_activity('db_import_sheet', detail=f'{sheet_id[:30]}, {s} -> {e}')
            _refresh_copilot_state(
                report_id=result.get('report_id'),
                reason='db_import_sheet',
                source='db_import',
            )
            ds.update_job(job_id, status='done', progress=100,
                          current_brand=f'Complete — {s} to {e}',
                          report_id=result.get('report_id'))
        except Exception as ex:
            ds.update_job(job_id, status='error', error_msg=str(ex))

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/drive-sync/trigger', methods=['POST'])
def api_drive_sync_trigger():
    """Check for new/changed files and import them."""
    try:
        from modules.drive_sync import DriveSyncOrchestrator
        orch    = DriveSyncOrchestrator()
        results = orch.check_new_files()
        if any(r.get('status') == 'success' for r in results):
            _refresh_copilot_state(reason='drive_sync_trigger', source='drive_sync')
        return jsonify({
            'success':  True,
            'imported': sum(1 for r in results if r.get('status') == 'success'),
            'skipped':  sum(1 for r in results if r.get('status') == 'skipped'),
            'errors':   sum(1 for r in results if r.get('status') == 'error'),
            'results':  results,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/drive-sync/full-import', methods=['POST'])
def api_drive_sync_full_import():
    """
    Background full historical import — imports ALL files from both Drive folders.
    Returns a job_id; poll /api/drive-sync/job/<job_id> for progress.
    """
    import uuid, threading
    job_id = uuid.uuid4().hex
    ds.create_job(job_id)

    def _run():
        try:
            from modules.drive_sync import DriveSyncOrchestrator
            orch = DriveSyncOrchestrator()

            # Discover month groups first so we know the total
            groups = orch.get_month_groups()
            total  = len(groups)
            ds.update_job(job_id, total=total, status='running',
                          current_brand='Connecting to Google Drive...')

            imported, errors = 0, 0

            def _progress(current, total_files, file_name):
                nonlocal imported, errors
                pct = int(current / max(total_files, 1) * 100)
                ds.update_job(job_id, progress=pct, current_brand=file_name)

            results = orch.full_historical_sync(progress_cb=_progress, groups=groups)
            imported = sum(1 for r in results if r.get('status') == 'success')
            errors   = sum(1 for r in results if r.get('status') == 'error')
            if imported:
                _refresh_copilot_state(reason='drive_sync_full_import', source='drive_sync')

            ds.update_job(job_id, status='done', progress=100,
                          current_brand=f'Complete: {imported} imported, {errors} errors')
        except Exception as e:
            ds.update_job(job_id, status='error', error_msg=str(e))

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/drive-sync/job/<job_id>')
def api_drive_sync_job(job_id):
    """Poll status of a full-import job."""
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)


@app.route('/api/drive-sync/toggle', methods=['POST'])
def api_drive_sync_toggle():
    """Toggle automatic sync (placeholder — sync currently manual)."""
    return jsonify({'success': True, 'auto_sync': True})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 60)
    print(f"  DALA Analytics Portal — http://127.0.0.1:{port}")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=port, use_reloader=False)
