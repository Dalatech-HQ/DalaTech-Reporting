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

import os, io, json, traceback, shutil, uuid, threading, tempfile
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

from modules.ingestion        import load_and_clean, filter_by_date, split_by_brand
from modules.kpi              import calculate_kpis, calculate_perf_score, generate_narrative
from modules.pdf_generator_html import generate_pdf_html
from modules.pdf_generator      import generate_pdf as generate_pdf_reportlab
from modules.pdf_generator_html import render_pdf_report_html, render_pdf_bytes
from modules.html_generator   import generate_html, render_html_report
from modules.portfolio_generator import generate_portfolio_html
from modules.data_store       import DataStore
from modules.alerts           import check_and_save_alerts, run_portfolio_alerts
from modules.predictor        import build_brand_forecasts, stock_depletion_date, growth_label, growth_color
from modules.historical       import (
    get_brand_monthly_history, get_portfolio_monthly_trend,
    get_repeat_purchase_map_data, generate_insights,
    get_color_scheme_for_month, get_monthly_metrics
)
from modules.brand_names      import canonicalize_brand_name
from modules.geocoding        import is_geocoding_available
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


def _safe_name(brand_name):
    return brand_name.replace(' ', '_').replace("'", '').replace('/', '-')


def _merge_depletions_by_brand(brand_kpis_rows):
    """Collapse obvious duplicate brand variants into one depletion summary."""
    merged = {}
    for row in brand_kpis_rows or []:
        brand_name = canonicalize_brand_name(row.get('brand_name', ''))
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

def _run_generation(job_id, file_bytes, start_date, end_date, selected_brands, filename, report_type=None):
    """Background thread: runs full generation and persists state to SQLite."""
    def _upd(**kw):
        ds.update_job(job_id, **kw)

    try:
        df_all    = load_and_clean(io.BytesIO(file_bytes))
        df_ranged = filter_by_date(df_all, start_date, end_date)
        brand_data = split_by_brand(df_ranged)

        # Filter to selected brands
        if selected_brands:
            brand_data = {b: df for b, df in brand_data.items() if b in selected_brands}

        brands = list(brand_data.keys())
        _upd(total=len(brands) + 1)  # +1 for portfolio

        # Compute all KPIs
        all_kpis = {}
        for b in brands:
            all_kpis[b] = calculate_kpis(brand_data[b])

        total_portfolio_revenue = sum(k['total_revenue'] for k in all_kpis.values())
        portfolio_avg_revenue   = total_portfolio_revenue / max(len(brands), 1)

        for b in brands:
            all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], portfolio_avg_revenue)

        # Save report to DB — upsert: reuse existing row for same date range
        all_stores = set()
        for k in all_kpis.values():
            if k.get('top_stores') is not None and not k['top_stores'].empty:
                all_stores.update(k['top_stores']['Store'].tolist())

        total_qty_sum = sum(k['total_qty'] for k in all_kpis.values())
        existing_report = ds.get_report_by_date_range(start_date, end_date)
        if existing_report:
            report_id = existing_report['id']
            ds.clear_report_data(report_id)  # wipe old alerts/kpis to avoid duplicates
            ds.update_report(report_id, xls_filename=filename,
                             total_revenue=total_portfolio_revenue,
                             total_qty=total_qty_sum,
                             total_stores=len(all_stores),
                             brand_count=len(brands))
        else:
            report_id = ds.save_report(
                start_date=start_date, end_date=end_date,
                xls_filename=filename,
                total_revenue=total_portfolio_revenue,
                total_qty=total_qty_sum,
                total_stores=len(all_stores),
                brand_count=len(brands),
                report_type=report_type,
            )
        _upd(report_id=report_id)

        # Save KPIs + alerts
        for b in brands:
            k = all_kpis[b]
            ps = k.get('perf_score', {})
            share = round(k['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)
            ds.save_brand_kpis(report_id, b, k, ps, share)
            ds.save_brand_detail_json(report_id, b, k)
            if not k['daily_sales'].empty:
                ds.save_daily_sales(report_id, b, k['daily_sales'])
            history = ds.get_brand_history(b, limit=3)
            check_and_save_alerts(report_id, b, k, portfolio_avg_revenue, history[1:], ds)
        run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)

        # ── Pre-generate AI narratives (batch, before PDFs so they embed in them) ─
        ai_narratives = {}
        if gemini_available():
            _upd(current_brand='Generating AI Narratives...')
            for b in brands:
                try:
                    history = ds.get_brand_history(b, limit=6)
                    text, _ = generate_brand_narrative(b, all_kpis[b], history, portfolio_avg_revenue)
                    if text:
                        ai_narratives[b] = text
                        ds.save_narrative(report_id, b, text)
                except Exception:
                    pass
            # Portfolio narrative
            try:
                report_meta = ds.get_report(report_id)
                pt = generate_portfolio_narrative(all_kpis, report_meta)
                if pt:
                    ds.save_narrative(report_id, '__portfolio__', pt)
            except Exception:
                pass

        # ── Pre-push to Google Sheets (auto, before PDFs so URL embeds in them) ──
        sheets_urls = {}
        try:
            from modules.sheets import push_brand_to_sheets, sheets_available
            if sheets_available():
                _upd(current_brand='Syncing to Google Sheets...')
                for b in brands:
                    try:
                        url = push_brand_to_sheets(
                            brand_name=b,
                            brand_df=brand_data[b],
                            start_date=start_date,
                            end_date=end_date,
                        )
                        if url:
                            sheets_urls[b] = url
                            ds.log_activity('sheets_sync', 'Auto-synced to Google Sheets', b, report_id)
                    except Exception:
                        pass
        except Exception:
            pass

        # Generate per-brand files with retry logic
        start_dt  = datetime.strptime(start_date, '%Y-%m-%d')
        month_tag = start_dt.strftime('%b%Y')

        def try_generate_pdf(brand_name, pdf_path, kpis, max_retries=2):
            """Attempt PDF generation with retries. Returns (success, is_pdf, error)."""
            for attempt in range(max_retries):
                try:
                    result_path = generate_pdf_html(
                        output_path=pdf_path, brand_name=brand_name, kpis=kpis,
                        start_date=start_date, end_date=end_date,
                        portfolio_avg_revenue=portfolio_avg_revenue,
                        total_portfolio_revenue=total_portfolio_revenue,
                        ai_narrative=ai_narratives.get(brand_name),
                        sheets_url=sheets_urls.get(brand_name),
                    )
                    # Check if PDF was actually created or if HTML fallback was used
                    is_pdf = result_path.endswith('.pdf') and os.path.exists(result_path)
                    is_html = result_path.endswith('.html') and os.path.exists(result_path)
                    if is_pdf:
                        return True, True, None  # success, is_pdf, error
                    elif is_html:
                        return True, False, None  # success (HTML fallback), is_pdf=False, error
                    else:
                        return False, False, "File not created"
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        return False, False, str(e)
            return False, False, "Max retries exceeded"

        def try_generate_html(brand_name, html_path, kpis, max_retries=2):
            """Attempt HTML generation with retries."""
            for attempt in range(max_retries):
                try:
                    generate_html(output_path=html_path, brand_name=brand_name, kpis=kpis,
                                  start_date=start_date, end_date=end_date,
                                  portfolio_avg_revenue=portfolio_avg_revenue,
                                  total_portfolio_revenue=total_portfolio_revenue)
                    return True, None
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(0.5 * (attempt + 1))
                    else:
                        return False, str(e)
            return False, "Max retries exceeded"

        brands_done = []
        for i, brand_name in enumerate(brands):
            _upd(current_brand=brand_name)
            safe      = _safe_name(brand_name)
            pdf_path  = os.path.join(PDF_DIR,  f"{safe}_Report_{month_tag}.pdf")
            html_path = os.path.join(HTML_DIR, f"{safe}_Report_{month_tag}.html")
            kpis      = all_kpis[brand_name]

            brand_result = {'brand': brand_name, 'pdf': False, 'html': False, 'error': None}

            # Generate PDF with retry (may return HTML as fallback)
            pdf_success, is_actually_pdf, pdf_error = try_generate_pdf(brand_name, pdf_path, kpis)
            brand_result['pdf'] = pdf_success and is_actually_pdf
            brand_result['html'] = pdf_success  # If PDF gen succeeded (even HTML fallback), we have an HTML
            if pdf_error:
                brand_result['error'] = f'PDF: {pdf_error}'

            # Only generate separate HTML if PDF generation completely failed
            if not pdf_success:
                html_success, html_error = try_generate_html(brand_name, html_path, kpis)
                brand_result['html'] = html_success
                if html_error:
                    brand_result['error'] = (brand_result['error'] or '') + f' HTML: {html_error}'

            # Clear error if at least one output format succeeded
            if (brand_result['pdf'] or brand_result['html']) and brand_result['error']:
                brand_result['error'] = None  # Don't show error if we have at least one output

            brands_done.append(brand_result)
            _upd(brands_done=brands_done, progress=round((i + 1) / (len(brands) + 1) * 100))

        # Portfolio dashboard
        _upd(current_brand='Portfolio Dashboard')
        portfolio_path = os.path.join(HTML_DIR, f"PORTFOLIO_Dashboard_{month_tag}.html")
        portfolio_filename = None
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
        except Exception as e:
            job_obj = ds.get_job(job_id) or {}
            errs = job_obj.get('errors', [])
            errs.append(f'Portfolio: {e}')
            _upd(errors=errs)

        for b in brands:
            ds.get_or_create_token(b)

        _upd(progress=100, status='done', current_brand=None,
             portfolio_file=portfolio_filename)

    except Exception as exc:
        _upd(status='error', error_msg=str(exc))


@app.route('/api/generate_async', methods=['POST'])
def generate_async():
    """Start background generation. Returns {job_id} immediately."""
    file           = request.files.get('tally_file')
    start_date     = request.form.get('start_date', '').strip()
    end_date       = request.form.get('end_date', '').strip()
    selected_raw   = request.form.get('selected_brands', '')
    selected_brands = [b.strip() for b in selected_raw.split(',') if b.strip()] if selected_raw else []
    report_type    = request.form.get('report_type', '').strip() or None

    if not file or not start_date or not end_date:
        return jsonify({'success': False, 'error': 'Missing file or dates'}), 400

    file_bytes = file.read()
    job_id     = uuid.uuid4().hex
    ds.create_job(job_id)   # persisted to SQLite — survives worker restarts

    t = threading.Thread(
        target=_run_generation,
        args=(job_id, file_bytes, start_date, end_date, selected_brands, file.filename, report_type),
        daemon=True,
    )
    t.start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/generation_status/<job_id>')
def generation_status(job_id):
    job = ds.get_job(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    return jsonify({'success': True, **job})


# ── Trends / Forecasting Dashboard ────────────────────────────────────────────

@app.route('/trends')
def trends():
    """
    Historical trend analysis dashboard with MoM growth,
    color themes, and insights.
    """
    def render_trends_error(message, available_months=None, year=None, month=None):
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
            map_data_json='[]',
            color_scheme=get_color_scheme_for_month(selected_year, selected_month),
            available_months=available_months or [],
            current_year=selected_year,
            current_month=selected_month,
            google_maps_key=os.environ.get('GOOGLE_MAPS_API_KEY', ''),
            alert_count=ds.get_unacknowledged_count(),
        )
    
    # Load historical data
    hist_path = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')
    if not os.path.exists(hist_path):
        return render_trends_error("Historical data not available")
    
    try:
        df = pd.read_excel(hist_path)
        df['Date'] = pd.to_datetime(df['Date'])
    except Exception as e:
        return render_trends_error(f"Error loading data: {e}")
    
    # Get available months
    df['YearMonth'] = df['Date'].dt.to_period('M')
    available_ym = sorted(df['YearMonth'].unique())
    available_months = [{'year': ym.year, 'month': ym.month, 
                        'label': ym.strftime('%b %Y')} for ym in available_ym]
    
    # Get selected month (default to latest)
    month_param = request.args.get('month', '')
    if month_param:
        year, month = map(int, month_param.split('-'))
    else:
        year, month = available_ym[-1].year, available_ym[-1].month
    
    # Calculate metrics
    metrics = get_monthly_metrics(df, year, month)
    if not metrics:
        return render_trends_error(
            "No data for selected month",
            available_months=available_months,
            year=year,
            month=month,
        )
    
    # Get historical data for sparklines
    historical = get_portfolio_monthly_trend(df)
    
    # Get insights
    insights = generate_insights(historical)
    
    # Get color scheme
    color_scheme = get_color_scheme_for_month(year, month)
    
    # Get top stores for this month
    sales_df = df[(df['Date'].dt.year == year) & (df['Date'].dt.month == month) & 
                  (df['Vch Type'] == 'Sales')]
    
    top_stores = []
    if not sales_df.empty:
        store_revenue = sales_df.groupby('Particulars')['Sales_Value'].sum().sort_values(ascending=False).head(10)
        top_stores = [{'name': name, 'revenue': rev} for name, rev in store_revenue.items()]
    
    # Get top products
    top_products = []
    if not sales_df.empty:
        product_revenue = sales_df.groupby('SKUs')['Sales_Value'].sum().sort_values(ascending=False).head(10)
        top_products = [{'name': name, 'revenue': rev} for name, rev in product_revenue.items()]
    
    # Get map data with geocoding
    map_data = get_repeat_purchase_map_data(df, year, month, top_n=20)
    
    # Try to geocode store locations if API key is available
    google_maps_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
    if google_maps_key and map_data:
        from modules.geocoding import geocode_stores_batch
        try:
            coords = geocode_stores_batch([m['store_name'] for m in map_data], google_maps_key)
            for m in map_data:
                if m['store_name'] in coords:
                    m['latitude'] = coords[m['store_name']][0]
                    m['longitude'] = coords[m['store_name']][1]
        except Exception as e:
            print(f"Geocoding error: {e}")
    
    # Filter to only stores with coordinates for the map
    map_data_with_coords = [m for m in map_data if m.get('latitude') and m.get('longitude')]
    
    # Helper to convert numpy types to native Python types for JSON
    def convert_to_native(obj):
        if isinstance(obj, dict):
            return {k: convert_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_native(i) for i in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        return obj
    
    # Convert data for JSON serialization
    historical_native = convert_to_native(historical)
    map_data_native = convert_to_native(map_data_with_coords)
    
    return render_template('portal/trends.html',
                           metrics=metrics,
                           insights=insights,
                           historical=historical,
                           historical_json=json.dumps(historical_native),
                           top_stores=top_stores,
                           top_stores_json=json.dumps(top_stores),
                           top_products=top_products,
                           top_products_json=json.dumps(top_products),
                           map_data=map_data,
                           map_data_json=json.dumps(map_data_native),
                           color_scheme=color_scheme,
                           available_months=available_months,
                           current_year=year,
                           current_month=month,
                           google_maps_key=google_maps_key,
                           alert_count=ds.get_unacknowledged_count())


# ── How It Works (Public Documentation) ───────────────────────────────────────

@app.route('/how-it-works')
def how_it_works():
    """Public documentation page explaining the system."""
    return render_template('portal/docs.html', 
                           alert_count=ds.get_unacknowledged_count())


# ── Admin: Retailer Count ─────────────────────────────────────────────────────

@app.route('/admin/retailers')
def admin_retailers():
    """Admin-only view of retailer counts per brand."""
    # In production, add authentication check here
    hist_path = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')
    if not os.path.exists(hist_path):
        return render_template('portal/admin_retailers.html',
                               retailers=[],
                               alert_count=ds.get_unacknowledged_count())
    
    df = pd.read_excel(hist_path)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Get latest month's retailer counts per brand
    latest_date = df['Date'].max()
    latest_month = df[df['Date'].dt.to_period('M') == latest_date.to_period('M')]
    sales_data = latest_month[latest_month['Vch Type'] == 'Sales']
    
    retailer_stats = []
    for brand in sorted(sales_data['Brand Partner'].unique()):
        brand_data = sales_data[sales_data['Brand Partner'] == brand]
        stats = {
            'brand': brand,
            'store_count': brand_data['Particulars'].nunique(),
            'sku_count': brand_data['SKUs'].nunique(),
            'total_revenue': brand_data['Sales_Value'].sum(),
            'last_order': brand_data['Date'].max().strftime('%Y-%m-%d'),
            'status': 'Active' if len(brand_data) > 0 else 'Inactive'
        }
        retailer_stats.append(stats)
    
    return render_template('portal/admin_retailers.html',
                           retailers=retailer_stats,
                           alert_count=ds.get_unacknowledged_count())


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

    brand_data = split_by_brand(df_ranged)
    if not brand_data:
        return jsonify({'success': False, 'error': 'No sales data found.'}), 422

    brands = list(brand_data.keys())

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
    if existing_report:
        report_id = existing_report['id']
        ds.clear_report_data(report_id)
        ds.update_report(report_id, xls_filename=file.filename,
                         total_revenue=total_portfolio_revenue,
                         total_qty=total_qty_sum,
                         total_stores=len(all_stores),
                         brand_count=len(brands))
    else:
        report_id = ds.save_report(
            start_date=start_date,
            end_date=end_date,
            xls_filename=file.filename,
            total_revenue=total_portfolio_revenue,
            total_qty=total_qty_sum,
            total_stores=len(all_stores),
            brand_count=len(brands),
        )

    # Generate files
    ok_pdf = ok_html = 0
    errors = []

    for brand_name in brands:
        kpis = all_kpis[brand_name]
        safe = _safe_name(brand_name)
        pdf_path  = os.path.join(PDF_DIR,  f"{safe}_Report_Feb2026.pdf")
        html_path = os.path.join(HTML_DIR, f"{safe}_Report_Feb2026.html")

        perf = calculate_perf_score(kpis, portfolio_avg_revenue)
        kpis['perf_score'] = perf
        portfolio_share = round(kpis['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)

        # Save to DB
        ds.save_brand_kpis(report_id, brand_name, kpis, perf, portfolio_share)
        ds.save_brand_detail_json(report_id, brand_name, kpis)
        if not kpis['daily_sales'].empty:
            ds.save_daily_sales(report_id, brand_name, kpis['daily_sales'])

        # Alerts
        history = ds.get_brand_history(brand_name, limit=3)
        check_and_save_alerts(report_id, brand_name, kpis,
                              portfolio_avg_revenue, history[1:], ds)

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
            )
            ok_html += 1
        except Exception as e:
            errors.append({'brand': brand_name, 'type': 'HTML', 'error': str(e)})

    # Portfolio dashboard
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    month_tag = start_dt.strftime('%b%Y')
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

    return jsonify({
        'success':    True,
        'report_id':  report_id,
        'pdf_count':  ok_pdf,
        'html_count': ok_html,
        'brands':     len(brands),
        'errors':     errors,
        'portfolio_dashboard': f"/download/html/{os.path.basename(portfolio_path)}",
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

    return render_template('portal/dashboard.html',
                           report=report,
                           brand_kpis=brand_kpis,
                           reports=ds.get_all_reports(),
                           alerts=alerts[:5],
                           alert_count=alert_count,
                           portfolio_file=portfolio_file if portfolio_exists else None)


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
def brand_detail(brand_name):
    latest = ds.get_latest_report()
    report_id = request.args.get('report_id', latest['id'] if latest else None, type=int)
    alert_count = ds.get_unacknowledged_count()

    # Latest KPIs for this brand
    kpis = ds.get_brand_kpis_single(report_id, brand_name) if report_id else None

    # Historical trend
    history = ds.get_brand_history(brand_name, limit=12)
    hist_oldest = list(reversed(history))

    # Forecast
    canonical_brand = canonicalize_brand_name(brand_name)
    forecast = build_brand_forecasts({brand_name: hist_oldest}).get(canonical_brand, {})

    # Daily sales
    daily = ds.get_daily_sales(report_id, brand_name) if report_id else []

    # Depletion
    depletion = stock_depletion_date(
        {'total_closing_stock': kpis.get('closing_stock_total', 0) if kpis else 0,
         'stock_days_cover':    kpis.get('stock_days_cover', 0) if kpis else 0},
    ) if kpis else {}

    # Token
    token = ds.get_or_create_token(brand_name)

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
                           report=ds.get_report(report_id) if report_id else None,
                           alert_count=alert_count)


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
    tokens = ds.get_all_tokens()
    alert_count = ds.get_unacknowledged_count()
    smtp_ok  = bool(os.environ.get('SMTP_USER') and os.environ.get('SMTP_PASSWORD'))
    twilio_ok = bool(os.environ.get('TWILIO_ACCOUNT_SID') and os.environ.get('TWILIO_AUTH_TOKEN'))
    return render_template('portal/settings.html',
                           tokens=tokens,
                           alert_count=alert_count,
                           smtp_configured=smtp_ok,
                           twilio_configured=twilio_ok,
                           reports=ds.get_all_reports())


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
    hist_oldest = list(reversed(history))
    forecast = build_brand_forecasts({brand_name: hist_oldest}).get(canonicalize_brand_name(brand_name), {})
    daily   = ds.get_daily_sales(report_id, brand_name) if report_id else []

    # PDF link
    safe = _safe_name(brand_name)
    pdf_files = [f for f in os.listdir(PDF_DIR) if f.startswith(safe) and f.endswith('.pdf')] \
                if os.path.isdir(PDF_DIR) else []
    pdf_file = pdf_files[0] if pdf_files else None

    return render_template('portal/brand_portal.html',
                           brand_name=brand_name,
                           brand_info=brand_info,
                           kpis=kpis,
                           history=history,
                           forecast=forecast,
                           daily=daily,
                           pdf_file=pdf_file,
                           report=ds.get_report(report_id) if report_id else None)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/acknowledge', methods=['POST'])
def acknowledge():
    alert_id = request.json.get('alert_id')
    if not alert_id:
        return jsonify({'success': False}), 400
    ds.acknowledge_alert(alert_id)
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
    return jsonify({'success': True})


@app.route('/api/regenerate_token', methods=['POST'])
def regenerate_token():
    brand_name = (request.json or {}).get('brand_name', '')
    if not brand_name:
        return jsonify({'success': False}), 400
    token = ds.regenerate_token(brand_name)
    portal_url = url_for('brand_portal', token=token, _external=True)
    return jsonify({'success': True, 'token': token, 'portal_url': portal_url})


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
                    return pd.DataFrame(records)
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

    # Derive top SKU from product_qty
    top_sku     = product_qty_df.iloc[0]['SKU']     if not product_qty_df.empty else '—'
    top_sku_qty = product_qty_df.iloc[0]['Quantity'] if not product_qty_df.empty else 0
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

    return {
        # Scalars
        'total_revenue':         bk.get('total_revenue', 0),
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
        'top_store_name':        bk.get('top_store_name') or '—',
        'top_store_revenue':     bk.get('top_store_revenue', 0),
        'top_store_pct':         top_store_pct,
        'wow_rev_change':        bk.get('wow_rev_change', 0),
        'wow_qty_change':        bk.get('wow_qty_change', 0),
        'top_sku':               top_sku,
        'top_sku_qty':           top_sku_qty,
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


@app.route('/api/report_pdf/<int:report_id>/<path:brand_name>')
def api_report_pdf(report_id, brand_name):
    """Generate and serve a PDF report for a single brand from stored DB data."""
    report = ds.get_report(report_id)
    if not report:
        abort(404)
    kpis = _reconstruct_kpis_from_db(report_id, brand_name)
    if not kpis:
        abort(404)

    all_bk          = ds.get_all_brand_kpis(report_id)
    total_portfolio = sum(b['total_revenue'] for b in all_bk)
    avg_portfolio   = total_portfolio / max(len(all_bk), 1)

    safe = _safe_name(brand_name)
    month_tag = datetime.strptime(report['start_date'], '%Y-%m-%d').strftime('%b%Y')
    fname = f"{safe}_Report_{month_tag}.pdf"

    try:
        html_content = render_pdf_report_html(
            brand_name=brand_name,
            kpis=kpis,
            start_date=report['start_date'],
            end_date=report['end_date'],
            portfolio_avg_revenue=avg_portfolio,
            total_portfolio_revenue=total_portfolio,
        )
        pdf_bytes = render_pdf_bytes(html_content)
    except Exception as html_pdf_error:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                temp_path = temp_file.name
            generate_pdf_reportlab(
                output_path=temp_path,
                brand_name=brand_name,
                kpis=kpis,
                start_date=report['start_date'],
                end_date=report['end_date'],
            )
            with open(temp_path, 'rb') as fh:
                pdf_bytes = fh.read()
        except Exception as fallback_error:
            return jsonify({
                'error': str(html_pdf_error),
                'fallback_error': str(fallback_error),
            }), 500
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=fname,
        mimetype='application/pdf',
    )


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

    try:
        html_content = render_html_report(
            brand_name=brand_name,
            kpis=kpis,
            start_date=report['start_date'],
            end_date=report['end_date'],
            portfolio_avg_revenue=avg_portfolio,
            total_portfolio_revenue=total_portfolio,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return Response(html_content, mimetype='text/html')


@app.route('/files')
def list_files():
    pdfs  = sorted(f for f in os.listdir(PDF_DIR)  if f.endswith('.pdf')) if os.path.isdir(PDF_DIR) else []
    htmls = sorted(f for f in os.listdir(HTML_DIR) if f.endswith('.html')) if os.path.isdir(HTML_DIR) else []
    return jsonify({'pdfs': pdfs, 'htmls': htmls})


# ── Forecasting Dashboard ─────────────────────────────────────────────────────

@app.route('/forecasting')
def forecasting():
    alert_count = ds.get_unacknowledged_count()
    report      = ds.get_latest_report()
    all_brands  = ds.get_all_brands_in_db()
    if not all_brands:
        return render_template('portal/forecasting.html', forecasts={},
                               forecasts_json='{}', depletions={},
                               growing_count=0, declining_count=0,
                               stable_count=0, stock_warning_count=0,
                               report=None, alert_count=alert_count)

    brand_histories = {b: list(reversed(ds.get_brand_history(b, limit=12)))
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

    # Serialise forecasts to JSON (no numpy types)
    import json as _json
    forecasts_safe = forecasts

    return render_template('portal/forecasting.html',
                           forecasts=forecasts, forecasts_json=_json.dumps(forecasts_safe),
                           depletions=depletions, report=report,
                           growing_count=growing_count, declining_count=declining_count,
                           stable_count=stable_count, stock_warning_count=stock_warning_count,
                           eligible_3m_count=eligible_3m_count,
                           eligible_6m_count=eligible_6m_count,
                           eligible_12m_count=eligible_12m_count,
                           alert_count=alert_count)


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

    # Build SKU data from the raw file via daily_sales (proxy: brand + skus)
    # For a real SKU breakdown we read from the latest in-memory data via DB
    brand_kpis_rows = ds.get_all_brand_kpis(report['id'])

    # Approximate SKU list from daily_sales aggregated at brand level
    # (Full SKU detail requires re-reading the raw file — we use brand summary here)
    sku_data = []
    brand_sku_counts = []
    for bk in brand_kpis_rows:
        brand_sku_counts.append({'brand': bk['brand_name'], 'sku_count': bk['unique_skus']})
        # Revenue-per-sku estimate for visual
        if bk['unique_skus'] > 0:
            rev_per_sku = bk['total_revenue'] / bk['unique_skus']
            qty_per_sku = bk['total_qty'] / bk['unique_skus']
            sku_data.append({
                'name': f"{bk['brand_name']} — Avg SKU",
                'brand': bk['brand_name'],
                'revenue': round(rev_per_sku, 0),
                'qty': round(qty_per_sku, 0),
                'stores': bk['num_stores'],
            })

    sku_data.sort(key=lambda x: x['revenue'], reverse=True)
    brands = [bk['brand_name'] for bk in brand_kpis_rows]

    return render_template('portal/sku_analytics.html',
                           sku_data=sku_data,
                           sku_json=_json.dumps(sku_data),
                           brand_sku_counts_json=_json.dumps(brand_sku_counts),
                           brands=brands, report=report, alert_count=alert_count)


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
    return redirect(url_for('alert_rules_view'))


@app.route('/api/toggle_alert_rule', methods=['POST'])
def api_toggle_alert_rule():
    rule_id = int(request.form.get('rule_id', 0))
    active  = int(request.form.get('active', 1))
    ds.toggle_alert_rule(rule_id, active)
    return redirect(url_for('alert_rules_view'))


@app.route('/api/delete_alert_rule', methods=['POST'])
def api_delete_alert_rule():
    rule_id = int(request.form.get('rule_id', 0))
    ds.delete_alert_rule(rule_id)
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
    report = ds.get_latest_report()
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
        writer.writerows(kpis)
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
                         round(bk['total_revenue'], 0), round(rev_per, 0)])
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

@app.route('/api/activity')
def api_activity():
    limit = min(int(request.args.get('limit', 50)), 200)
    return jsonify(ds.get_activity_log(limit=limit))


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

    report = ds.get_latest_report()

    # ── HELP ──────────────────────────────────────────────────────────────────
    if cmd in ('HELP', 'HI', 'HELLO', 'START', '?'):
        return twiml_reply(
            "DALA Analytics Bot\n\n"
            "Commands:\n"
            "• REPORT — Portfolio summary\n"
            "• REPORT [brand name] — Brand summary\n"
            "• ALERTS — Active alerts\n"
            "• BRANDS — All brand partners\n"
            "• HELP — Show this menu\n\n"
            f"Latest data: {report['month_label'] if report else 'No data yet'}"
        )

    # ── ALERTS ────────────────────────────────────────────────────────────────
    if cmd == 'ALERTS':
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
        query_lower = brand_query.lower()
        matched = next(
            (b for b in all_brands if query_lower in b.lower()), None
        )
        if not matched:
            return twiml_reply(
                f"Brand '{brand_query}' not found.\n"
                f"Try: BRANDS to see all brands."
            )

        kpis = ds.get_brand_kpis_single(report['id'], matched)
        if not kpis:
            return twiml_reply(f"{matched} has no data in the latest report.")

        grade  = kpis.get('perf_grade', '-')
        score  = kpis.get('perf_score', 0)
        rev    = kpis.get('total_revenue', 0)
        stores = kpis.get('num_stores', 0)
        repeat = kpis.get('repeat_pct', 0)
        stock  = kpis.get('stock_days_cover', 0)

        lines = [
            f"{matched} — {report['month_label']}",
            f"Grade: {grade} ({score}/100)",
            f"Revenue: N{rev:,.0f}",
            f"Stores: {stores} | Repeat: {repeat:.1f}%",
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
            else:
                result = _run_pipeline_from_df(combined, 'db_import_upload', s, e, ds)
                report_id = result.get('report_id')

            ds.log_activity('db_import_upload',
                            detail=f'{len(file_buffers)} file(s), {s} -> {e}, mode={merge_mode}')
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
