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

import os, io, json, traceback, shutil, uuid, threading
from datetime import datetime
from functools import wraps

from flask import (
    Flask, render_template, request, send_file,
    jsonify, redirect, url_for, abort, session,
)

# In-memory job tracker for async generation
_JOBS = {}   # job_id -> {status, progress, total, current_brand, brands_done, errors, report_id}

from modules.ingestion        import load_and_clean, filter_by_date, split_by_brand
from modules.kpi              import calculate_kpis, calculate_perf_score, generate_narrative
from modules.pdf_generator_html import generate_pdf_html
from modules.html_generator   import generate_html
from modules.portfolio_generator import generate_portfolio_html
from modules.data_store       import DataStore
from modules.alerts           import check_and_save_alerts, run_portfolio_alerts
from modules.predictor        import build_brand_forecasts, stock_depletion_date, growth_label, growth_color
from modules.historical       import (
    get_brand_monthly_history, get_portfolio_monthly_trend,
    get_repeat_purchase_map_data, generate_insights,
    get_color_scheme_for_month, get_monthly_metrics
)
from modules.geocoding        import is_geocoding_available

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
    import numpy as np

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
    date_min  = df['Date'].min()
    date_max  = df['Date'].max()
    file_size_kb = round(file.content_length / 1024, 1) if file.content_length else 0

    # ── Vch type breakdown ────────────────────────────────────────────────────
    vch_counts = df['Vch Type'].value_counts().to_dict()
    sales_df = df[df['Vch Type'] == 'Sales']

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

def _run_generation(job_id, file_bytes, start_date, end_date, selected_brands, filename):
    """Background thread: runs full generation and updates _JOBS[job_id]."""
    job = _JOBS[job_id]
    try:
        df_all    = load_and_clean(io.BytesIO(file_bytes))
        df_ranged = filter_by_date(df_all, start_date, end_date)
        brand_data = split_by_brand(df_ranged)

        # Filter to selected brands
        if selected_brands:
            brand_data = {b: df for b, df in brand_data.items() if b in selected_brands}

        brands = list(brand_data.keys())
        job['total'] = len(brands) + 1  # +1 for portfolio

        # Compute all KPIs
        all_kpis = {}
        for b in brands:
            all_kpis[b] = calculate_kpis(brand_data[b])

        total_portfolio_revenue = sum(k['total_revenue'] for k in all_kpis.values())
        portfolio_avg_revenue   = total_portfolio_revenue / max(len(brands), 1)

        for b in brands:
            all_kpis[b]['perf_score'] = calculate_perf_score(all_kpis[b], portfolio_avg_revenue)

        # Save report to DB
        all_stores = set()
        for k in all_kpis.values():
            if k.get('top_stores') is not None and not k['top_stores'].empty:
                all_stores.update(k['top_stores']['Store'].tolist())

        report_id = ds.save_report(
            start_date=start_date, end_date=end_date,
            xls_filename=filename,
            total_revenue=total_portfolio_revenue,
            total_qty=sum(k['total_qty'] for k in all_kpis.values()),
            total_stores=len(all_stores),
            brand_count=len(brands),
        )
        job['report_id'] = report_id

        # Save KPIs + alerts
        for b in brands:
            k = all_kpis[b]
            ps = k.get('perf_score', {})
            share = round(k['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)
            ds.save_brand_kpis(report_id, b, k, ps, share)
            if not k['daily_sales'].empty:
                ds.save_daily_sales(report_id, b, k['daily_sales'])
            history = ds.get_brand_history(b, limit=3)
            check_and_save_alerts(report_id, b, k, portfolio_avg_revenue, history[1:], ds)
        run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)

        # Generate per-brand files
        start_dt  = datetime.strptime(start_date, '%Y-%m-%d')
        month_tag = start_dt.strftime('%b%Y')

        for i, brand_name in enumerate(brands):
            job['current_brand'] = brand_name
            safe      = _safe_name(brand_name)
            pdf_path  = os.path.join(PDF_DIR,  f"{safe}_Report_{month_tag}.pdf")
            html_path = os.path.join(HTML_DIR, f"{safe}_Report_{month_tag}.html")
            kpis      = all_kpis[brand_name]

            brand_result = {'brand': brand_name, 'pdf': False, 'html': False, 'error': None}

            try:
                generate_pdf_html(output_path=pdf_path, brand_name=brand_name, kpis=kpis,
                                  start_date=start_date, end_date=end_date,
                                  portfolio_avg_revenue=portfolio_avg_revenue,
                                  total_portfolio_revenue=total_portfolio_revenue)
                brand_result['pdf'] = True
            except Exception as e:
                brand_result['error'] = str(e)

            try:
                generate_html(output_path=html_path, brand_name=brand_name, kpis=kpis,
                              start_date=start_date, end_date=end_date,
                              portfolio_avg_revenue=portfolio_avg_revenue,
                              total_portfolio_revenue=total_portfolio_revenue)
                brand_result['html'] = True
            except Exception as e:
                brand_result['error'] = (brand_result['error'] or '') + ' HTML:' + str(e)

            job['brands_done'].append(brand_result)
            job['progress'] = round((i + 1) / job['total'] * 100)

        # Portfolio dashboard
        job['current_brand'] = 'Portfolio Dashboard'
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
            job['portfolio_file'] = os.path.basename(portfolio_path)
        except Exception as e:
            job['errors'].append(f'Portfolio: {e}')

        for b in brands:
            ds.get_or_create_token(b)

        job['progress'] = 100
        job['status']   = 'done'
        job['current_brand'] = None

    except Exception as exc:
        job['status'] = 'error'
        job['error_msg'] = str(exc)


@app.route('/api/generate_async', methods=['POST'])
def generate_async():
    """Start background generation. Returns {job_id} immediately."""
    file           = request.files.get('tally_file')
    start_date     = request.form.get('start_date', '').strip()
    end_date       = request.form.get('end_date', '').strip()
    selected_raw   = request.form.get('selected_brands', '')
    selected_brands = [b.strip() for b in selected_raw.split(',') if b.strip()] if selected_raw else []

    if not file or not start_date or not end_date:
        return jsonify({'success': False, 'error': 'Missing file or dates'}), 400

    file_bytes = file.read()
    job_id     = uuid.uuid4().hex
    _JOBS[job_id] = {
        'status':       'running',
        'progress':     0,
        'total':        0,
        'current_brand': None,
        'brands_done':  [],
        'errors':       [],
        'report_id':    None,
        'portfolio_file': None,
    }

    t = threading.Thread(
        target=_run_generation,
        args=(job_id, file_bytes, start_date, end_date, selected_brands, file.filename),
        daemon=True,
    )
    t.start()
    return jsonify({'success': True, 'job_id': job_id})


@app.route('/api/generation_status/<job_id>')
def generation_status(job_id):
    job = _JOBS.get(job_id)
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
    import pandas as pd
    
    # Load historical data
    hist_path = os.path.join(BASE_DIR, '2024to2026salesreport.xlsx')
    if not os.path.exists(hist_path):
        return render_template('portal/trends.html', 
                               error="Historical data not available",
                               metrics=None, insights=None)
    
    try:
        df = pd.read_excel(hist_path)
        df['Date'] = pd.to_datetime(df['Date'])
    except Exception as e:
        return render_template('portal/trends.html',
                               error=f"Error loading data: {e}",
                               metrics=None, insights=None)
    
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
        return render_template('portal/trends.html',
                               error="No data for selected month",
                               metrics=None, insights=None)
    
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
    
    return render_template('portal/trends.html',
                           metrics=metrics,
                           insights=insights,
                           historical=historical,
                           historical_json=json.dumps(historical),
                           top_stores=top_stores,
                           top_stores_json=json.dumps(top_stores),
                           top_products=top_products,
                           top_products_json=json.dumps(top_products),
                           map_data=map_data,
                           map_data_json=json.dumps(map_data_with_coords),
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
    import pandas as pd
    
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

    # Save report to DB
    all_stores = set()
    for k in all_kpis.values():
        if k.get('top_stores') is not None and not k['top_stores'].empty:
            all_stores.update(k['top_stores']['Store'].tolist())

    report_id = ds.save_report(
        start_date=start_date,
        end_date=end_date,
        xls_filename=file.filename,
        total_revenue=total_portfolio_revenue,
        total_qty=sum(k['total_qty'] for k in all_kpis.values()),
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

    # Attach forecast label to each brand from history
    forecasts = {}
    for b in all_brand_names:
        hist = ds.get_brand_revenue_trend(b, limit=6)
        hist_oldest_first = list(reversed(hist))
        forecasts[b] = {
            'growth_label': growth_label(hist_oldest_first),
            'growth_color': growth_color(growth_label(hist_oldest_first)),
        }

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
    forecast = build_brand_forecasts({brand_name: hist_oldest}).get(brand_name, {})

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
    forecast = build_brand_forecasts({brand_name: hist_oldest}).get(brand_name, {})
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


@app.route('/files')
def list_files():
    pdfs  = sorted(f for f in os.listdir(PDF_DIR)  if f.endswith('.pdf')) if os.path.isdir(PDF_DIR) else []
    htmls = sorted(f for f in os.listdir(HTML_DIR) if f.endswith('.html')) if os.path.isdir(HTML_DIR) else []
    return jsonify({'pdfs': pdfs, 'htmls': htmls})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 60)
    print(f"  DALA Analytics Portal — http://127.0.0.1:{port}")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=port, use_reloader=False)
