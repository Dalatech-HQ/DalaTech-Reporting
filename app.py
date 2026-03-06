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

import os, io, json, traceback, shutil
from datetime import datetime
from functools import wraps

from flask import (
    Flask, render_template, request, send_file,
    jsonify, redirect, url_for, abort, session,
)

from modules.ingestion        import load_and_clean, filter_by_date, split_by_brand
from modules.kpi              import calculate_kpis, calculate_perf_score, generate_narrative
from modules.pdf_generator_html import generate_pdf_html
from modules.html_generator   import generate_html
from modules.portfolio_generator import generate_portfolio_html
from modules.data_store       import DataStore
from modules.alerts           import check_and_save_alerts, run_portfolio_alerts
from modules.predictor        import build_brand_forecasts, stock_depletion_date, growth_label, growth_color

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
