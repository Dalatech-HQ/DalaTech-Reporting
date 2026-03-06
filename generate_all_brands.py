"""
generate_all_brands.py — Master script: clears output, generates PDF + HTML for all 28 brands,
saves all KPIs to SQLite history, generates portfolio dashboard, and runs smart alerts.

Output:
  output/pdf/BrandName_Report_Feb2026.pdf    — 2-page printable PDF
  output/html/BrandName_Report_Feb2026.html  — interactive Plotly dashboard
  output/html/PORTFOLIO_Dashboard_Feb2026.html — master portfolio view

Run: python generate_all_brands.py
"""

import os, sys, shutil, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.ingestion           import load_and_clean, filter_by_date, split_by_brand
from modules.kpi                 import calculate_kpis, calculate_perf_score
from modules.pdf_generator_html  import generate_pdf_html
from modules.html_generator      import generate_html
from modules.portfolio_generator import generate_portfolio_html
from modules.data_store          import DataStore
from modules.alerts              import check_and_save_alerts, run_portfolio_alerts

# ── Config ────────────────────────────────────────────────────────────────────
XLS        = 'extracted/February Monthly Report/Raw_Files_From_Tally/febSalesReportData.xls'
START_DATE = '2026-02-01'
END_DATE   = '2026-02-28'
PDF_DIR    = os.path.join('output', 'pdf')
HTML_DIR   = os.path.join('output', 'html')

# ── Clear output folder ───────────────────────────────────────────────────────
print("Clearing output folder...")
if os.path.exists('output'):
    shutil.rmtree('output')
os.makedirs(PDF_DIR,  exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)
print("Output folder cleared and recreated.\n")

# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading data...")
df_all     = load_and_clean(XLS)
df_ranged  = filter_by_date(df_all, START_DATE, END_DATE)
brand_data = split_by_brand(df_ranged)

brands = list(brand_data.keys())
print(f"Brands with sales: {len(brands)}")
print(f"Brands: {', '.join(brands)}\n")

# ── Compute all KPIs first (needed for portfolio aggregates) ──────────────────
print("Computing KPIs...")
all_kpis = {}
for brand in brands:
    all_kpis[brand] = calculate_kpis(brand_data[brand])

total_portfolio_revenue = sum(k['total_revenue'] for k in all_kpis.values())
portfolio_avg_revenue   = total_portfolio_revenue / max(len(brands), 1)
print(f"Total portfolio revenue: {total_portfolio_revenue:,.0f}")
print(f"Avg brand revenue:       {portfolio_avg_revenue:,.0f}\n")

# ── Add perf scores to each KPI dict ─────────────────────────────────────────
for brand in brands:
    perf = calculate_perf_score(all_kpis[brand], portfolio_avg_revenue)
    all_kpis[brand]['perf_score'] = perf

# ── Save to SQLite ────────────────────────────────────────────────────────────
print("Saving to database...")
ds = DataStore()

all_stores = set()
for k in all_kpis.values():
    if k.get('top_stores') is not None and not k['top_stores'].empty:
        all_stores.update(k['top_stores']['Store'].tolist())

report_id = ds.save_report(
    start_date=START_DATE,
    end_date=END_DATE,
    xls_filename=os.path.basename(XLS),
    total_revenue=total_portfolio_revenue,
    total_qty=sum(k['total_qty'] for k in all_kpis.values()),
    total_stores=len(all_stores),
    brand_count=len(brands),
)
print(f"Report saved (ID={report_id})\n")

for brand in brands:
    k = all_kpis[brand]
    ps = k.get('perf_score', {})
    portfolio_share = round(k['total_revenue'] / max(total_portfolio_revenue, 1) * 100, 2)
    ds.save_brand_kpis(report_id, brand, k, ps, portfolio_share)
    if not k['daily_sales'].empty:
        ds.save_daily_sales(report_id, brand, k['daily_sales'])
    history = ds.get_brand_history(brand, limit=3)
    check_and_save_alerts(report_id, brand, k, portfolio_avg_revenue, history[1:], ds)

run_portfolio_alerts(report_id, ds.get_all_brand_kpis(report_id), ds)
alert_count = len(ds.get_alerts(report_id))
print(f"Alerts generated: {alert_count}\n")

# ── Generate brand reports ────────────────────────────────────────────────────
ok_pdf  = 0
ok_html = 0
errors  = []

for i, brand_name in enumerate(brands, 1):
    safe      = brand_name.replace(' ', '_').replace("'", '').replace('/', '-')
    pdf_path  = os.path.join(PDF_DIR,  f"{safe}_Report_Feb2026.pdf")
    html_path = os.path.join(HTML_DIR, f"{safe}_Report_Feb2026.html")

    print(f"[{i:2d}/{len(brands)}] {brand_name}")

    kpis = all_kpis[brand_name]

    # PDF
    try:
        generate_pdf_html(
            output_path=pdf_path,
            brand_name=brand_name,
            kpis=kpis,
            start_date=START_DATE,
            end_date=END_DATE,
            portfolio_avg_revenue=portfolio_avg_revenue,
            total_portfolio_revenue=total_portfolio_revenue,
        )
        print(f"         PDF  -> {os.path.basename(pdf_path)}")
        ok_pdf += 1
    except Exception as e:
        print(f"         PDF ERROR: {e}")
        errors.append((brand_name, 'PDF', str(e)))

    # HTML
    try:
        generate_html(
            output_path=html_path,
            brand_name=brand_name,
            kpis=kpis,
            start_date=START_DATE,
            end_date=END_DATE,
            portfolio_avg_revenue=portfolio_avg_revenue,
            total_portfolio_revenue=total_portfolio_revenue,
        )
        print(f"         HTML -> {os.path.basename(html_path)}")
        ok_html += 1
    except Exception as e:
        print(f"         HTML ERROR: {e}")
        errors.append((brand_name, 'HTML', str(e)))

# ── Portfolio dashboard ───────────────────────────────────────────────────────
print("\nGenerating portfolio dashboard...")
portfolio_path = os.path.join(HTML_DIR, 'PORTFOLIO_Dashboard_Feb2026.html')
try:
    generate_portfolio_html(
        output_path=portfolio_path,
        all_brand_kpis=all_kpis,
        brand_data_raw=brand_data,
        start_date=START_DATE,
        end_date=END_DATE,
        total_portfolio_revenue=total_portfolio_revenue,
    )
    print(f"Portfolio -> {os.path.basename(portfolio_path)}")
except Exception as e:
    print(f"Portfolio ERROR: {e}")
    errors.append(('PORTFOLIO', 'Dashboard', str(e)))

# ── Brand partner tokens ──────────────────────────────────────────────────────
print("\nEnsuring brand partner tokens...")
for brand in brands:
    ds.get_or_create_token(brand)
print(f"Tokens ready for {len(brands)} brands.")

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  Done.")
print(f"  PDFs generated:        {ok_pdf}/{len(brands)}")
print(f"  HTMLs generated:       {ok_html}/{len(brands)}")
print(f"  Portfolio dashboard:   output/html/PORTFOLIO_Dashboard_Feb2026.html")
print(f"  Database report ID:    {report_id}")
print(f"  Alerts:                {alert_count}")
print(f"  Portal:                python app.py  ->  http://127.0.0.1:5000")

if errors:
    print(f"\n  Errors ({len(errors)}):")
    for brand, kind, msg in errors:
        print(f"    [{kind}] {brand}: {msg}")

print(f"\n  PDF files  -> output/pdf/")
print(f"  HTML files -> output/html/")
print(f"{'='*60}")
