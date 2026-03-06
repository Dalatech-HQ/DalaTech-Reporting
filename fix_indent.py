with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Extract the api_preview function (lines 88-297)
old_func_start = content.find('def api_preview():')
old_func_end = content.find('# ── Async generate')

# Create the new properly indented function
new_func = '''def api_preview():
    """
    Instant file analysis — parses the Tally export and returns a full data
    profile without generating any reports. Returns JSON in < 3 seconds.
    """
    import numpy as np
    
    # Wrap entire function in try-except to always return JSON
    try:
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
        
        # For very large files (historical data), limit preview to most recent 3 months
        # to avoid timeouts while still giving useful preview
        is_large_file = row_count > 10000
        preview_df = df.copy()
        
        if is_large_file:
            # Get most recent 3 months for preview
            date_max_all = df['Date'].max()
            date_min_preview = date_max_all - pd.Timedelta(days=90)
            preview_df = df[df['Date'] >= date_min_preview].copy()
        
        date_min  = preview_df['Date'].min()
        date_max  = preview_df['Date'].max()
        file_size_kb = round(file.content_length / 1024, 1) if file.content_length else 0

        # ── Vch type breakdown ────────────────────────────────────────────────────
        # Use full df for brand list, preview_df for detailed stats
        vch_counts = preview_df['Vch Type'].value_counts().to_dict()
        sales_df = preview_df[preview_df['Vch Type'] == 'Sales']
        
        # Get all brands from full file
        all_brands_in_file = set(df['Brand Partner'].unique())
        known_brands = set(ds.get_all_brands_in_db())
        brand_stats  = []

        if not sales_df.empty:
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
        active_brands      = set(sales_df['Brand Partner'].unique()) if not sales_df.empty else set()
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
            issues.append({'level': 'info', 'msg': f'{len(zero_sales_brands)} brand(s) have no Sales transactions.'})
            penalty += 3

        if missing_brands:
            issues.append({'level': 'warning', 'msg': f'{len(missing_brands)} brand(s) from your history are absent in this file.'})
            penalty += len(missing_brands) * 2

        new_brands = [b for b in brand_stats if b['is_new']]
        if new_brands:
            issues.append({'level': 'info', 'msg': f'{len(new_brands)} new brand(s) detected (not in history).'})

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
                    issues.append({'level': 'warning', 'msg': f'Revenue spike detected on {spikes.index[0].strftime("%b %d")}.'})

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
            'total_stores':   int(sales_df['Particulars'].nunique()) if not sales_df.empty else 0,
            'total_skus':     int(sales_df['SKUs'].nunique()) if not sales_df.empty else 0,
            'total_revenue':  float(sales_df['Sales_Value'].sum()) if not sales_df.empty else 0,
            'total_qty':      float(sales_df['Quantity'].sum()) if not sales_df.empty else 0,
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
        
    except Exception as e:
        import traceback
        print(f'API Preview Error: {e}')
        print(traceback.format_exc())
        return jsonify({'success': False, 'error': f'Server error: {str(e)}'}), 500

'''

# Replace the old function with the new one
new_content = content[:old_func_start] + new_func + content[old_func_end:]

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print('Fixed api_preview function')
