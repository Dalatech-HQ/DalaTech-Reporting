"""
kpi.py — KPI calculation, reorder analysis, and narrative generation.

All business logic lives here. The PDF generator and chart modules
consume the dict returned by calculate_kpis().
"""

import pandas as pd

VCH_SALES               = 'Sales'
VCH_AVAILABLE_INVENTORY = 'Available Inventory'
VCH_INVENTORY_PICKUP    = 'Inventory Pickup by Dala'
VCH_INVENTORY_SUPPLIED  = 'Inventory Supplied by Brands'


# ── Public API ───────────────────────────────────────────────────────────────

def calculate_kpis(brand_df):
    """
    Calculate all KPIs and analysis for a single brand partner.

    Args:
        brand_df: DataFrame slice for one brand (all Vch Types included).

    Returns:
        dict with every metric needed by the PDF generator and chart module.
    """
    sales      = brand_df[brand_df['Vch Type'] == VCH_SALES].copy()
    avail_inv  = brand_df[brand_df['Vch Type'] == VCH_AVAILABLE_INVENTORY].copy()
    inv_pickup = brand_df[brand_df['Vch Type'] == VCH_INVENTORY_PICKUP].copy()
    inv_supply = brand_df[brand_df['Vch Type'] == VCH_INVENTORY_SUPPLIED].copy()

    # ── Core KPIs ────────────────────────────────────────────────────────────
    total_revenue        = sales['Sales_Value'].sum()
    total_qty            = sales['Quantity'].sum()
    unique_skus          = sales['SKUs'].nunique()
    num_stores           = sales['Particulars'].nunique()
    avg_revenue_per_store = (total_revenue / num_stores) if num_stores > 0 else 0

    # ── Top 10 Stores by Revenue ──────────────────────────────────────────────
    top_stores = (
        sales.groupby('Particulars')['Sales_Value']
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    top_stores.columns = ['Store', 'Revenue']

    # ── Product Performance ───────────────────────────────────────────────────
    product_qty = (
        sales.groupby('SKUs')['Quantity']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    product_qty.columns = ['SKU', 'Quantity']

    product_value = (
        sales.groupby('SKUs')['Sales_Value']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    product_value.columns = ['SKU', 'Revenue']

    # ── Daily Sales Trend ─────────────────────────────────────────────────────
    daily_sales = (
        sales.groupby('Date')
        .agg(Revenue=('Sales_Value', 'sum'), Quantity=('Quantity', 'sum'))
        .reset_index()
        .sort_values('Date')
    )

    # ── Peak Day ──────────────────────────────────────────────────────────────
    if not daily_sales.empty:
        peak_row     = daily_sales.loc[daily_sales['Revenue'].idxmax()]
        peak_date    = peak_row['Date']
        peak_revenue = peak_row['Revenue']
        peak_qty     = peak_row['Quantity']
    else:
        peak_date = peak_revenue = peak_qty = None

    # ── Top Store Spotlight ───────────────────────────────────────────────────
    if not top_stores.empty:
        top_store_name    = top_stores.iloc[0]['Store']
        top_store_revenue = top_stores.iloc[0]['Revenue']
        top_store_pct     = (top_store_revenue / total_revenue * 100) if total_revenue > 0 else 0
    else:
        top_store_name = 'N/A'
        top_store_revenue = top_store_pct = 0

    # ── Top SKU ───────────────────────────────────────────────────────────────
    if not product_qty.empty:
        top_sku     = product_qty.iloc[0]['SKU']
        top_sku_qty = product_qty.iloc[0]['Quantity']
    else:
        top_sku = 'N/A'
        top_sku_qty = 0

    # ── Reorder Analysis ──────────────────────────────────────────────────────
    reorder_df    = _calculate_reorders(sales)
    repeat_stores = int((reorder_df['Order Count'] >= 2).sum()) if not reorder_df.empty else 0
    single_stores = int((reorder_df['Order Count'] == 1).sum()) if not reorder_df.empty else 0

    # ── Trading Days ───────────────────────────────────────────────────────────
    trading_days = int(daily_sales['Date'].nunique()) if not daily_sales.empty else 0

    # ── Weekly Breakdown (for WoW% sparklines + WoW change) ───────────────────
    if not sales.empty and not daily_sales.empty:
        min_date = pd.Timestamp(daily_sales['Date'].min())
        weekly_rev = []
        weekly_qty = []
        for week_num in range(4):
            w_start = min_date + pd.Timedelta(days=week_num * 7)
            w_end   = w_start + pd.Timedelta(days=6)
            mask    = (sales['Date'] >= w_start) & (sales['Date'] <= w_end)
            weekly_rev.append(float(sales.loc[mask, 'Sales_Value'].sum()))
            weekly_qty.append(float(sales.loc[mask, 'Quantity'].sum()))
        tot_rev = sum(weekly_rev) or 1
        tot_qty = sum(weekly_qty) or 1
        weekly_rev_pct = [round(v / tot_rev * 100, 1) for v in weekly_rev]
        weekly_qty_pct = [round(v / tot_qty * 100, 1) for v in weekly_qty]
        # W4 vs W3 change
        wow_rev_change = round((weekly_rev[3] - weekly_rev[2]) / weekly_rev[2] * 100, 1) \
                         if weekly_rev[2] > 0 else 0.0
        wow_qty_change = round((weekly_qty[3] - weekly_qty[2]) / weekly_qty[2] * 100, 1) \
                         if weekly_qty[2] > 0 else 0.0
    else:
        weekly_rev_pct = [0, 0, 0, 0]
        weekly_qty_pct = [0, 0, 0, 0]
        wow_rev_change = 0.0
        wow_qty_change = 0.0

    # ── Repeat % (for scorecard) ───────────────────────────────────────────────
    total_stores_stocked = repeat_stores + single_stores
    repeat_pct = round(repeat_stores / total_stores_stocked * 100, 1) \
                 if total_stores_stocked > 0 else 0.0

    # ── Store Frequency Heatmap Data ──────────────────────────────────────────
    if not sales.empty and not top_stores.empty:
        top8 = top_stores.head(8)['Store'].tolist()
        hmap = sales[sales['Particulars'].isin(top8)].copy()
        hmap['Date'] = pd.to_datetime(hmap['Date'])
        store_heatmap_df = (
            hmap.groupby(['Particulars', 'Date'])['Vch No.']
            .nunique()
            .reset_index()
            .rename(columns={'Particulars': 'Store', 'Vch No.': 'Orders'})
        )
    else:
        store_heatmap_df = pd.DataFrame(columns=['Store', 'Date', 'Orders'])

    # ── Closing Stock (Available Inventory rows) ──────────────────────────────
    closing_stock = avail_inv[['SKUs', 'Quantity']].copy()
    closing_stock.columns = ['SKU', 'Closing Stock (Cartons)']
    total_closing_stock = closing_stock['Closing Stock (Cartons)'].sum()

    # ── Inventory Pickup by DALA ──────────────────────────────────────────────
    pickup_summary = (
        inv_pickup.groupby('SKUs').agg(
            Quantity=('Quantity', 'sum'),
            Value=('Sales_Value', 'sum')
        ).reset_index()
    )
    pickup_summary.columns = ['SKU', 'Qty Picked Up', 'Value']
    total_pickup_qty   = inv_pickup['Quantity'].sum()
    total_pickup_value = inv_pickup['Sales_Value'].sum()

    # ── Inventory Supplied by Brand ───────────────────────────────────────────
    supply_summary = (
        inv_supply.groupby('SKUs').agg(
            Quantity=('Quantity', 'sum'),
            Value=('Sales_Value', 'sum')
        ).reset_index()
    )
    supply_summary.columns = ['SKU', 'Qty Supplied', 'Value']
    total_supplied_qty   = inv_supply['Quantity'].sum()
    total_supplied_value = inv_supply['Sales_Value'].sum()

    # ── Inventory Health ───────────────────────────────────────────────────────
    avg_daily_qty   = total_qty / max(trading_days, 1)
    stock_days_cover = round(total_closing_stock / max(avg_daily_qty, 0.01), 1) \
                       if total_closing_stock > 0 else 0.0
    if total_closing_stock == 0:
        inv_health_status = 'No Stock Data'
        inv_health_color  = 'gray'
    elif stock_days_cover >= 14:
        inv_health_status = 'Healthy'
        inv_health_color  = 'green'
    elif stock_days_cover >= 7:
        inv_health_status = 'Watch'
        inv_health_color  = 'amber'
    else:
        inv_health_status = 'Low Stock'
        inv_health_color  = 'red'

    return {
        # Core KPIs
        'total_revenue':         total_revenue,
        'total_qty':             total_qty,
        'unique_skus':           unique_skus,
        'num_stores':            num_stores,
        'avg_revenue_per_store': avg_revenue_per_store,

        # Chart data
        'top_stores':    top_stores,
        'product_qty':   product_qty,
        'product_value': product_value,
        'daily_sales':   daily_sales,

        # Spotlight values for narrative + header callouts
        'top_store_name':    top_store_name,
        'top_store_revenue': top_store_revenue,
        'top_store_pct':     top_store_pct,
        'top_sku':           top_sku,
        'top_sku_qty':       top_sku_qty,
        'peak_date':         peak_date,
        'peak_revenue':      peak_revenue,
        'peak_qty':          peak_qty,

        # Reorder analysis
        'reorder_analysis': reorder_df,
        'repeat_stores':    repeat_stores,
        'single_stores':    single_stores,

        # Inventory
        'closing_stock':       closing_stock,
        'total_closing_stock': total_closing_stock,
        'pickup_summary':      pickup_summary,
        'total_pickup_qty':    total_pickup_qty,
        'total_pickup_value':  total_pickup_value,
        'supply_summary':      supply_summary,
        'total_supplied_qty':  total_supplied_qty,
        'total_supplied_value':total_supplied_value,

        # Weekly breakdown + WoW changes
        'weekly_rev_pct':  weekly_rev_pct,
        'weekly_qty_pct':  weekly_qty_pct,
        'wow_rev_change':  wow_rev_change,
        'wow_qty_change':  wow_qty_change,

        # Activity
        'trading_days': trading_days,
        'repeat_pct':   repeat_pct,

        # Inventory health
        'inv_health_status': inv_health_status,
        'inv_health_color':  inv_health_color,
        'stock_days_cover':  stock_days_cover,

        # Store frequency heatmap
        'store_heatmap_df': store_heatmap_df,

        # Raw slices (for Phase 3 Google Sheets push)
        'sales_df': sales,
        'full_df':  brand_df,
    }


def generate_narrative(brand_name, kpis, start_date, end_date):
    """
    Generate a deterministic business summary for report exports.
    """
    start  = pd.Timestamp(start_date)
    end    = pd.Timestamp(end_date)
    period = f"{start.strftime('%B %d')} – {end.strftime('%B %d, %Y')}"

    def fmt(v):
        return f"₦{v:,.0f}"

    def fmt_qty(v):
        return f"{float(v):,.1f}"

    def wow_phrase(value):
        value = float(value or 0)
        if value >= 1:
            return f"up {value:.1f}% versus the prior week"
        if value <= -1:
            return f"down {abs(value):.1f}% versus the prior week"
        return "largely flat versus the prior week"

    total_tracked = int(kpis['repeat_stores'] + kpis['single_stores'])
    repeat_rate = float(kpis.get('repeat_pct', 0) or 0)
    stock_total = float(kpis.get('total_closing_stock', 0) or 0)
    stock_days = float(kpis.get('stock_days_cover', 0) or 0)
    inv_status = kpis.get('inv_health_status') or 'No Stock Data'

    if inv_status == 'Healthy':
        action_line = "Stock cover remains healthy, so the focus should stay on protecting sell-through in the strongest outlets."
    elif inv_status == 'Watch':
        action_line = "Stock cover is tightening, so the faster-moving SKUs should be replenished early in the next cycle."
    elif inv_status == 'Low Stock':
        action_line = "Stock cover is low, so replenishing the fastest-moving SKUs should be the immediate priority."
    elif inv_status == 'Overstocked':
        action_line = "Stock is sitting above the ideal level, so pushing sell-through on slower lines should be the priority."
    else:
        action_line = "Warehouse stock records should be reviewed alongside sales performance before the next planning cycle."

    s1 = (
        f"From {period}, {brand_name} generated {fmt(kpis['total_revenue'])} in revenue, "
        f"sold {fmt_qty(kpis['total_qty'])} packs, and reached {kpis['num_stores']} "
        f"store{'s' if kpis['num_stores'] != 1 else ''} across {kpis['unique_skus']} SKU"
        f"{'s' if kpis['unique_skus'] != 1 else ''}. "
        f"Average revenue per store closed at {fmt(kpis['avg_revenue_per_store'])}."
    )

    s2 = (
        f"The strongest outlet was {kpis['top_store_name']}, contributing "
        f"{kpis['top_store_pct']:.1f}% of sales worth {fmt(kpis['top_store_revenue'])}. "
        f"The leading SKU by volume was {kpis['top_sku']}, with {fmt_qty(kpis['top_sku_qty'])} packs sold."
    )

    if kpis['peak_date'] is not None:
        s3 = (
            f"Sales peaked on {pd.Timestamp(kpis['peak_date']).strftime('%B %d')} at "
            f"{fmt(kpis['peak_revenue'])} from {fmt_qty(kpis['peak_qty'])} packs. "
            f"Revenue finished {wow_phrase(kpis.get('wow_rev_change', 0))}, while quantity ended {wow_phrase(kpis.get('wow_qty_change', 0))}."
        )
    else:
        s3 = (
            f"Revenue finished {wow_phrase(kpis.get('wow_rev_change', 0))}, "
            f"while quantity ended {wow_phrase(kpis.get('wow_qty_change', 0))}."
        )

    if total_tracked > 0:
        s4 = (
            f"{kpis['repeat_stores']} of the {total_tracked} tracked stores reordered during the period, "
            f"which puts repeat purchase at {repeat_rate:.0f}%."
        )
        if kpis['single_stores'] > 0:
            s4 += (
                f" The remaining {kpis['single_stores']} store{'s' if kpis['single_stores'] != 1 else ''} "
                f"ordered once and should be the clearest follow-up opportunities."
            )
    else:
        s4 = "No reorder activity was captured in the tracked store set for this period."

    if stock_total > 0:
        s5 = (
            f"Closing stock ended at {fmt_qty(stock_total)} packs, equal to about {stock_days:.1f} days of cover, "
            f"and inventory is currently marked {inv_status}. {action_line}"
        )
    else:
        s5 = action_line

    return ' '.join(s for s in [s1, s2, s3, s4, s5] if s)


def calculate_perf_score(kpis, portfolio_avg_revenue=None):
    """
    Score 0-100 across 4 dimensions. Returns dict with total, grade, and subscores.

    Dimensions (each 0-25):
      Revenue   — vs portfolio average
      Loyalty   — repeat customer %
      Reach     — unique stores stocked
      Activity  — trading days in period
    """
    avg = portfolio_avg_revenue or kpis['total_revenue']
    rev_ratio = kpis['total_revenue'] / max(avg, 1)
    if rev_ratio >= 2.0:   rev_s = 25
    elif rev_ratio >= 1.5: rev_s = 20
    elif rev_ratio >= 1.0: rev_s = 15
    elif rev_ratio >= 0.5: rev_s = 10
    else:                  rev_s = 5

    rp = kpis['repeat_pct']
    if rp >= 75:   loy_s = 25
    elif rp >= 50: loy_s = 20
    elif rp >= 25: loy_s = 15
    elif rp >= 10: loy_s = 10
    else:          loy_s = 5

    st = kpis['num_stores']
    if st >= 20:  reach_s = 25
    elif st >= 10: reach_s = 20
    elif st >= 5:  reach_s = 15
    elif st >= 2:  reach_s = 10
    else:          reach_s = 5

    td = kpis['trading_days']
    if td >= 20:   act_s = 25
    elif td >= 15: act_s = 20
    elif td >= 10: act_s = 15
    elif td >= 5:  act_s = 10
    else:          act_s = 5

    total = rev_s + loy_s + reach_s + act_s
    if total >= 80:   grade = 'A'
    elif total >= 60: grade = 'B'
    elif total >= 40: grade = 'C'
    else:             grade = 'D'

    grade_color = {'A': '#1E8449', 'B': '#2E86C1', 'C': '#B7770D', 'D': '#E8192C'}[grade]
    grade_label = {'A': 'Excellent', 'B': 'Good', 'C': 'Average', 'D': 'Needs Attention'}[grade]

    return {
        'total':          total,
        'grade':          grade,
        'grade_color':    grade_color,
        'grade_label':    grade_label,
        'revenue_score':  rev_s,
        'loyalty_score':  loy_s,
        'reach_score':    reach_s,
        'activity_score': act_s,
    }


# ── Internal helpers ─────────────────────────────────────────────────────────

def _calculate_reorders(sales_df):
    """
    Reorder = same store (Particulars) with 2+ distinct Vch No. in the period.
    Returns a sorted DataFrame with order count, date range, revenue, and status.
    """
    if sales_df.empty:
        return pd.DataFrame(
            columns=['Store', 'Order Count', 'First Order', 'Last Order', 'Total Revenue', 'Status']
        )

    agg = (
        sales_df.groupby('Particulars')
        .agg(
            order_count   =('Vch No.', 'nunique'),
            first_order   =('Date', 'min'),
            last_order    =('Date', 'max'),
            total_revenue =('Sales_Value', 'sum'),
        )
        .reset_index()
    )
    agg.columns = ['Store', 'Order Count', 'First Order', 'Last Order', 'Total Revenue']
    agg = agg.sort_values('Order Count', ascending=False).reset_index(drop=True)
    agg['Status'] = agg['Order Count'].apply(
        lambda x: 'Repeat Customer' if x >= 2 else 'Single Order — Follow Up'
    )

    return agg


def calculate_churn(current_df, prev_df):
    """
    Cross-period store churn analysis for a single brand.

    Returns a dict with:
      churned_stores:     stores active last period, silent this period
      reactivated_stores: stores absent last period, back this period
      new_stores:         stores appearing for the first time this period
      retained_stores:    stores active in both periods
    Each list contains dicts: {store, prev_revenue, curr_revenue}
    """
    if current_df is None or current_df.empty:
        return {'churned_stores': [], 'reactivated_stores': [], 'new_stores': [], 'retained_stores': []}

    sales_current = current_df[current_df.get('Vch Type', pd.Series(['Sales'] * len(current_df))) == 'Sales'] \
        if 'Vch Type' in current_df.columns else current_df
    curr_stores = set(sales_current['Particulars'].dropna().unique()) \
        if 'Particulars' in sales_current.columns else set()

    curr_rev_by_store = {}
    if 'Particulars' in sales_current.columns and 'Sales_Value' in sales_current.columns:
        curr_rev_by_store = sales_current.groupby('Particulars')['Sales_Value'].sum().to_dict()

    if prev_df is None or (hasattr(prev_df, 'empty') and prev_df.empty):
        new_stores = [{'store': s, 'prev_revenue': 0, 'curr_revenue': curr_rev_by_store.get(s, 0)}
                      for s in curr_stores]
        return {'churned_stores': [], 'reactivated_stores': [], 'new_stores': new_stores, 'retained_stores': []}

    sales_prev = prev_df[prev_df.get('Vch Type', pd.Series(['Sales'] * len(prev_df))) == 'Sales'] \
        if 'Vch Type' in prev_df.columns else prev_df
    prev_stores = set(sales_prev['Particulars'].dropna().unique()) \
        if 'Particulars' in sales_prev.columns else set()

    prev_rev_by_store = {}
    if 'Particulars' in sales_prev.columns and 'Sales_Value' in sales_prev.columns:
        prev_rev_by_store = sales_prev.groupby('Particulars')['Sales_Value'].sum().to_dict()

    churned = sorted(prev_stores - curr_stores)
    new = sorted(curr_stores - prev_stores)
    retained = sorted(prev_stores & curr_stores)

    return {
        'churned_stores':     [{'store': s, 'prev_revenue': prev_rev_by_store.get(s, 0), 'curr_revenue': 0}
                               for s in churned],
        'reactivated_stores': [],  # Would require 3+ periods to detect
        'new_stores':         [{'store': s, 'prev_revenue': 0, 'curr_revenue': curr_rev_by_store.get(s, 0)}
                               for s in new],
        'retained_stores':    [{'store': s, 'prev_revenue': prev_rev_by_store.get(s, 0),
                                'curr_revenue': curr_rev_by_store.get(s, 0)}
                               for s in retained],
    }
