"""
charts_html.py — Chart generators for HTML-based PDF reports.

All functions return base64-encoded PNG strings for direct embedding in HTML.
"""

import io
import base64
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ── DALA colour palette ───────────────────────────────────────────────────────
C_NAVY   = '#1B2B5E'
C_RED    = '#E8192C'
C_ACCENT = '#2E86C1'
C_GREEN  = '#1E8449'
C_AMBER  = '#C0922A'
C_GRAY   = '#DDE3ED'
C_BG     = '#FFFFFF'
C_TEXT   = '#1A1A2E'
C_MUTED  = '#7A849E'
C_GRID   = '#EAEEF5'

DPI = 150

# ── Internal helpers ──────────────────────────────────────────────────────────

def _save_base64(fig) -> str:
    """Serialize figure to base64-encoded PNG."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=DPI, bbox_inches='tight',
                facecolor=C_BG, edgecolor='none', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def _naira(v):
    """Compact Naira label."""
    if v >= 1_000_000:
        return f'₦{v/1_000_000:.1f}M'
    if v >= 1_000:
        return f'₦{v/1_000:.0f}K'
    return f'₦{v:,.0f}'


def _qty(v):
    """Compact quantity label."""
    s = f'{v:.2f}'.rstrip('0').rstrip('.')
    return s


# ═══════════════════════════════════════════════════════════════════════════════
#  CHART FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def chart_top_stores(top_stores_df, width_in=6.0, height_in=2.5) -> str:
    """Horizontal bar chart — top stores by revenue. Returns base64 PNG."""
    df = top_stores_df.copy()
    if df.empty:
        return ""
    
    n = min(len(df), 8)
    df = df.head(n)
    
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    
    # Reverse so #1 is at top
    df = df.iloc[::-1].reset_index(drop=True)
    colors = [C_RED if i == n-1 else C_NAVY for i in range(n)]
    
    bars = ax.barh(df['Store'], df['Revenue'], color=colors, height=0.6)
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, df['Revenue'])):
        ax.text(val + max(df['Revenue'])*0.01, bar.get_y() + bar.get_height()/2,
                _naira(val), va='center', ha='left', fontsize=8, fontweight='bold')
    
    ax.set_xlim(0, max(df['Revenue']) * 1.25)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=9)
    ax.tick_params(axis='x', labelsize=8, colors=C_MUTED)
    ax.set_xlabel('Revenue (₦)', fontsize=9, color=C_MUTED)
    ax.grid(axis='x', alpha=0.3, color=C_GRAY)
    
    plt.tight_layout()
    return _save_base64(fig)


def chart_product_qty(product_qty_df, width_in=3.0, height_in=2.2) -> str:
    """Horizontal bar chart — top SKUs by quantity."""
    df = product_qty_df.head(6).copy()
    if df.empty:
        return ""
    
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    
    df = df.iloc[::-1].reset_index(drop=True)
    bars = ax.barh(df['SKU'], df['Quantity'], color=C_ACCENT, height=0.6)
    
    for bar, val in zip(bars, df['Quantity']):
        ax.text(val + max(df['Quantity'])*0.02, bar.get_y() + bar.get_height()/2,
                _qty(val), va='center', ha='left', fontsize=8)
    
    ax.set_xlim(0, max(df['Quantity']) * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=8)
    ax.tick_params(axis='x', labelsize=7, colors=C_MUTED)
    ax.set_xlabel('Carton Packs', fontsize=8, color=C_MUTED)
    ax.grid(axis='x', alpha=0.3, color=C_GRAY)
    
    plt.tight_layout()
    return _save_base64(fig)


def chart_product_value(product_value_df, width_in=3.0, height_in=2.2) -> str:
    """Horizontal bar chart — top SKUs by revenue."""
    df = product_value_df.head(6).copy()
    if df.empty:
        return ""
    
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    
    df = df.iloc[::-1].reset_index(drop=True)
    bars = ax.barh(df['SKU'], df['Revenue'], color=C_RED, height=0.6)
    
    for bar, val in zip(bars, df['Revenue']):
        ax.text(val + max(df['Revenue'])*0.02, bar.get_y() + bar.get_height()/2,
                _naira(val), va='center', ha='left', fontsize=8)
    
    ax.set_xlim(0, max(df['Revenue']) * 1.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=8)
    ax.tick_params(axis='x', labelsize=7, colors=C_MUTED)
    ax.set_xlabel('Revenue (₦)', fontsize=8, color=C_MUTED)
    ax.grid(axis='x', alpha=0.3, color=C_GRAY)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))
    
    plt.tight_layout()
    return _save_base64(fig)


def chart_daily_trend(daily_sales_df, width_in=6.5, height_in=1.8) -> str:
    """Area + line chart — daily revenue trend."""
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""
    
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')
    
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    
    x = df['Date'].values
    y = df['Revenue'].values
    
    ax.fill_between(x, y, alpha=0.15, color=C_NAVY)
    ax.plot(x, y, color=C_NAVY, linewidth=2, marker='o', markersize=4)
    
    # Peak marker
    peak_idx = int(np.argmax(y))
    ax.scatter(x[peak_idx], y[peak_idx], color=C_RED, s=60, zorder=5)
    
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _naira(v)))
    
    n_days = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days + 1
    if n_days <= 14:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=7)
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(C_GRAY)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.tick_params(axis='y', labelsize=7, colors=C_MUTED)
    ax.grid(axis='y', alpha=0.3, color=C_GRAY)
    
    plt.tight_layout()
    return _save_base64(fig)


def chart_reorder(reorder_df, width_in=6.0, height_in=1.7) -> str:
    """Horizontal bar chart — stores by order count."""
    df = reorder_df.copy()
    if df.empty:
        return ""
    
    df = df.head(8).iloc[::-1].reset_index(drop=True)
    
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    
    colors = [C_GREEN if status == 'Repeat Customer' else C_AMBER 
              for status in df['Status']]
    
    bars = ax.barh(df['Store'], df['Order Count'], color=colors, height=0.6)
    
    max_orders = df['Order Count'].max()
    for bar, val in zip(bars, df['Order Count']):
        label = f'{int(val)} order{"s" if val != 1 else ""}'
        ax.text(val + max_orders*0.02, bar.get_y() + bar.get_height()/2,
                label, va='center', ha='left', fontsize=7)
    
    ax.set_xlim(0, max_orders * 1.3)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=6.5)
    ax.tick_params(axis='x', labelsize=6.5, colors=C_MUTED)
    ax.set_xlabel('Number of Orders', fontsize=7, color=C_MUTED)
    ax.grid(axis='x', alpha=0.3, color=C_GRAY)
    
    plt.tight_layout(pad=0.2)
    return _save_base64(fig)


def chart_store_heatmap(store_heatmap_df, width_in=6.2, height_in=1.5) -> str:
    """
    Matplotlib heatmap — top stores (rows) x dates (columns). For PDF page 2.
    """
    import pandas as pd
    import matplotlib.colors as mcolors
    df = store_heatmap_df.copy()
    if df.empty:
        return ""

    df['Date'] = pd.to_datetime(df['Date'])
    pivot = df.pivot_table(index='Store', columns='Date', values='Orders',
                            fill_value=0, aggfunc='sum')
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)
    store_rank = pivot.sum(axis=1).sort_values(ascending=False)
    pivot = pivot.loc[store_rank.head(8).index]

    n_stores = len(pivot)
    n_dates  = len(pivot.columns)
    height_in = max(1.25, min(1.7, n_stores * 0.12 + 0.65))

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    cmap = mcolors.LinearSegmentedColormap.from_list(
        'dala', ['#F4F6FA', '#A8B8D8', C_NAVY]
    )
    ax.imshow(pivot.values, aspect='auto', cmap=cmap,
              vmin=0, vmax=max(pivot.values.max(), 1))

    ax.set_yticks(range(n_stores))
    ax.set_yticklabels(
        [s[:20] + ('…' if len(s) > 20 else '') for s in pivot.index],
        fontsize=5.8, color=C_TEXT
    )

    date_labels = [d.strftime('%d %b') for d in pivot.columns]
    tick_step = max(1, n_dates // 10)
    ax.set_xticks(range(0, n_dates, tick_step))
    ax.set_xticklabels(date_labels[::tick_step], rotation=45, ha='right', fontsize=5.6)
    ax.tick_params(axis='both', length=0)

    for spine in ax.spines.values():
        spine.set_visible(False)

    plt.tight_layout(pad=0.15)
    return _save_base64(fig)


def chart_sparkline(daily_sales_df, width_in=1.5, height_in=0.4) -> str:
    """Mini sparkline for KPI card."""
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""

    df = df.sort_values('Date')

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    x = range(len(df))
    y = df['Revenue'].values

    ax.fill_between(x, y, alpha=0.3, color=C_NAVY)
    ax.plot(x, y, color=C_NAVY, linewidth=1.5)

    ax.axis('off')
    ax.set_ylim(0, max(y) * 1.1)

    plt.tight_layout(pad=0)
    return _save_base64(fig)


def chart_weekly_bars(weekly_pct, color=C_NAVY, width_in=2.0, height_in=0.7) -> str:
    """
    4-bar mini chart showing week-by-week % contribution to monthly total.
    Used on KPI cards to replicate the Power BI 'WoW% Trend' sparkline.
    """
    if not weekly_pct or all(v == 0 for v in weekly_pct):
        return ""

    weeks = ['W1', 'W2', 'W3', 'W4']
    vals  = list(weekly_pct)[:4]

    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.patch.set_facecolor('none')
    ax.set_facecolor('none')

    bar_colors = [C_RED if v == max(vals) else color for v in vals]
    bars = ax.bar(weeks, vals, color=bar_colors, width=0.6, linewidth=0)

    for bar, pct in zip(bars, vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            f'{pct:.0f}%',
            ha='center', va='bottom',
            fontsize=6.5, fontweight='bold',
            color=C_TEXT,
        )

    ax.set_ylim(0, max(vals) * 1.55)
    ax.axis('off')
    plt.tight_layout(pad=0.1)
    return _save_base64(fig)


def chart_dual_trend(daily_sales_df, width_in=6.5, height_in=2.0) -> str:
    """
    Dual-axis trend: filled area = revenue (left axis), line = quantity (right axis).
    Mirrors the Power BI 'Sales Trend' chart at the bottom of the dashboard.
    """
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return ""

    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    fig, ax1 = plt.subplots(figsize=(width_in, height_in))
    ax2 = ax1.twinx()

    x = df['Date'].values
    rev = df['Revenue'].values
    qty = df['Quantity'].values

    # Revenue — filled area
    ax1.fill_between(x, rev, alpha=0.18, color=C_RED)
    ax1.plot(x, rev, color=C_RED, linewidth=1.8, label='Total_Revenue')

    # Quantity — line
    ax2.plot(x, qty, color=C_NAVY, linewidth=1.6, linestyle='--', label='Total_Quantity')

    # Axes formatting
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _naira(v)))
    ax1.tick_params(axis='y', labelsize=7, colors=C_RED)
    ax1.tick_params(axis='x', labelsize=7)
    ax2.tick_params(axis='y', labelsize=7, colors=C_NAVY)

    n_days = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days + 1
    interval = 5 if n_days > 14 else 2
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=interval))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=7)

    for ax in [ax1, ax2]:
        ax.spines['top'].set_visible(False)
    ax1.spines['left'].set_color(C_RED)
    ax2.spines['right'].set_color(C_NAVY)
    ax1.spines['bottom'].set_color(C_GRAY)
    ax1.grid(axis='y', alpha=0.2, color=C_GRAY)

    # Legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc='upper left', fontsize=7, framealpha=0.85, edgecolor=C_GRAY)

    plt.tight_layout(pad=0.5)
    return _save_base64(fig)


def chart_stock_vertical(closing_stock_df, width_in=1.8, height_in=3.0) -> str:
    """
    Vertical bar chart — current stock level by SKU.
    Mirrors the 'Current Stock Level' panel on the right side of the Power BI dashboard.
    """
    df = closing_stock_df.copy()
    if df.empty:
        return ""

    df = df.sort_values('Closing Stock (Cartons)', ascending=False).head(10)

    # Truncate long SKU names for labels
    labels = [s[:18] + '…' if len(s) > 18 else s for s in df['SKU']]
    vals   = df['Closing Stock (Cartons)'].values

    fig, ax = plt.subplots(figsize=(width_in, height_in))

    bar_colors = [C_NAVY] * len(vals)
    bars = ax.bar(range(len(vals)), vals, color=bar_colors, width=0.65, linewidth=0)

    # Value labels on top of bars
    for bar, val in zip(bars, vals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(vals) * 0.01,
            f'{val:.0f}',
            ha='center', va='bottom',
            fontsize=6.5, fontweight='bold', color=C_NAVY,
        )

    ax.set_xticks(range(len(vals)))
    ax.set_xticklabels(labels, rotation=90, fontsize=5.5, color=C_TEXT)
    ax.set_ylim(0, max(vals) * 1.25)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}'))
    ax.tick_params(axis='y', labelsize=6.5, colors=C_MUTED)
    ax.tick_params(axis='x', length=0)

    for sp in ['top', 'right', 'left']:
        ax.spines[sp].set_visible(False)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.grid(axis='y', alpha=0.25, color=C_GRID)

    plt.tight_layout(pad=0.3)
    return _save_base64(fig)
