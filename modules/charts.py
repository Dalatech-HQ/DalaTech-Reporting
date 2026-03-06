"""
charts.py — Matplotlib chart generators for DALA brand partner reports.

Every function returns raw PNG bytes (or None if there is no data to plot).
The PDF generator embeds these bytes directly via ReportLab's Image flowable.

All charts use the DALA colour palette and a clean, minimal style that mirrors
the Power BI dashboard aesthetic described in Section 10 of the brief.
"""

import io
import matplotlib
matplotlib.use('Agg')          # non-interactive backend — must come before pyplot

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
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

# ── Shared rcParams ───────────────────────────────────────────────────────────
plt.rcParams.update({
    'font.family':        'DejaVu Sans',
    'font.size':          8.5,
    'axes.facecolor':     C_BG,
    'figure.facecolor':   C_BG,
    'axes.spines.top':    False,
    'axes.spines.right':  False,
    'axes.spines.left':   False,
    'axes.edgecolor':     C_GRAY,
    'axes.grid':          True,
    'grid.color':         C_GRID,
    'grid.linewidth':     0.7,
    'grid.alpha':         1.0,
    'text.color':         C_TEXT,
    'axes.labelcolor':    C_MUTED,
    'xtick.color':        C_MUTED,
    'ytick.color':        C_TEXT,
    'xtick.labelsize':    7.5,
    'ytick.labelsize':    8.0,
    'axes.titlesize':     9.5,
    'axes.titleweight':   'bold',
    'axes.titlecolor':    C_TEXT,
    'axes.titlepad':      10,
})

DPI = 150   # render at 150 dpi for crisp PDF embedding


# ── Internal helpers ──────────────────────────────────────────────────────────

def _save(fig) -> bytes:
    """Serialise figure to PNG bytes and close it."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=DPI, bbox_inches='tight',
                facecolor=C_BG, edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _naira(v):
    """Compact Naira label: ₦1.2M, ₦340K, ₦5,200."""
    if v >= 1_000_000:
        return f'\u20a6{v / 1_000_000:.1f}M'
    if v >= 1_000:
        return f'\u20a6{v / 1_000:.0f}K'
    return f'\u20a6{v:,.0f}'


def _qty(v):
    """Compact quantity label, stripping trailing zeros."""
    s = f'{v:.2f}'.rstrip('0').rstrip('.')
    return s


def _bar_label(ax, bars, formatter, color, offset_frac=0.015):
    """Add value labels to the right of each horizontal bar."""
    if not bars:
        return
    max_val = max(b.get_width() for b in bars) or 1
    offset  = max_val * offset_frac
    for bar in bars:
        w = bar.get_width()
        ax.text(
            w + offset,
            bar.get_y() + bar.get_height() / 2,
            formatter(w),
            va='center', ha='left',
            fontsize=7.5, color=color, fontweight='bold',
        )


def _fig_height(n_items, per_item=0.42, min_h=2.4, max_h=4.8):
    return min(max_h, max(min_h, n_items * per_item))


# ═══════════════════════════════════════════════════════════════════════════════
#  CHART FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def chart_top_stores(top_stores_df, width_in=7.0) -> bytes | None:
    """
    Horizontal bar chart — top stores by revenue.
    Bars are DALA navy. Top bar is highlighted with DALA red.
    """
    df = top_stores_df.copy()
    if df.empty:
        return None

    n         = len(df)
    height_in = _fig_height(n)
    fig, ax   = plt.subplots(figsize=(width_in, height_in))

    # Reverse so #1 appears at the top
    df    = df.iloc[::-1].reset_index(drop=True)
    bar_colors = [C_RED if i == n - 1 else C_NAVY for i in range(n)]

    bars = ax.barh(
        df['Store'], df['Revenue'],
        color=bar_colors, height=0.62, zorder=3,
        linewidth=0,
    )

    _bar_label(ax, bars, _naira, C_NAVY)

    ax.set_xlabel('Revenue (₦)', labelpad=6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))
    ax.set_xlim(0, df['Revenue'].max() * 1.28)
    ax.tick_params(axis='y', length=0, pad=6)
    ax.tick_params(axis='x', length=3)
    ax.grid(axis='x', zorder=0)
    ax.grid(axis='y', visible=False)
    ax.spines['bottom'].set_color(C_GRAY)

    fig.tight_layout(pad=0.8)
    return _save(fig)


def chart_product_qty(product_qty_df, width_in=3.4, height_in=None) -> bytes | None:
    """
    Horizontal bar chart — top SKUs by quantity sold (carton packs).
    """
    df = product_qty_df.head(8).copy()
    if df.empty:
        return None

    n         = len(df)
    height_in = height_in or _fig_height(n)
    fig, ax   = plt.subplots(figsize=(width_in, height_in))

    df   = df.iloc[::-1].reset_index(drop=True)
    bars = ax.barh(
        df['SKU'], df['Quantity'],
        color=C_ACCENT, height=0.62, zorder=3, linewidth=0,
    )

    _bar_label(ax, bars, _qty, C_ACCENT)

    ax.set_xlabel('Carton Packs', labelpad=6)
    ax.set_xlim(0, df['Quantity'].max() * 1.3)
    ax.set_title('By Quantity (Cartons)')
    ax.tick_params(axis='y', length=0, pad=4)
    ax.tick_params(axis='x', length=3)
    ax.grid(axis='x', zorder=0)
    ax.grid(axis='y', visible=False)
    ax.spines['bottom'].set_color(C_GRAY)

    fig.tight_layout(pad=0.8)
    return _save(fig)


def chart_product_value(product_value_df, width_in=3.4, height_in=None) -> bytes | None:
    """
    Horizontal bar chart — top SKUs by revenue.
    """
    df = product_value_df.head(8).copy()
    if df.empty:
        return None

    n         = len(df)
    height_in = height_in or _fig_height(n)
    fig, ax   = plt.subplots(figsize=(width_in, height_in))

    df   = df.iloc[::-1].reset_index(drop=True)
    bars = ax.barh(
        df['SKU'], df['Revenue'],
        color=C_RED, height=0.62, zorder=3, linewidth=0,
    )

    _bar_label(ax, bars, _naira, C_RED)

    ax.set_xlabel('Revenue (₦)', labelpad=6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))
    ax.set_xlim(0, df['Revenue'].max() * 1.3)
    ax.set_title('By Revenue (₦)')
    ax.tick_params(axis='y', length=0, pad=4)
    ax.tick_params(axis='x', length=3)
    ax.grid(axis='x', zorder=0)
    ax.grid(axis='y', visible=False)
    ax.spines['bottom'].set_color(C_GRAY)

    fig.tight_layout(pad=0.8)
    return _save(fig)


def chart_product_pair(product_qty_df, product_value_df, width_in=7.0) -> bytes | None:
    """
    Side-by-side horizontal bar charts in a single figure:
    left = by quantity, right = by revenue.
    Ensures both panels have identical y-axis labels and scale together.
    """
    skus_qty = list(product_qty_df.head(8)['SKU'])
    skus_val = list(product_value_df.head(8)['SKU'])

    # Unified SKU list (union, ordered by qty rank)
    seen, skus = set(), []
    for s in skus_qty + skus_val:
        if s not in seen:
            skus.append(s)
            seen.add(s)
    skus = skus[:8]
    n    = len(skus)

    if n == 0:
        return None

    qty_map = dict(zip(product_qty_df['SKU'], product_qty_df['Quantity']))
    val_map = dict(zip(product_value_df['SKU'], product_value_df['Revenue']))

    qtys = [qty_map.get(s, 0) for s in skus]
    vals = [val_map.get(s, 0) for s in skus]

    height_in = _fig_height(n)
    fig, (ax_q, ax_v) = plt.subplots(1, 2, figsize=(width_in, height_in))
    fig.subplots_adjust(wspace=0.55, left=0.22, right=0.96, top=0.90, bottom=0.18)

    skus_rev = list(reversed(skus))
    qtys_rev = list(reversed(qtys))
    vals_rev = list(reversed(vals))

    # ── Left: By Quantity ────────────────────────────────────────────────────
    bars_q = ax_q.barh(skus_rev, qtys_rev, color=C_ACCENT,
                        height=0.62, zorder=3, linewidth=0)
    _bar_label(ax_q, bars_q, _qty, C_ACCENT)
    ax_q.set_xlabel('Carton Packs', labelpad=5)
    ax_q.set_xlim(0, max(qtys_rev, default=1) * 1.3)
    ax_q.set_title('By Quantity (Cartons)', pad=8)
    ax_q.tick_params(axis='y', length=0, pad=4)
    ax_q.tick_params(axis='x', length=3)
    ax_q.grid(axis='x', zorder=0)
    ax_q.grid(axis='y', visible=False)
    ax_q.spines['bottom'].set_color(C_GRAY)

    # ── Right: By Revenue ────────────────────────────────────────────────────
    bars_v = ax_v.barh(skus_rev, vals_rev, color=C_RED,
                        height=0.62, zorder=3, linewidth=0)
    _bar_label(ax_v, bars_v, _naira, C_RED)
    ax_v.set_xlabel('Revenue (₦)', labelpad=5)
    ax_v.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: _naira(x)))
    ax_v.set_xlim(0, max(vals_rev, default=1) * 1.3)
    ax_v.set_title('By Revenue (₦)', pad=8)
    ax_v.tick_params(axis='y', length=0, pad=4)
    ax_v.tick_params(axis='x', length=3)
    # Hide y-tick labels on the right panel (already shown on left)
    ax_v.set_yticklabels([])
    ax_v.grid(axis='x', zorder=0)
    ax_v.grid(axis='y', visible=False)
    ax_v.spines['bottom'].set_color(C_GRAY)

    return _save(fig)


def chart_reorder(reorder_df, width_in=7.0) -> bytes | None:
    """
    Horizontal bar chart — stores ranked by order count.
    Green = repeat customers, amber = single-order stores.
    """
    df = reorder_df.copy()
    if df.empty:
        return None

    df        = df.head(15).iloc[::-1].reset_index(drop=True)
    n         = len(df)
    height_in = _fig_height(n, per_item=0.40, min_h=2.4, max_h=5.0)
    fig, ax   = plt.subplots(figsize=(width_in, height_in))

    bar_colors = [
        C_GREEN if s == 'Repeat Customer' else C_AMBER
        for s in df['Status']
    ]
    bars = ax.barh(
        df['Store'], df['Order Count'],
        color=bar_colors, height=0.62, zorder=3, linewidth=0,
    )

    max_orders = df['Order Count'].max() or 1
    for bar, val, status in zip(bars, df['Order Count'], df['Status']):
        label = f'{int(val)} order{"s" if val != 1 else ""}'
        col   = C_GREEN if status == 'Repeat Customer' else C_AMBER
        ax.text(
            bar.get_width() + max_orders * 0.02,
            bar.get_y() + bar.get_height() / 2,
            label,
            va='center', ha='left',
            fontsize=7.5, color=col, fontweight='bold',
        )

    legend_handles = [
        mpatches.Patch(facecolor=C_GREEN, label='Repeat Customer'),
        mpatches.Patch(facecolor=C_AMBER, label='Single Order — Follow Up'),
    ]
    ax.legend(handles=legend_handles, loc='lower right', fontsize=8,
              framealpha=0.92, edgecolor=C_GRAY, fancybox=False)

    ax.set_xlabel('Number of Distinct Orders', labelpad=6)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_xlim(0, max_orders * 1.35)
    ax.tick_params(axis='y', length=0, pad=6)
    ax.tick_params(axis='x', length=3)
    ax.grid(axis='x', zorder=0)
    ax.grid(axis='y', visible=False)
    ax.spines['bottom'].set_color(C_GRAY)

    fig.tight_layout(pad=0.8)
    return _save(fig)


def chart_daily_trend(daily_sales_df, width_in=7.0, height_in=2.6) -> bytes | None:
    """
    Area + line chart — daily revenue trend over the report period.
    Peak day is annotated with a red dot and label.
    """
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return None

    # Ensure Date is datetime
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    fig, ax = plt.subplots(figsize=(width_in, height_in),
                           constrained_layout=True)

    x = df['Date'].values
    y = df['Revenue'].values

    ax.fill_between(x, y, alpha=0.10, color=C_NAVY, zorder=2)
    ax.plot(x, y, color=C_NAVY, linewidth=2.0, zorder=3,
            marker='o', markersize=3.5,
            markerfacecolor=C_NAVY, markeredgewidth=0)

    # Peak marker
    peak_idx = int(np.argmax(y))
    ax.scatter(
        x[peak_idx], y[peak_idx],
        color=C_RED, zorder=5, s=55, linewidth=0,
    )
    ax.annotate(
        f' {_naira(y[peak_idx])}',
        xy=(x[peak_idx], y[peak_idx]),
        fontsize=7.5, color=C_RED, fontweight='bold',
        va='bottom', ha='left',
    )

    # Axis formatting
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: _naira(v)))

    # Date ticks — fewer labels for readability
    n_days = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days + 1
    if n_days <= 14:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    elif n_days <= 31:
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    else:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right', fontsize=7.5)

    ax.set_ylabel('Daily Revenue (₦)', labelpad=6)
    ax.tick_params(axis='y', length=0)
    ax.tick_params(axis='x', length=3)
    ax.grid(axis='y', zorder=0)
    ax.grid(axis='x', visible=False)
    ax.spines['bottom'].set_color(C_GRAY)
    ax.spines['left'].set_color(C_GRAY)
    ax.spines['left'].set_visible(True)    # keep left spine for trend chart

    fig.tight_layout(pad=0.8)
    return _save(fig)
