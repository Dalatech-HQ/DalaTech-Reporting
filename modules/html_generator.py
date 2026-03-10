"""
html_generator.py — Standalone interactive HTML report generator using Plotly.

Each function returns a Plotly figure-div HTML string for embedding in Jinja2 templates.
The template includes Plotly.js once from CDN; all chart divs use include_plotlyjs=False.
"""

import os
import base64
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .kpi import generate_narrative, calculate_perf_score, build_reorder_trend
from .predictor import monthly_growth_outlook
from .gmv import render_gmv_window_svg

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
LOGO_PATH    = os.path.join(BASE_DIR, 'logo.jpeg')

jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml'])
)

# ── Colour palette ─────────────────────────────────────────────────────────────
C_NAVY  = '#1B2B5E'
C_RED   = '#E8192C'
C_BLUE  = '#2E86C1'
C_GREEN = '#1E8449'
C_AMBER = '#C0922A'
C_GRAY  = '#DDE3ED'
C_BG    = '#F4F6FA'
C_WHITE = '#FFFFFF'
C_MUTED = '#7A849E'

PLOTLY_CFG = dict(displayModeBar=True, responsive=True, scrollZoom=True)

LAYOUT_BASE = dict(
    paper_bgcolor=C_WHITE,
    plot_bgcolor=C_WHITE,
    font=dict(family='Segoe UI, Helvetica Neue, Arial', size=11, color='#1A1A2E'),
    hoverlabel=dict(bgcolor=C_NAVY, font_color='white', font_size=12),
    showlegend=False,
    # margin intentionally omitted — set per-chart
)


def _div(fig, div_id=None) -> str:
    """Render a Plotly figure to an HTML div string (no Plotly.js included)."""
    kwargs = dict(full_html=False, include_plotlyjs=False, config=PLOTLY_CFG)
    if div_id:
        kwargs['div_id'] = div_id
    return fig.to_html(**kwargs)


def _naira(v):
    if v >= 1_000_000: return f'₦{v/1_000_000:.1f}M'
    if v >= 1_000:     return f'₦{v/1_000:.1f}K'
    return f'₦{v:,.0f}'


def _infer_report_type(start_date: str, end_date: str, override: str | None = None) -> str:
    report_type = (override or '').strip().lower()
    if report_type in {'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly'}:
        return report_type
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except Exception:
        return 'custom'

    days = (end_dt - start_dt).days + 1
    is_full_year = start_dt.month == 1 and start_dt.day == 1 and end_dt.month == 12 and end_dt.day == 31
    is_quarter = (
        start_dt.day == 1 and
        start_dt.month in (1, 4, 7, 10) and
        end_dt.month == start_dt.month + 2 and
        ((end_dt.month in (3, 12) and end_dt.day == 31) or
         (end_dt.month == 6 and end_dt.day == 30) or
         (end_dt.month == 9 and end_dt.day == 30))
    )
    next_dt = datetime.fromordinal(end_dt.toordinal() + 1)
    is_full_month = start_dt.day == 1 and next_dt.day == 1

    if is_full_year or days in (365, 366):
        return 'yearly'
    if is_quarter or 85 <= days <= 95:
        return 'quarterly'
    if is_full_month or 28 <= days <= 31:
        return 'monthly'
    if days <= 7:
        return 'weekly'
    if days <= 14:
        return 'biweekly'
    return 'custom'


def _build_period_labels(start_date: str, end_date: str,
                         report_type: str | None = None,
                         month_label: str | None = None) -> tuple[str, str]:
    report_type = _infer_report_type(start_date, end_date, report_type)
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if report_type == 'weekly':
        label = month_label or f"Week of {start_dt.strftime('%d %b %Y')}"
        return f'{label} Sales Report', label
    if report_type == 'biweekly':
        label = month_label or f"{start_dt.strftime('%d %b')} - {end_dt.strftime('%d %b %Y')}"
        return f'Biweekly Sales Report', label
    if report_type == 'quarterly':
        label = month_label or f"Q{((start_dt.month - 1) // 3) + 1} {start_dt.year}"
        return f'{label} Quarterly Sales Report', label
    if report_type == 'yearly':
        label = month_label or f'{start_dt.year}'
        return f'{label} Annual Sales Report', label
    if report_type == 'monthly':
        label = month_label or start_dt.strftime('%B %Y')
        return f'{start_dt.strftime("%B")} Monthly Sales Report', label
    label = month_label or f"{start_dt.strftime('%d %b %Y')} - {end_dt.strftime('%d %b %Y')}"
    return 'Sales Report', label


# ═══════════════════════════════════════════════════════════════════════════════
#  PLOTLY CHART BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def plotly_dual_trend(daily_sales_df: pd.DataFrame) -> str:
    """Dual-axis area+line: Revenue (left, red fill) + Quantity (right, navy dashed)."""
    df = daily_sales_df.copy()
    if df.empty or len(df) < 2:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">Insufficient data</p>'

    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')

    # Peak day
    peak_idx = df['Revenue'].idxmax()
    peak_date = df.loc[peak_idx, 'Date']
    peak_rev  = df.loc[peak_idx, 'Revenue']

    fig = go.Figure()

    # Revenue area
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df['Revenue'],
        name='Total Revenue',
        fill='tozeroy', fillcolor='rgba(232,25,44,0.12)',
        line=dict(color=C_RED, width=2.5),
        mode='lines',
        hovertemplate='<b>%{x|%d %b}</b><br>Revenue: ₦%{y:,.0f}<extra></extra>',
        yaxis='y1',
    ))

    # Peak annotation
    fig.add_trace(go.Scatter(
        x=[peak_date], y=[peak_rev],
        mode='markers+text',
        marker=dict(color=C_RED, size=10, symbol='circle'),
        text=[f'Peak: {_naira(peak_rev)}'],
        textposition='top center',
        textfont=dict(size=10, color=C_RED, family='Segoe UI'),
        showlegend=False,
        yaxis='y1',
        hoverinfo='skip',
    ))

    # Quantity line
    fig.add_trace(go.Scatter(
        x=df['Date'], y=df['Quantity'],
        name='Total Quantity',
        line=dict(color=C_NAVY, width=2, dash='dot'),
        mode='lines',
        hovertemplate='<b>%{x|%d %b}</b><br>Qty: %{y:.1f} packs<extra></extra>',
        yaxis='y2',
    ))

    layout = {**LAYOUT_BASE,
        'yaxis': dict(
            title='Revenue (₦)', side='left',
            tickformat=',.0f', tickprefix='₦',
            gridcolor=C_GRAY, gridwidth=0.5,
            showline=True, linecolor=C_RED, linewidth=1.5,
        ),
        'yaxis2': dict(
            title='Quantity (Packs)', side='right', overlaying='y',
            showgrid=False, showline=True, linecolor=C_NAVY, linewidth=1.5,
        ),
        'xaxis': dict(tickformat='%d %b', gridcolor=C_GRAY, gridwidth=0.3),
        'legend': dict(
            orientation='h', yanchor='bottom', y=1.02,
            xanchor='right', x=1,
            bgcolor='rgba(255,255,255,0.85)',
            bordercolor=C_GRAY, borderwidth=1,
        ),
        'showlegend': True,
        'margin': dict(l=60, r=60, t=40, b=40),
        'height': 240,
    }
    fig.update_layout(**layout)
    return _div(fig, 'trend-chart')


def plotly_top_stores(top_stores_df: pd.DataFrame) -> str:
    """Interactive horizontal bar — top stores by revenue."""
    df = top_stores_df.head(8).copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No data</p>'

    df = df.iloc[::-1].reset_index(drop=True)
    colors = [C_RED if i == len(df)-1 else C_NAVY for i in range(len(df))]
    text_labels = [_naira(v) for v in df['Revenue']]

    fig = go.Figure(go.Bar(
        x=df['Revenue'], y=df['Store'],
        orientation='h',
        marker_color=colors,
        text=text_labels,
        textposition='outside',
        textfont=dict(size=10, color=C_NAVY, family='Segoe UI'),
        hovertemplate='<b>%{y}</b><br>Revenue: ₦%{x:,.0f}<extra></extra>',
    ))

    fig.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(
            tickformat=',.0f', tickprefix='₦',
            showgrid=True, gridcolor=C_GRAY, gridwidth=0.5,
            range=[0, df['Revenue'].max() * 1.3],
        ),
        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
        margin=dict(l=10, r=80, t=10, b=30),
        height=max(180, len(df) * 32),
    )
    return _div(fig, 'stores-chart')


def plotly_top_products(product_value_df: pd.DataFrame) -> str:
    """Interactive horizontal bar — top products by revenue."""
    df = product_value_df.head(8).copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No data</p>'

    df = df.iloc[::-1].reset_index(drop=True)
    text_labels = [_naira(v) for v in df['Revenue']]

    fig = go.Figure(go.Bar(
        x=df['Revenue'], y=df['SKU'],
        orientation='h',
        marker_color=C_RED,
        text=text_labels,
        textposition='outside',
        textfont=dict(size=10, color=C_RED, family='Segoe UI'),
        hovertemplate='<b>%{y}</b><br>Revenue: ₦%{x:,.0f}<extra></extra>',
    ))

    fig.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(
            tickformat=',.0f', tickprefix='₦',
            showgrid=True, gridcolor=C_GRAY, gridwidth=0.5,
            range=[0, df['Revenue'].max() * 1.3],
        ),
        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
        margin=dict(l=10, r=80, t=10, b=30),
        height=max(180, len(df) * 32),
    )
    return _div(fig, 'products-chart')


def plotly_store_heatmap(store_heatmap_df: pd.DataFrame) -> str:
    """Interactive Plotly heatmap with daily / weekly / monthly views."""
    df = store_heatmap_df.copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No heatmap data available</p>'

    if 'Date' not in df.columns or 'Store' not in df.columns or 'Orders' not in df.columns:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No heatmap data available</p>'

    if pd.api.types.is_numeric_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'], unit='ms', errors='coerce')
    else:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Store']).copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No heatmap data available</p>'

    store_order = (
        df.groupby('Store')['Orders']
        .sum()
        .sort_values(ascending=False)
    )
    top_stores = store_order.head(8).index.tolist()
    df = df[df['Store'].isin(top_stores)].copy()
    stores = top_stores

    colorscale = [
        [0.0, '#F4F6FA'],
        [0.2, '#D8E0EF'],
        [0.5, '#8EA3CE'],
        [1.0, C_NAVY],
    ]

    def _build_period_view(freq: str, label: str, xfmt: str):
        working = df.copy()
        working['Period'] = working['Date'].dt.to_period(freq).dt.start_time
        pivot = working.pivot_table(
            index='Store',
            columns='Period',
            values='Orders',
            fill_value=0,
            aggfunc='sum',
        )
        pivot = pivot.reindex(stores, fill_value=0)
        pivot = pivot.reindex(sorted(pivot.columns), axis=1)
        if pivot.empty:
            x_vals = []
            z_vals = [[0] for _ in stores]
        else:
            x_vals = [d.strftime(xfmt) for d in pivot.columns]
            z_vals = pivot.values.tolist()
        return {
            'label': label,
            'x': x_vals,
            'y': stores,
            'z': z_vals,
            'customdata': [[label for _ in x_vals] for _ in stores],
        }

    views = [
        _build_period_view('D', 'Daily', '%d %b'),
        _build_period_view('W-MON', 'Weekly', 'Week of %d %b'),
        _build_period_view('M', 'Monthly', '%b %Y'),
    ]

    fig = go.Figure()
    for idx, view in enumerate(views):
        fig.add_trace(go.Heatmap(
            z=view['z'],
            x=view['x'],
            y=view['y'],
            colorscale=colorscale,
            coloraxis='coloraxis',
            hovertemplate='<b>%{y}</b><br>%{x}<br>%{customdata}<br>Orders: %{z}<extra></extra>',
            customdata=view['customdata'],
            visible=(idx == 0),
            xgap=2,
            ygap=2,
        ))

    fig.update_layout(
        **LAYOUT_BASE,
        coloraxis=dict(
            colorscale=colorscale,
            cmin=0,
            colorbar=dict(title='Orders', thickness=12),
        ),
        xaxis=dict(
            side='bottom',
            tickangle=-35,
            tickfont=dict(size=9),
            showgrid=False,
            automargin=True,
        ),
        yaxis=dict(tickfont=dict(size=9), automargin=True),
        margin=dict(l=10, r=20, t=54, b=68),
        height=max(220, len(stores) * 40 + 90),
        updatemenus=[dict(
            type='buttons',
            direction='left',
            x=0,
            y=1.18,
            xanchor='left',
            yanchor='top',
            buttons=[
                dict(
                    label=view['label'],
                    method='update',
                    args=[
                        {'visible': [i == idx for i in range(len(views))]},
                        {'annotations': [dict(
                            text=f"{view['label']} view",
                            x=1,
                            y=1.18,
                            xref='paper',
                            yref='paper',
                            xanchor='right',
                            yanchor='top',
                            showarrow=False,
                            font=dict(size=11, color=C_MUTED),
                        )]},
                    ],
                )
                for idx, view in enumerate(views)
            ],
        )],
        annotations=[dict(
            text='Daily view',
            x=1,
            y=1.18,
            xref='paper',
            yref='paper',
            xanchor='right',
            yanchor='top',
            showarrow=False,
            font=dict(size=11, color=C_MUTED),
        )],
    )
    return _div(fig, 'heatmap-chart')


def plotly_stock_bars(closing_stock_df: pd.DataFrame) -> str:
    """Vertical bar chart — current closing stock per SKU."""
    df = closing_stock_df.copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No inventory data</p>'

    df = df.sort_values('Closing Stock (Cartons)', ascending=False).head(10)
    labels = [s[:20] + '…' if len(s) > 20 else s for s in df['SKU']]

    fig = go.Figure(go.Bar(
        x=labels, y=df['Closing Stock (Cartons)'],
        marker_color=C_NAVY,
        text=[f'{v:.0f}' for v in df['Closing Stock (Cartons)']],
        textposition='outside',
        textfont=dict(size=9, color=C_NAVY),
        hovertemplate='<b>%{x}</b><br>Stock: %{y:.0f} cartons<extra></extra>',
    ))

    fig.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(tickangle=-50, tickfont=dict(size=8), showgrid=False),
        yaxis=dict(showgrid=True, gridcolor=C_GRAY),
        margin=dict(l=10, r=10, t=20, b=80),
        height=220,
    )
    return _div(fig, 'stock-chart')


def plotly_reorder_bars(reorder_df: pd.DataFrame) -> str:
    """Horizontal bar — stores by order count, green=repeat, amber=single."""
    df = reorder_df.copy()
    if df.empty:
        return '<p style="color:#9BA8C0;text-align:center;padding:20px;">No reorder data</p>'

    df = df.head(12).iloc[::-1].reset_index(drop=True)
    colors = [C_GREEN if s == 'Repeat Customer' else C_AMBER for s in df['Status']]
    labels = [f'{int(v)} order{"s" if v != 1 else ""}' for v in df['Order Count']]

    fig = go.Figure(go.Bar(
        x=df['Order Count'], y=df['Store'],
        orientation='h',
        marker_color=colors,
        text=labels,
        textposition='outside',
        textfont=dict(size=9),
        hovertemplate='<b>%{y}</b><br>Orders: %{x}<extra></extra>',
    ))

    fig.update_layout(
        **LAYOUT_BASE,
        xaxis=dict(
            tickformat='d', showgrid=True, gridcolor=C_GRAY,
            range=[0, df['Order Count'].max() * 1.35],
        ),
        yaxis=dict(showgrid=False, tickfont=dict(size=9)),
        margin=dict(l=10, r=80, t=10, b=30),
        height=max(160, len(df) * 30),
    )
    return _div(fig, 'reorder-chart')


# ═══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def render_html_report(brand_name: str, kpis: dict,
                       start_date: str, end_date: str,
                       portfolio_avg_revenue: float = None,
                       total_portfolio_revenue: float = None,
                       report_type: str | None = None,
                       month_label: str | None = None,
                       ai_narrative: str = None,
                       growth_outlook: dict | None = None,
                       gmv_window: dict | None = None) -> str:
    """Render the standalone interactive HTML report and return the HTML string."""
    # ── Plotly chart divs ─────────────────────────────────────────────────────
    chart_trend    = plotly_dual_trend(kpis['daily_sales'])
    chart_stores   = plotly_top_stores(kpis['top_stores'])
    chart_products = plotly_top_products(kpis['product_value'])
    chart_heatmap  = plotly_store_heatmap(kpis['store_heatmap_df'])
    chart_stock    = plotly_stock_bars(kpis['closing_stock'])
    chart_reorder  = plotly_reorder_bars(kpis['reorder_analysis'])

    # ── Scorecard ─────────────────────────────────────────────────────────────
    perf = calculate_perf_score(kpis, portfolio_avg_revenue)
    growth_outlook = growth_outlook or monthly_growth_outlook([])
    gmv_chart_svg = render_gmv_window_svg(gmv_window) if gmv_window else ''
    reorder_trend = kpis.get('reorder_trend') or build_reorder_trend(kpis=kpis)

    # ── Portfolio share ───────────────────────────────────────────────────────
    portfolio_share = None
    if total_portfolio_revenue and total_portfolio_revenue > 0:
        portfolio_share = round(kpis['total_revenue'] / total_portfolio_revenue * 100, 1)

    # ── Table data ────────────────────────────────────────────────────────────
    def _naira_k(v):
        if v >= 1_000_000: return f'₦{v/1_000_000:.1f}M'
        if v >= 1_000:     return f'₦{v/1_000:.1f}K'
        return f'₦{v:,.0f}'

    top_stores_sorted = kpis['top_stores'].sort_values('Revenue', ascending=False) if not kpis['top_stores'].empty else kpis['top_stores']
    top_products_sorted = kpis['product_value'].sort_values('Revenue', ascending=False) if not kpis['product_value'].empty else kpis['product_value']

    top_stores_table = [
        {
            'store': r['Store'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / kpis['total_revenue'] * 100, 1)
                     if kpis['total_revenue'] > 0 else 0,
        }
        for _, r in top_stores_sorted.head(5).iterrows()
    ]

    top_products_table = [
        {
            'sku':   r['SKU'],
            'value': _naira_k(r['Revenue']),
            'pct':   round(r['Revenue'] / kpis['total_revenue'] * 100, 1)
                     if kpis['total_revenue'] > 0 else 0,
        }
        for _, r in top_products_sorted.head(5).iterrows()
    ]

    closing_stock_table = [
        {
            'sku':   r['SKU'],
            'qty':   f"{r['Closing Stock (Cartons)']:.0f}",
            'health': kpis['inv_health_status'],
            'color':  kpis['inv_health_color'],
        }
        for _, r in kpis['closing_stock'].iterrows()
    ]

    pickup_table = [
        {'sku': r['SKU'], 'qty': f"{r['Qty Picked Up']:.0f}",
         'value': _naira_k(r['Value'])}
        for _, r in kpis['pickup_summary'].iterrows()
    ]

    supply_table = [
        {'sku': r['SKU'], 'qty': f"{r['Qty Supplied']:.0f}",
         'value': _naira_k(r['Value'])}
        for _, r in kpis['supply_summary'].iterrows()
    ]

    # Reports always use the deterministic analysis summary.
    narrative = generate_narrative(brand_name, kpis, start_date, end_date)

    # ── Dates ─────────────────────────────────────────────────────────────────
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt   = datetime.strptime(end_date,   '%Y-%m-%d')
    display_start = start_dt.strftime('%d %b %Y')
    display_end   = end_dt.strftime('%d %b %Y')
    report_title, period_badge = _build_period_labels(start_date, end_date, report_type, month_label)

    # ── Logo ──────────────────────────────────────────────────────────────────
    logo_data = ''
    if os.path.exists(LOGO_PATH):
        with open(LOGO_PATH, 'rb') as f:
            logo_data = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"

    # ── Render template ───────────────────────────────────────────────────────
    template     = jinja_env.get_template('report_interactive.html')
    html_content = template.render(
        brand_name=brand_name,
        start_date=display_start,
        end_date=display_end,
        report_title=report_title,
        period_badge=period_badge,
        kpis=kpis,
        narrative=narrative,
        logo_path=logo_data,
        perf=perf,
        portfolio_share=portfolio_share,
        top_stores_table=top_stores_table,
        top_products_table=top_products_table,
        closing_stock_table=closing_stock_table,
        pickup_table=pickup_table,
        supply_table=supply_table,
        chart_trend=chart_trend,
        chart_stores=chart_stores,
        chart_products=chart_products,
        chart_heatmap=chart_heatmap,
        chart_stock=chart_stock,
        chart_reorder=chart_reorder,
        reorder_trend=reorder_trend,
        growth_outlook=growth_outlook,
        gmv_window=gmv_window,
        gmv_chart_svg=gmv_chart_svg,
    )

    return html_content


def generate_html(output_path: str, brand_name: str, kpis: dict,
                  start_date: str, end_date: str,
                  portfolio_avg_revenue: float = None,
                  total_portfolio_revenue: float = None,
                  report_type: str | None = None,
                  month_label: str | None = None,
                  ai_narrative: str = None,
                  growth_outlook: dict | None = None,
                  gmv_window: dict | None = None) -> str:
    """
    Generate a standalone interactive HTML report using Plotly.

    Args:
        output_path:              Absolute path for .html output file
        brand_name:               Brand partner display name
        kpis:                     Dict from calculate_kpis()
        start_date / end_date:    'YYYY-MM-DD'
        portfolio_avg_revenue:    Optional — avg revenue across all brands (for scorecard)
        total_portfolio_revenue:  Optional — sum revenue all brands (for portfolio share %)

    Returns:
        output_path
    """
    html_content = render_html_report(
        brand_name=brand_name,
        kpis=kpis,
        start_date=start_date,
        end_date=end_date,
        portfolio_avg_revenue=portfolio_avg_revenue,
        total_portfolio_revenue=total_portfolio_revenue,
        report_type=report_type,
        month_label=month_label,
        ai_narrative=ai_narrative,
        growth_outlook=growth_outlook,
        gmv_window=gmv_window,
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return output_path
