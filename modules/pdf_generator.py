"""
pdf_generator.py — ReportLab PDF builder for DALA brand partner reports.

Phase 2: All five chart types (daily trend, top stores, product pair,
         reorder) are embedded as matplotlib PNG images.
         Text tables remain as fallback if a chart has no data.
"""

import io
import os
import pandas as pd

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, Image,
)
from reportlab.platypus.flowables import Flowable

from .kpi    import generate_narrative
from .charts import (
    chart_top_stores, chart_product_pair,
    chart_reorder,    chart_daily_trend,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGO_PATH = os.path.join(BASE_DIR, 'logo.jpeg')

# ── Colour palette ─────────────────────────────────────────────────────────────
NAVY       = colors.HexColor('#1B2B5E')
NAVY_MID   = colors.HexColor('#243770')
DALA_RED   = colors.HexColor('#E8192C')
WHITE      = colors.white
LIGHT_GRAY = colors.HexColor('#F4F6FA')
MID_GRAY   = colors.HexColor('#DDE3ED')
DARK_TEXT  = colors.HexColor('#1A1A2E')
BODY_TEXT  = colors.HexColor('#3D4560')
MUTED      = colors.HexColor('#7A849E')
GREEN      = colors.HexColor('#1E8449')
GREEN_BG   = colors.HexColor('#EAF4EE')
AMBER      = colors.HexColor('#B7770D')
AMBER_BG   = colors.HexColor('#FEF9E7')
ACCENT     = colors.HexColor('#2E86C1')
ACCENT_BG  = colors.HexColor('#EBF5FB')

# ── Page geometry ──────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = A4
MARGIN    = 1.4 * cm
CONTENT_W = PAGE_W - 2 * MARGIN      # ≈ 515 pt


# ── Typography ─────────────────────────────────────────────────────────────────
def _style(name, **kw):
    base = dict(fontName='Helvetica', fontSize=9, leading=13,
                textColor=BODY_TEXT, spaceAfter=2)
    base.update(kw)
    return ParagraphStyle(name, **base)

STYLE_H1        = _style('h1', fontName='Helvetica-Bold', fontSize=16,
                          textColor=WHITE, alignment=TA_CENTER, spaceAfter=0)
STYLE_H3        = _style('h3', fontName='Helvetica-Bold', fontSize=9,
                          textColor=NAVY, spaceAfter=2)
STYLE_BODY      = _style('body', fontSize=9, leading=14, spaceAfter=6)
STYLE_MUTED     = _style('muted', fontSize=8, textColor=MUTED, spaceAfter=2)
STYLE_KPI_VAL   = _style('kpi_val', fontName='Helvetica-Bold', fontSize=18,
                          textColor=NAVY, alignment=TA_CENTER, spaceAfter=0)
STYLE_KPI_LBL   = _style('kpi_lbl', fontSize=7.5, textColor=MUTED,
                          alignment=TA_CENTER, spaceAfter=0)
STYLE_TH        = _style('th', fontName='Helvetica-Bold', fontSize=8.5,
                          textColor=WHITE, alignment=TA_CENTER, spaceAfter=0)
STYLE_TD        = _style('td', fontSize=8.5, textColor=DARK_TEXT,
                          alignment=TA_LEFT, spaceAfter=0)
STYLE_NARRATIVE = _style('narr', fontSize=9.5, leading=16,
                          textColor=DARK_TEXT, spaceAfter=0)
STYLE_SUBHDR    = _style('subhdr', fontName='Helvetica-Bold', fontSize=8,
                          textColor=WHITE, alignment=TA_LEFT, spaceAfter=0)


# ── Formatters ────────────────────────────────────────────────────────────────
def _fc(v):  return f'\u20a6{v:,.0f}'
def _fq(v):  return f'{v:.2f}'.rstrip('0').rstrip('.')
def _fp(v):  return f'{v:.1f}%'
def _fd(v):  return pd.Timestamp(v).strftime('%d %b') if v is not None else '—'


# ── Shared builders ────────────────────────────────────────────────────────────

def _section_header(title, accent=NAVY):
    data = [[Paragraph(title, STYLE_SUBHDR)]]
    tbl  = Table(data, colWidths=[CONTENT_W])
    tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), accent),
        ('LEFTPADDING',   (0,0), (-1,-1), 8),
        ('RIGHTPADDING',  (0,0), (-1,-1), 8),
        ('TOPPADDING',    (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    return tbl


def _table_style(header_bg=NAVY, alt_bg=LIGHT_GRAY, n_rows=0):
    cmds = [
        ('BACKGROUND',    (0,0), (-1,0),  header_bg),
        ('TEXTCOLOR',     (0,0), (-1,0),  WHITE),
        ('FONTNAME',      (0,0), (-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0,0), (-1,0),  8.5),
        ('ALIGN',         (0,0), (-1,0),  'CENTER'),
        ('TOPPADDING',    (0,0), (-1,0),  7),
        ('BOTTOMPADDING', (0,0), (-1,0),  7),
        ('FONTNAME',      (0,1), (-1,-1), 'Helvetica'),
        ('FONTSIZE',      (0,1), (-1,-1), 8.5),
        ('TOPPADDING',    (0,1), (-1,-1), 5),
        ('BOTTOMPADDING', (0,1), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (-1,-1), 8),
        ('RIGHTPADDING',  (0,0), (-1,-1), 8),
        ('GRID',          (0,0), (-1,-1), 0.5, MID_GRAY),
        ('LINEBELOW',     (0,0), (-1,0),  1.0, header_bg),
    ]
    for i in range(2, n_rows + 1, 2):
        cmds.append(('BACKGROUND', (0,i), (-1,i), alt_bg))
    return TableStyle(cmds)


def _chart_image(png_bytes, fig_w_in, fig_h_in, pdf_width=None):
    """
    Wrap matplotlib PNG bytes as a ReportLab Image at the correct PDF dimensions.
    Aspect ratio is preserved from the matplotlib figure size.
    """
    if png_bytes is None:
        return None
    target_w = pdf_width or CONTENT_W
    target_h = target_w * (fig_h_in / fig_w_in)
    return Image(io.BytesIO(png_bytes), width=target_w, height=target_h)


# ═══════════════════════════════════════════════════════════════════════════════
#  SECTION BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def _build_header(brand_name, start_date, end_date):
    start_fmt = pd.Timestamp(start_date).strftime('%d %b %Y')
    end_fmt   = pd.Timestamp(end_date).strftime('%d %b %Y')
    period    = f'{start_fmt}  –  {end_fmt}'

    logo_cell = (
        Image(LOGO_PATH, width=1.8*cm, height=1.8*cm)
        if os.path.exists(LOGO_PATH)
        else Paragraph('DALA', _style('lf', fontName='Helvetica-Bold',
                                       fontSize=16, textColor=WHITE))
    )

    title_para  = Paragraph(brand_name.upper(), STYLE_H1)
    period_para = Paragraph(
        f'Sales Performance Report  |  {period}',
        _style('per', fontSize=8.5, textColor=colors.HexColor('#A8B8D8'),
               alignment=TA_CENTER),
    )
    dala_label  = Paragraph(
        'DALA TECHNOLOGIES',
        _style('dl', fontSize=7, textColor=colors.HexColor('#7A90B8'),
               alignment=TA_RIGHT, fontName='Helvetica-Bold'),
    )

    tbl = Table(
        [[logo_cell, [title_para, Spacer(1,2), period_para], dala_label]],
        colWidths=[2.2*cm, CONTENT_W - 4.4*cm, 2.2*cm],
    )
    tbl.setStyle(TableStyle([
        ('BACKGROUND',   (0,0), (-1,-1), NAVY),
        ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
        ('LEFTPADDING',  (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING',   (0,0), (-1,-1), 12),
        ('BOTTOMPADDING',(0,0), (-1,-1), 12),
    ]))
    return [tbl]


def _build_kpi_cards(kpis):
    cards = [
        ('Total Revenue',             _fc(kpis['total_revenue'])),
        ('Total Qty Sold (Cartons)',   _fq(kpis['total_qty'])),
        ('Unique SKUs',               str(kpis['unique_skus'])),
        ('Stores Reached',            str(kpis['num_stores'])),
        ('Avg Revenue / Store',       _fc(kpis['avg_revenue_per_store'])),
    ]
    card_w = CONTENT_W / len(cards)

    cells = []
    for label, value in cards:
        cell = Table(
            [[Paragraph(value, STYLE_KPI_VAL)],
             [Paragraph(label, STYLE_KPI_LBL)]],
            colWidths=[card_w - 4],
        )
        cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), WHITE),
            ('TOPPADDING',    (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
            ('LEFTPADDING',   (0,0), (-1,-1), 4),
            ('RIGHTPADDING',  (0,0), (-1,-1), 4),
            ('BOX',           (0,0), (-1,-1), 1, MID_GRAY),
        ]))
        cells.append(cell)

    row = Table([cells], colWidths=[card_w]*len(cards))
    row.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), LIGHT_GRAY),
        ('LEFTPADDING',   (0,0), (-1,-1), 2),
        ('RIGHTPADDING',  (0,0), (-1,-1), 2),
        ('TOPPADDING',    (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('BOX',           (0,0), (-1,-1), 1, MID_GRAY),
    ]))
    return [_section_header('KEY PERFORMANCE INDICATORS'), row]


def _build_daily_trend(kpis):
    """Daily revenue trend chart — full width, sits just below KPI cards."""
    df = kpis['daily_sales']
    if df.empty or len(df) < 2:
        return []

    fig_w, fig_h = 7.0, 2.6
    png  = chart_daily_trend(df, width_in=fig_w, height_in=fig_h)
    if png is None:
        return []

    img = _chart_image(png, fig_w, fig_h)
    return [_section_header('DAILY SALES TREND'), img]


def _build_top_stores(kpis):
    """Horizontal bar chart — top 10 stores by revenue."""
    df = kpis['top_stores']

    if df.empty:
        return [_section_header('TOP STORES BY REVENUE'),
                Paragraph('No sales data available.', STYLE_MUTED)]

    n         = len(df)
    fig_h     = max(2.4, min(4.8, n * 0.42))
    fig_w     = 7.0
    png       = chart_top_stores(df, width_in=fig_w)

    if png:
        img = _chart_image(png, fig_w, fig_h)
        return [_section_header('TOP STORES BY REVENUE'), img]

    # ── Fallback: plain table ─────────────────────────────────────────────────
    total_rev = kpis['total_revenue'] or 1
    rows      = [['#', 'Store', 'Revenue (₦)', 'Share']]
    for i, r in df.iterrows():
        rows.append([
            Paragraph(str(i+1), _style('rk', alignment=TA_CENTER, fontSize=8.5)),
            Paragraph(r['Store'], STYLE_TD),
            Paragraph(_fc(r['Revenue']),  _style('rv', alignment=TA_RIGHT, fontSize=8.5)),
            Paragraph(_fp(r['Revenue']/total_rev*100), _style('sh', alignment=TA_RIGHT, fontSize=8.5)),
        ])
    tbl = Table(rows, colWidths=[0.8*cm, CONTENT_W-4.6*cm, 2.6*cm, 1.2*cm])
    tbl.setStyle(_table_style(n_rows=n))
    return [_section_header('TOP STORES BY REVENUE'), tbl]


def _build_product_performance(kpis):
    """Paired horizontal bar charts — SKUs by qty (left) and by revenue (right)."""
    df_qty = kpis['product_qty']
    df_val = kpis['product_value']

    if df_qty.empty and df_val.empty:
        return [_section_header('PRODUCT PERFORMANCE'),
                Paragraph('No product data available.', STYLE_MUTED)]

    n     = max(len(df_qty.head(8)), len(df_val.head(8)))
    fig_h = max(2.4, min(4.8, n * 0.42))
    fig_w = 7.0

    png = chart_product_pair(df_qty, df_val, width_in=fig_w)

    if png:
        img = _chart_image(png, fig_w, fig_h)
        return [_section_header('PRODUCT PERFORMANCE'), img]

    # ── Fallback: side-by-side text tables ────────────────────────────────────
    def _make_tbl(df, val_col, fmt):
        rows = [['SKU', val_col]]
        for _, r in df.head(8).iterrows():
            rows.append([
                Paragraph(r['SKU'], STYLE_TD),
                Paragraph(fmt(r[val_col]),
                          _style('pv', alignment=TA_RIGHT, fontSize=8.5)),
            ])
        hw   = (CONTENT_W - 1*cm) / 2
        t    = Table(rows, colWidths=[hw-2.5*cm, 2.5*cm])
        t.setStyle(_table_style(n_rows=len(rows)-1))
        t.setStyle(TableStyle([('ALIGN',(1,1),(-1,-1),'RIGHT')]))
        return t

    tbl_qty = _make_tbl(df_qty, 'Quantity', _fq)
    tbl_val = _make_tbl(df_val, 'Revenue',  _fc)
    side    = Table([[tbl_qty, Spacer(1*cm,1), tbl_val]],
                    colWidths=[(CONTENT_W-1*cm)/2, 1*cm, (CONTENT_W-1*cm)/2])
    side.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP')]))
    return [_section_header('PRODUCT PERFORMANCE'), side]


def _build_reorder_analysis(kpis):
    """Reorder summary cards + horizontal bar chart of stores by order count."""
    df = kpis['reorder_analysis']

    # ── Summary stat strip ────────────────────────────────────────────────────
    summary = Table([[
        Paragraph(f"Repeat Customers: {kpis['repeat_stores']}",
                  _style('rs', fontName='Helvetica-Bold', fontSize=9, textColor=GREEN)),
        Paragraph(f"Single Order – Follow Up: {kpis['single_stores']}",
                  _style('ss', fontName='Helvetica-Bold', fontSize=9, textColor=AMBER)),
        Paragraph(f"Total Stores: {kpis['repeat_stores'] + kpis['single_stores']}",
                  _style('ts', fontName='Helvetica-Bold', fontSize=9, textColor=NAVY)),
    ]], colWidths=[CONTENT_W/3]*3)
    summary.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (0,-1), GREEN_BG),
        ('BACKGROUND',    (1,0), (1,-1), AMBER_BG),
        ('BACKGROUND',    (2,0), (2,-1), ACCENT_BG),
        ('BOX',           (0,0), (-1,-1), 0.5, MID_GRAY),
        ('INNERGRID',     (0,0), (-1,-1), 0.5, MID_GRAY),
        ('TOPPADDING',    (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING',   (0,0), (-1,-1), 10),
        ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))

    items = [_section_header('REORDER ANALYSIS'), Spacer(1, 2*mm), summary]

    if df.empty:
        return items

    # ── Reorder bar chart ─────────────────────────────────────────────────────
    n     = len(df.head(15))
    fig_h = max(2.4, min(5.0, n * 0.40))
    fig_w = 7.0
    png   = chart_reorder(df, width_in=fig_w)

    if png:
        items.append(Spacer(1, 3*mm))
        items.append(_chart_image(png, fig_w, fig_h))
    else:
        # Fallback table
        rows = [['Store', 'Orders', 'First', 'Last', 'Revenue (₦)', 'Status']]
        for _, r in df.iterrows():
            sc = GREEN if r['Status'] == 'Repeat Customer' else AMBER
            rows.append([
                Paragraph(r['Store'], STYLE_TD),
                Paragraph(str(r['Order Count']), _style('oc', alignment=TA_CENTER, fontSize=8.5)),
                Paragraph(_fd(r['First Order']), _style('fo', alignment=TA_CENTER, fontSize=8.5)),
                Paragraph(_fd(r['Last Order']),  _style('lo', alignment=TA_CENTER, fontSize=8.5)),
                Paragraph(_fc(r['Total Revenue']),_style('rv', alignment=TA_RIGHT, fontSize=8.5)),
                Paragraph(r['Status'], _style('st', fontSize=8, textColor=sc, fontName='Helvetica-Bold')),
            ])
        tbl = Table(rows, colWidths=[CONTENT_W-8.1*cm, 1.2*cm, 1.8*cm, 1.8*cm, 2.3*cm, 3.0*cm])
        tbl.setStyle(_table_style(n_rows=len(rows)-1))
        items.append(Spacer(1, 3*mm))
        items.append(tbl)

    return items


def _build_inventory_section(kpis):
    items = [_section_header('INVENTORY & STOCK MOVEMENT'), Spacer(1, 3*mm)]

    # ── Closing Stock ─────────────────────────────────────────────────────────
    closing = kpis['closing_stock']
    if not closing.empty:
        items.append(Paragraph('Closing Stock Balance', STYLE_H3))
        rows = [['SKU', 'Closing Stock (Cartons)']]
        for _, r in closing.iterrows():
            rows.append([Paragraph(r['SKU'], STYLE_TD),
                         Paragraph(_fq(r['Closing Stock (Cartons)']),
                                   _style('cs', alignment=TA_RIGHT, fontSize=8.5))])
        rows.append([
            Paragraph('TOTAL', _style('tot', fontName='Helvetica-Bold', fontSize=8.5)),
            Paragraph(_fq(kpis['total_closing_stock']),
                      _style('tv', fontName='Helvetica-Bold', alignment=TA_RIGHT, fontSize=8.5)),
        ])
        n   = len(rows) - 2
        tbl = Table(rows, colWidths=[CONTENT_W - 3*cm, 3*cm])
        tbl.setStyle(_table_style(n_rows=n))
        tbl.setStyle(TableStyle([
            ('ALIGN',      (1,1), (1,-1), 'RIGHT'),
            ('FONTNAME',   (0,-1), (-1,-1), 'Helvetica-Bold'),
            ('BACKGROUND', (0,-1), (-1,-1), MID_GRAY),
            ('LINEABOVE',  (0,-1), (-1,-1), 1, NAVY),
        ]))
        items += [tbl, Spacer(1, 4*mm)]

    # ── DALA Pickup ───────────────────────────────────────────────────────────
    pickup = kpis['pickup_summary']
    if not pickup.empty:
        items.append(Paragraph('Inventory Picked Up by DALA', STYLE_H3))
        rows = [['SKU', 'Qty Picked Up', 'Value (₦)']]
        for _, r in pickup.iterrows():
            rows.append([Paragraph(r['SKU'], STYLE_TD),
                         Paragraph(_fq(r['Qty Picked Up']), _style('p', alignment=TA_RIGHT, fontSize=8.5)),
                         Paragraph(_fc(r['Value']),          _style('pv', alignment=TA_RIGHT, fontSize=8.5))])
        rows.append([
            Paragraph('TOTAL', _style('tt', fontName='Helvetica-Bold', fontSize=8.5)),
            Paragraph(_fq(kpis['total_pickup_qty']),
                      _style('tq', fontName='Helvetica-Bold', alignment=TA_RIGHT, fontSize=8.5)),
            Paragraph(_fc(kpis['total_pickup_value']),
                      _style('tpv', fontName='Helvetica-Bold', alignment=TA_RIGHT, fontSize=8.5)),
        ])
        n   = len(rows) - 2
        tbl = Table(rows, colWidths=[CONTENT_W-5*cm, 2*cm, 3*cm])
        tbl.setStyle(_table_style(n_rows=n))
        tbl.setStyle(TableStyle([
            ('ALIGN',      (1,1), (-1,-1), 'RIGHT'),
            ('FONTNAME',   (0,-1), (-1,-1), 'Helvetica-Bold'),
            ('BACKGROUND', (0,-1), (-1,-1), MID_GRAY),
            ('LINEABOVE',  (0,-1), (-1,-1), 1, NAVY),
        ]))
        items += [tbl, Spacer(1, 4*mm)]

    # ── Brand Supply ──────────────────────────────────────────────────────────
    supply = kpis['supply_summary']
    if not supply.empty:
        items.append(Paragraph('Inventory Supplied by Brand', STYLE_H3))
        rows = [['SKU', 'Qty Supplied', 'Value (₦)']]
        for _, r in supply.iterrows():
            rows.append([Paragraph(r['SKU'], STYLE_TD),
                         Paragraph(_fq(r['Qty Supplied']), _style('s', alignment=TA_RIGHT, fontSize=8.5)),
                         Paragraph(_fc(r['Value']),         _style('sv', alignment=TA_RIGHT, fontSize=8.5))])
        rows.append([
            Paragraph('TOTAL', _style('tts', fontName='Helvetica-Bold', fontSize=8.5)),
            Paragraph(_fq(kpis['total_supplied_qty']),
                      _style('tsq', fontName='Helvetica-Bold', alignment=TA_RIGHT, fontSize=8.5)),
            Paragraph(_fc(kpis['total_supplied_value']),
                      _style('tsv', fontName='Helvetica-Bold', alignment=TA_RIGHT, fontSize=8.5)),
        ])
        n   = len(rows) - 2
        tbl = Table(rows, colWidths=[CONTENT_W-5*cm, 2*cm, 3*cm])
        tbl.setStyle(_table_style(n_rows=n))
        tbl.setStyle(TableStyle([
            ('ALIGN',      (1,1), (-1,-1), 'RIGHT'),
            ('FONTNAME',   (0,-1), (-1,-1), 'Helvetica-Bold'),
            ('BACKGROUND', (0,-1), (-1,-1), MID_GRAY),
            ('LINEABOVE',  (0,-1), (-1,-1), 1, NAVY),
        ]))
        items.append(tbl)

    if len(items) == 2:
        items.append(Paragraph('No inventory movement recorded in this period.', STYLE_MUTED))

    return items


def _build_narrative(brand_name, kpis, start_date, end_date):
    text = generate_narrative(brand_name, kpis, start_date, end_date)
    box  = Table([[Paragraph(text, STYLE_NARRATIVE)]], colWidths=[CONTENT_W])
    box.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), ACCENT_BG),
        ('BOX',           (0,0), (-1,-1), 1.5, ACCENT),
        ('LEFTPADDING',   (0,0), (-1,-1), 12),
        ('RIGHTPADDING',  (0,0), (-1,-1), 12),
        ('TOPPADDING',    (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))
    return [_section_header('BUSINESS INSIGHT'), Spacer(1, 2*mm), box]


def _build_sheets_link(sheets_link):
    link  = (f'<link href="{sheets_link}" color="#2E86C1">'
             f'<u>Open raw data in Google Sheets \u2192</u></link>')
    box   = Table([
        [Paragraph('Your data is also available as a live spreadsheet:', STYLE_MUTED),
         Paragraph(link, _style('lnk', fontSize=9, textColor=ACCENT))],
    ], colWidths=[CONTENT_W*0.45, CONTENT_W*0.55])
    box.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), LIGHT_GRAY),
        ('BOX',           (0,0), (-1,-1), 1, MID_GRAY),
        ('LEFTPADDING',   (0,0), (-1,-1), 10),
        ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ('TOPPADDING',    (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    return [_section_header('DATA ACCESS'), Spacer(1, 2*mm), box]


def _build_footer():
    note = (
        'Confidential — prepared by DALA Technologies for this brand partner only. '
        'Data covers transactions recorded in Tally ERP for the selected period.'
    )
    return [
        Spacer(1, 4*mm),
        HRFlowable(width=CONTENT_W, thickness=0.5, color=MID_GRAY),
        Spacer(1, 2*mm),
        Paragraph(note, _style('foot', fontSize=7.5, textColor=MUTED, alignment=TA_CENTER)),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def generate_pdf(output_path, brand_name, kpis, start_date, end_date,
                 sheets_link=None):
    """
    Generate the full brand partner report PDF.

    Args:
        output_path:  absolute path where the PDF should be saved.
        brand_name:   display name of the brand partner.
        kpis:         dict from kpi.calculate_kpis().
        start_date:   'YYYY-MM-DD'
        end_date:     'YYYY-MM-DD'
        sheets_link:  optional Google Sheets URL (Phase 3).

    Returns:
        output_path (str)
    """
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN,  bottomMargin=MARGIN,
        title=f'{brand_name} — DALA Sales Report',
        author='DALA Technologies',
    )

    story = []

    story += _build_header(brand_name, start_date, end_date)
    story.append(Spacer(1, 5*mm))

    story += _build_kpi_cards(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_daily_trend(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_top_stores(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_product_performance(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_reorder_analysis(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_inventory_section(kpis)
    story.append(Spacer(1, 5*mm))

    story += _build_narrative(brand_name, kpis, start_date, end_date)

    if sheets_link:
        story.append(Spacer(1, 5*mm))
        story += _build_sheets_link(sheets_link)

    story += _build_footer()

    doc.build(story)
    return output_path
