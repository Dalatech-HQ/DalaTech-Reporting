import unittest

import pandas as pd

from modules.html_generator import render_html_report
from modules.kpi import build_reorder_trend
from modules.pdf_generator_html import render_pdf_report_html


def _empty_df(columns):
    return pd.DataFrame(columns=columns)


def _base_kpis():
    return {
        'total_revenue': 1_250_000,
        'gmv': 1_250_000,
        'total_qty': 125.0,
        'unique_skus': 4,
        'num_stores': 12,
        'avg_revenue_per_store': 104_166.67,
        'top_stores': _empty_df(['Store', 'Revenue']),
        'product_qty': _empty_df(['SKU', 'Quantity']),
        'product_value': _empty_df(['SKU', 'Revenue']),
        'daily_sales': _empty_df(['Date', 'Revenue', 'Quantity']),
        'top_store_name': 'Alpha Mart',
        'top_store_revenue': 200_000,
        'top_store_pct': 16.0,
        'top_sku': 'Classic Mix',
        'top_sku_qty': 55.0,
        'peak_date': None,
        'peak_revenue': 0,
        'peak_qty': 0,
        'reorder_analysis': _empty_df(['Store', 'Order Count', 'First Order', 'Last Order', 'Total Revenue', 'Status']),
        'repeat_stores': 6,
        'single_stores': 6,
        'closing_stock': _empty_df(['SKU', 'Closing Stock (Cartons)']),
        'total_closing_stock': 18.0,
        'pickup_summary': _empty_df(['SKU', 'Qty Picked Up', 'Value']),
        'total_pickup_qty': 0,
        'total_pickup_value': 0,
        'supply_summary': _empty_df(['SKU', 'Qty Supplied', 'Value']),
        'total_supplied_qty': 0,
        'total_supplied_value': 0,
        'weekly_rev_pct': [20, 24, 26, 30],
        'weekly_qty_pct': [22, 23, 25, 30],
        'wow_rev_change': 8.4,
        'wow_qty_change': 5.2,
        'trading_days': 20,
        'repeat_pct': 50.0,
        'inv_health_status': 'Healthy',
        'inv_health_color': 'green',
        'stock_days_cover': 14.0,
        'store_heatmap_df': _empty_df(['Store', 'Date', 'Orders']),
        'sales_df': _empty_df([]),
        'full_df': _empty_df([]),
    }


class ReorderTrendTests(unittest.TestCase):
    def test_build_reorder_trend_marks_improving_series(self):
        trend = build_reorder_trend(history_rows=[
            {
                'month_label': 'Dec 2025',
                'start_date': '2025-12-01',
                'end_date': '2025-12-31',
                'report_type': 'monthly',
                'repeat_stores': 3,
                'single_stores': 9,
                'repeat_pct': 25.0,
            },
            {
                'month_label': 'Jan 2026',
                'start_date': '2026-01-01',
                'end_date': '2026-01-31',
                'report_type': 'monthly',
                'repeat_stores': 5,
                'single_stores': 7,
                'repeat_pct': 41.7,
            },
            {
                'month_label': 'Feb 2026',
                'start_date': '2026-02-01',
                'end_date': '2026-02-28',
                'report_type': 'monthly',
                'repeat_stores': 7,
                'single_stores': 5,
                'repeat_pct': 58.3,
            },
        ])

        self.assertTrue(trend['available'])
        self.assertEqual(trend['status'], 'improving')
        self.assertEqual(trend['status_label'], 'Improving')
        self.assertEqual(trend['delta_display'], '+16.6 pts')
        self.assertEqual(trend['best_label'], 'Feb 26')

    def test_renderers_include_reorder_momentum_section(self):
        kpis = _base_kpis()
        kpis['reorder_trend'] = build_reorder_trend(history_rows=[
            {
                'month_label': 'Jan 2026',
                'start_date': '2026-01-01',
                'end_date': '2026-01-31',
                'report_type': 'monthly',
                'repeat_stores': 4,
                'single_stores': 8,
                'repeat_pct': 33.3,
            },
            {
                'month_label': 'Feb 2026',
                'start_date': '2026-02-01',
                'end_date': '2026-02-28',
                'report_type': 'monthly',
                'repeat_stores': 6,
                'single_stores': 6,
                'repeat_pct': 50.0,
            },
        ], kpis=kpis)

        html_report = render_html_report(
            brand_name='Test Brand',
            kpis=kpis,
            start_date='2026-02-01',
            end_date='2026-02-28',
        )
        pdf_html = render_pdf_report_html(
            brand_name='Test Brand',
            kpis=kpis,
            start_date='2026-02-01',
            end_date='2026-02-28',
        )

        self.assertIn('Reorder Momentum', html_report)
        self.assertIn('Repeat rate across recent periods', html_report)
        self.assertIn('Reorder Momentum', pdf_html)
        self.assertIn('Monthly repeat rate', pdf_html)


if __name__ == '__main__':
    unittest.main()
