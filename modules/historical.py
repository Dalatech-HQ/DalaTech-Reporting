"""
historical.py — Historical trend analysis and MoM calculations for DALA Analytics.

Provides month-over-month growth metrics, trend detection, and forecasting
based on the full historical dataset (May 2024 → Present).
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


def calculate_mom_growth(current_value: float, previous_value: float) -> float:
    """Calculate Month-over-Month percentage growth."""
    if previous_value == 0 or pd.isna(previous_value):
        return 0.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def get_monthly_metrics(df: pd.DataFrame, year: int, month: int) -> Dict:
    """
    Calculate all metrics for a specific month.
    
    Returns:
        Dict with revenue, quantity, store count, repeat rate, etc.
    """
    # Filter to specific month
    mask = (df['Date'].dt.year == year) & (df['Date'].dt.month == month)
    month_df = df[mask].copy()
    
    if month_df.empty:
        return None
    
    # Sales data only
    sales_df = month_df[month_df['Vch Type'] == 'Sales']
    
    if sales_df.empty:
        return None
    
    # Calculate metrics
    total_revenue = sales_df['Sales_Value'].sum()
    total_qty = sales_df['Quantity'].sum()
    unique_stores = sales_df['Particulars'].nunique()
    unique_skus = sales_df['SKUs'].nunique()
    
    # Repeat purchase rate
    store_orders = sales_df.groupby('Particulars').size()
    repeat_stores = (store_orders > 1).sum()
    repeat_rate = round((repeat_stores / unique_stores * 100), 2) if unique_stores > 0 else 0
    
    # Top store
    top_store = sales_df.groupby('Particulars')['Sales_Value'].sum().sort_values(ascending=False)
    top_store_name = top_store.index[0] if len(top_store) > 0 else None
    top_store_revenue = top_store.iloc[0] if len(top_store) > 0 else 0
    
    return {
        'year': year,
        'month': month,
        'month_label': datetime(year, month, 1).strftime('%b %Y'),
        'total_revenue': round(total_revenue, 2),
        'total_qty': round(total_qty, 2),
        'unique_stores': unique_stores,
        'unique_skus': unique_skus,
        'repeat_stores': repeat_stores,
        'repeat_rate': repeat_rate,
        'top_store_name': top_store_name,
        'top_store_revenue': round(top_store_revenue, 2),
        'avg_revenue_per_store': round(total_revenue / unique_stores, 2) if unique_stores > 0 else 0,
    }


def get_brand_monthly_history(df: pd.DataFrame, brand_name: str) -> List[Dict]:
    """
    Get monthly metrics for a specific brand across all time.
    
    Returns:
        List of monthly metric dicts with MoM growth calculated.
    """
    brand_df = df[df['Brand Partner'] == brand_name].copy()
    
    if brand_df.empty:
        return []
    
    # Get all unique year-months
    brand_df['YearMonth'] = brand_df['Date'].dt.to_period('M')
    year_months = sorted(brand_df['YearMonth'].unique())
    
    history = []
    prev_metrics = None
    
    for ym in year_months:
        metrics = get_monthly_metrics(brand_df, ym.year, ym.month)
        if metrics:
            # Calculate MoM growth
            if prev_metrics:
                metrics['revenue_mom'] = calculate_mom_growth(
                    metrics['total_revenue'], prev_metrics['total_revenue']
                )
                metrics['stores_mom'] = calculate_mom_growth(
                    metrics['unique_stores'], prev_metrics['unique_stores']
                )
                metrics['qty_mom'] = calculate_mom_growth(
                    metrics['total_qty'], prev_metrics['total_qty']
                )
            else:
                metrics['revenue_mom'] = 0
                metrics['stores_mom'] = 0
                metrics['qty_mom'] = 0
            
            history.append(metrics)
            prev_metrics = metrics
    
    return history


def get_portfolio_monthly_trend(df: pd.DataFrame) -> List[Dict]:
    """
    Get aggregated monthly metrics for entire portfolio.
    """
    df = df.copy()
    df['YearMonth'] = df['Date'].dt.to_period('M')
    year_months = sorted(df['YearMonth'].unique())
    
    history = []
    prev_metrics = None
    
    for ym in year_months:
        metrics = get_monthly_metrics(df, ym.year, ym.month)
        if metrics:
            # Count active brands this month
            month_df = df[(df['Date'].dt.year == ym.year) & (df['Date'].dt.month == ym.month)]
            sales_df = month_df[month_df['Vch Type'] == 'Sales']
            metrics['active_brands'] = sales_df['Brand Partner'].nunique()
            
            # MoM calculations
            if prev_metrics:
                metrics['revenue_mom'] = calculate_mom_growth(
                    metrics['total_revenue'], prev_metrics['total_revenue']
                )
                metrics['stores_mom'] = calculate_mom_growth(
                    metrics['unique_stores'], prev_metrics['unique_stores']
                )
            else:
                metrics['revenue_mom'] = 0
                metrics['stores_mom'] = 0
            
            history.append(metrics)
            prev_metrics = metrics
    
    return history


def get_store_repeat_analysis(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    Analyze repeat purchase behavior for each store in a given month.
    
    Returns:
        DataFrame with store-level metrics including repeat frequency.
    """
    mask = (df['Date'].dt.year == year) & (df['Date'].dt.month == month)
    month_df = df[mask]
    sales_df = month_df[month_df['Vch Type'] == 'Sales']
    
    if sales_df.empty:
        return pd.DataFrame()
    
    store_stats = sales_df.groupby('Particulars').agg({
        'Sales_Value': 'sum',
        'Quantity': 'sum',
        'Date': 'count',  # Number of transactions
        'SKUs': 'nunique',
    }).reset_index()
    
    store_stats.columns = ['store_name', 'total_revenue', 'total_qty', 'visit_count', 'unique_skus']
    
    # Calculate repeat category
    store_stats['repeat_category'] = store_stats['visit_count'].apply(
        lambda x: 'Frequent (5+)' if x >= 5 else 'Regular (2-4)' if x >= 2 else 'One-time'
    )
    
    # Calculate repeat percentage
    total_visits = store_stats['visit_count'].sum()
    store_stats['visit_percentage'] = (store_stats['visit_count'] / total_visits * 100).round(2)
    
    return store_stats.sort_values('total_revenue', ascending=False)


def get_repeat_purchase_map_data(df: pd.DataFrame, year: int, month: int, top_n: int = 20) -> List[Dict]:
    """
    Get data for map visualization of top repeat purchase stores.
    
    Returns:
        List of store dicts with repeat metrics (ready for geocoding).
    """
    store_stats = get_store_repeat_analysis(df, year, month)
    
    if store_stats.empty:
        return []
    
    # Get top N by revenue with repeat purchases
    top_stores = store_stats[store_stats['visit_count'] > 1].head(top_n)
    
    map_data = []
    for _, row in top_stores.iterrows():
        map_data.append({
            'store_name': row['store_name'],
            'total_revenue': row['total_revenue'],
            'visit_count': int(row['visit_count']),
            'repeat_category': row['repeat_category'],
            'visit_percentage': row['visit_percentage'],
            # Location will be filled by geocoding
            'latitude': None,
            'longitude': None,
        })
    
    return map_data


def calculate_growth_trend(history: List[Dict]) -> str:
    """
    Determine if brand is accelerating, decelerating, or stable.
    
    Returns: 'accelerating', 'decelerating', 'stable', or 'insufficient_data'
    """
    if len(history) < 3:
        return 'insufficient_data'
    
    # Get last 3 months of MoM growth
    recent_mom = [h['revenue_mom'] for h in history[-3:]]
    
    # Calculate trend
    if recent_mom[-1] > recent_mom[0] + 5:
        return 'accelerating'
    elif recent_mom[-1] < recent_mom[0] - 5:
        return 'decelerating'
    else:
        return 'stable'


def generate_insights(history: List[Dict]) -> Dict:
    """
    Generate "What is working / not working / what next" insights.
    
    Returns:
        Dict with structured insights for PDF reports.
    """
    if len(history) < 2:
        return {
            'working': ['Insufficient historical data for trend analysis.'],
            'not_working': [],
            'next_steps': ['Continue building sales history for better insights.']
        }
    
    current = history[-1]
    previous = history[-2]
    
    insights = {
        'working': [],
        'not_working': [],
        'next_steps': []
    }
    
    # Analyze revenue trend
    if current['revenue_mom'] > 10:
        insights['working'].append(f"Strong revenue growth (+{current['revenue_mom']}% MoM)")
    elif current['revenue_mom'] > 0:
        insights['working'].append(f"Steady revenue growth (+{current['revenue_mom']}% MoM)")
    elif current['revenue_mom'] < -10:
        insights['not_working'].append(f"Significant revenue decline ({current['revenue_mom']}% MoM)")
    
    # Analyze store growth
    if current['stores_mom'] > 20:
        insights['working'].append(f"Rapid store expansion (+{current['stores_mom']}% new stores)")
    elif current['stores_mom'] < 0:
        insights['not_working'].append(f"Store count declining ({current['stores_mom']}% MoM)")
    
    # Analyze repeat rate
    if current['repeat_rate'] > 60:
        insights['working'].append(f"Strong customer loyalty ({current['repeat_rate']}% repeat purchase rate)")
    elif current['repeat_rate'] < 30:
        insights['not_working'].append(f"Low repeat purchase rate ({current['repeat_rate']}% - focus on retention)")
    
    # Generate next steps
    if current['repeat_rate'] < 40:
        insights['next_steps'].append("Implement loyalty program to improve repeat purchase rate")
    
    if current['unique_stores'] < 20:
        insights['next_steps'].append("Focus on store acquisition and geographic expansion")
    
    trend = calculate_growth_trend(history)
    if trend == 'decelerating':
        insights['next_steps'].append("Review pricing strategy and promotional activities to reaccelerate growth")
    
    if not insights['working']:
        insights['working'].append("Sales activity ongoing - continue building market presence")
    
    if not insights['next_steps']:
        insights['next_steps'].append("Maintain current strategy while exploring new store partnerships")
    
    return insights


def get_color_scheme_for_month(year: int, month: int) -> Dict:
    """
    Get the color scheme for a specific month (rotates monthly).
    """
    # 12-month color rotation
    colors = [
        {'primary': '#E8192C', 'secondary': '#FF4D5E', 'light': '#FFE5E7', 'name': 'Crimson'},   # Jan
        {'primary': '#1B2B5E', 'secondary': '#3D5A94', 'light': '#E8EBF3', 'name': 'Navy'},      # Feb
        {'primary': '#1E8449', 'secondary': '#28B463', 'light': '#E9F7EF', 'name': 'Forest'},    # Mar
        {'primary': '#C0922A', 'secondary': '#D4AC54', 'light': '#FCF5E5', 'name': 'Amber'},     # Apr
        {'primary': '#8E44AD', 'secondary': '#AF7AC5', 'light': '#F5EEF8', 'name': 'Purple'},    # May
        {'primary': '#E74C3C', 'secondary': '#EC7063', 'light': '#FDEDEC', 'name': 'Red'},       # Jun
        {'primary': '#16A085', 'secondary': '#1ABC9C', 'light': '#E8F8F5', 'name': 'Teal'},      # Jul
        {'primary': '#D35400', 'secondary': '#E67E22', 'light': '#FEF5E7', 'name': 'Orange'},    # Aug
        {'primary': '#2C3E50', 'secondary': '#5D6D7E', 'light': '#EBF5FB', 'name': 'Midnight'},  # Sep
        {'primary': '#27AE60', 'secondary': '#52BE80', 'light': '#EAFAF1', 'name': 'Green'},     # Oct
        {'primary': '#F39C12', 'secondary': '#F5B041', 'light': '#FEF9E7', 'name': 'Gold'},      # Nov
        {'primary': '#C0392B', 'secondary': '#CD6155', 'light': '#FADBD8', 'name': 'Berry'},     # Dec
    ]
    
    return colors[month - 1]
