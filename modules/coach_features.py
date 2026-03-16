"""
coach_features.py

Deterministic feature builders for the Sales Coach and Retailer Intelligence
surfaces. These helpers work from the bundled historical workbook plus the
existing SQLite datastore so Gemini reasons over structured facts instead of
raw rows.
"""

from __future__ import annotations

import os
import re
from calendar import monthrange
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from .brand_names import canonicalize_brand_name
from .ingestion import load_and_clean
from .predictor import build_brand_forecasts
from .retailer_groups import retailer_group_for_name, retailer_group_choices, retailer_group_definition

BASE_DIR = Path(__file__).resolve().parents[1]
HISTORY_PATH = BASE_DIR / "2024to2026salesreport.xlsx"


def history_available(path: str | os.PathLike[str] | None = None) -> bool:
    return Path(path or HISTORY_PATH).is_file()


@lru_cache(maxsize=4)
def _load_sales_history_cached(path_str: str, mtime: float) -> pd.DataFrame:
    df = load_and_clean(path_str)
    df["Brand Partner Canonical"] = (
        df["Brand Partner"].fillna("").astype(str).map(canonicalize_brand_name)
    )
    df["Retailer Key"] = df["Particulars"].fillna("").astype(str).str.strip()
    df["YearMonth"] = df["Date"].dt.to_period("M")
    return df


def load_sales_history(path: str | os.PathLike[str] | None = None) -> pd.DataFrame:
    target = Path(path or HISTORY_PATH)
    if not target.is_file():
        return pd.DataFrame()
    df = _load_sales_history_cached(str(target), target.stat().st_mtime)
    return df.copy()


def _native(value: Any):
    if hasattr(value, "item"):
        return value.item()
    return value


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _retailer_identity(retailer_name: str, city: str | None = None, state: str | None = None) -> dict[str, str]:
    retailer_name = str(retailer_name or "").strip()
    parts = [part.strip() for part in retailer_name.split(",") if part.strip()]
    chain_name = parts[0] if parts else retailer_name or "Unknown retailer"
    branch_hint = ", ".join(parts[1:]) if len(parts) > 1 else ""
    if branch_hint and branch_hint.upper() == branch_hint:
        branch_hint = branch_hint.title()
    location_bits = [str(city or "").strip(), str(state or "").strip()]
    location_label = ", ".join(bit for bit in location_bits if bit)
    if not location_label:
        location_label = branch_hint or "Location detail pending"
    group = retailer_group_for_name(retailer_name)
    return {
        "chain_name": chain_name,
        "branch_label": branch_hint or chain_name,
        "location_label": location_label,
        "retailer_name": retailer_name or chain_name,
        "group_slug": group["slug"] if group else None,
        "group_name": group["name"] if group else None,
        "is_grouped": bool(group),
    }


def _group_codes(df: pd.DataFrame, group_slug: str) -> list[str]:
    if df.empty or not group_slug:
        return []
    target = retailer_group_definition(group_slug)
    if not target:
        return []
    codes = []
    for retailer_code in sorted(set(df["Retailer Key"].dropna().astype(str))):
        group = retailer_group_for_name(retailer_code)
        if group and group["slug"] == target["slug"]:
            codes.append(retailer_code)
    return codes


def _retailer_health_snapshot(revenue_mom: float | None, repeat_rate: float | None,
                              active_brands: int | None, transactions: int | None) -> dict[str, Any]:
    score = 48.0
    if revenue_mom is not None:
        score += _clamp(float(revenue_mom), -30.0, 30.0) * 0.9
    score += min(float(repeat_rate or 0) * 0.28, 24.0)
    score += min(int(active_brands or 0) * 1.35, 18.0)
    score += min(int(transactions or 0) * 0.25, 10.0)
    score = round(_clamp(score, 5.0, 98.0), 1)
    if score >= 72:
        band = "Strong"
        tone = "green"
    elif score >= 56:
        band = "Steady"
        tone = "blue"
    elif score >= 42:
        band = "Watch"
        tone = "amber"
    else:
        band = "Pressure"
        tone = "red"
    return {"health_score": score, "health_band": band, "health_tone": tone}


def _to_period(month_value: str | None):
    if not month_value:
        return None
    try:
        year, month = map(int, str(month_value).split("-"))
        return pd.Period(year=year, month=month, freq="M")
    except Exception:
        return None


def _latest_available_month(df: pd.DataFrame):
    sales_df = df[df["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return None
    periods = sorted(sales_df["YearMonth"].dropna().unique())
    return periods[-1] if periods else None


def _month_bounds(period: pd.Period) -> tuple[str, str]:
    start = pd.Timestamp(year=period.year, month=period.month, day=1)
    end = pd.Timestamp(year=period.year, month=period.month, day=monthrange(period.year, period.month)[1])
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _report_window(ds, df: pd.DataFrame, report_id: int | None = None,
                   period_type: str = "monthly", month_value: str | None = None):
    report = ds.get_report(report_id) if report_id else None
    reports = ds.get_all_reports()
    if report:
        current_type = str(report.get("report_type") or period_type or "monthly").lower()
        previous = next(
            (
                row for row in reports
                if row.get("report_type") == current_type and row["start_date"] < report["start_date"]
            ),
            None,
        )
        return {
            "period_type": current_type,
            "report": report,
            "label": report.get("month_label") or report["start_date"],
            "start_date": report["start_date"],
            "end_date": report["end_date"],
            "previous_report": previous,
            "previous_start_date": previous.get("start_date") if previous else None,
            "previous_end_date": previous.get("end_date") if previous else None,
        }

    selected_period = _to_period(month_value)
    if selected_period is None:
        selected_period = _latest_available_month(df)
    if selected_period is None:
        return {
            "period_type": "monthly",
            "report": None,
            "label": "",
            "start_date": None,
            "end_date": None,
            "previous_report": None,
            "previous_start_date": None,
            "previous_end_date": None,
        }

    current_start, current_end = _month_bounds(selected_period)
    previous_period = selected_period - 1
    prev_start, prev_end = _month_bounds(previous_period)
    return {
        "period_type": "monthly",
        "report": ds.get_report_by_month(selected_period.year, selected_period.month, "monthly"),
        "label": selected_period.strftime("%b %Y"),
        "start_date": current_start,
        "end_date": current_end,
        "previous_report": ds.get_report_by_month(previous_period.year, previous_period.month, "monthly"),
        "previous_start_date": prev_start,
        "previous_end_date": prev_end,
    }


def _comparison_basis(window: dict[str, Any]) -> dict[str, str | None]:
    period_type = str(window.get("period_type") or "monthly").lower()
    previous_report = window.get("previous_report") or {}
    previous_label = previous_report.get("month_label")
    if not previous_label and previous_report.get("start_date") and previous_report.get("end_date"):
        previous_label = f'{previous_report.get("start_date")} to {previous_report.get("end_date")}'
    if not previous_label and window.get("previous_start_date") and window.get("previous_end_date"):
        previous_label = f'{window.get("previous_start_date")} to {window.get("previous_end_date")}'

    if period_type == "weekly":
        basis = "Compared with the previous saved weekly period"
    else:
        basis = "Compared with the previous saved monthly period"

    if previous_label:
        basis = f"{basis}: {previous_label}"
    else:
        basis = f"{basis}; no earlier comparable period is available."

    return {
        "comparison_basis": basis,
        "comparison_period_label": previous_label,
    }


def _same_period_last_year(window: dict[str, Any]) -> dict[str, str | None]:
    start_date = window.get("start_date")
    end_date = window.get("end_date")
    period_type = str(window.get("period_type") or "monthly").lower()
    if not start_date or not end_date:
        return {
            "yoy_start_date": None,
            "yoy_end_date": None,
            "same_period_last_year_label": None,
        }

    start_ts = pd.Timestamp(start_date) - pd.DateOffset(years=1)
    end_ts = pd.Timestamp(end_date) - pd.DateOffset(years=1)
    if period_type == "monthly":
        label = start_ts.strftime("%b %Y")
    else:
        label = f'{start_ts.strftime("%Y-%m-%d")} to {end_ts.strftime("%Y-%m-%d")}'
    return {
        "yoy_start_date": start_ts.strftime("%Y-%m-%d"),
        "yoy_end_date": end_ts.strftime("%Y-%m-%d"),
        "same_period_last_year_label": label,
    }


def _filter_scope(ds, df: pd.DataFrame, scope_type: str, scope_key: str | None = None,
                  retailer_code: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    scope_type = (scope_type or "portfolio").strip().lower()
    if scope_type == "brand":
        brand_name = ds.analytics_brand_name(scope_key or "")
        return df[df["Brand Partner Canonical"] == brand_name].copy()
    if scope_type == "retailer":
        retailer_code = str(scope_key or "").strip()
        return df[df["Retailer Key"] == retailer_code].copy()
    if scope_type == "retailer_group":
        codes = set(_group_codes(df, str(scope_key or "").strip()))
        if not codes:
            return df.iloc[0:0].copy()
        return df[df["Retailer Key"].isin(codes)].copy()
    if scope_type == "brand_retailer":
        brand_name = ds.analytics_brand_name(scope_key or "")
        retailer_code = str(retailer_code or "").strip()
        scoped = df[df["Brand Partner Canonical"] == brand_name].copy()
        if retailer_code:
            scoped = scoped[scoped["Retailer Key"] == retailer_code].copy()
        return scoped
    return df.copy()


def _between(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if df.empty or not start_date or not end_date:
        return df.iloc[0:0].copy() if df.empty else df.copy()
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return df[(df["Date"] >= start) & (df["Date"] <= end)].copy()


def _money(value: float) -> float:
    return round(float(value or 0), 2)


def _pct_delta(current_value: float, previous_value: float) -> float | None:
    previous_value = float(previous_value or 0)
    current_value = float(current_value or 0)
    if previous_value == 0:
        return None if current_value == 0 else 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _count_delta(current_value: float, previous_value: float) -> float:
    return round(float(current_value or 0) - float(previous_value or 0), 2)


def _portfolio_brand_rank(df: pd.DataFrame, brand_name: str) -> tuple[int | None, int]:
    sales_df = df[df["Vch Type"] == "Sales"].copy()
    if sales_df.empty or not brand_name:
        return None, 0
    ranked = (
        sales_df.groupby("Brand Partner Canonical")["Sales_Value"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    ranked["rank"] = range(1, len(ranked) + 1)
    row = ranked[ranked["Brand Partner Canonical"] == brand_name]
    return (int(row.iloc[0]["rank"]), len(ranked)) if not row.empty else (None, len(ranked))


def _metrics_for_scope_frame(frame: pd.DataFrame, scope_type: str) -> dict[str, Any]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return {
            "revenue": 0.0,
            "quantity": 0.0,
            "transactions": 0,
            "active_days": 0,
            "unique_skus": 0,
            "active_stores": 0,
            "active_brands": 0,
            "repeat_rate": 0.0,
            "repeat_entities": 0,
            "single_entities": 0,
            "avg_revenue_per_entity": 0.0,
            "top_store_name": None,
            "top_store_revenue": 0.0,
            "top_brand_name": None,
            "top_brand_revenue": 0.0,
        }

    revenue = _money(sales_df["Sales_Value"].sum())
    quantity = _money(sales_df["Quantity"].sum())
    transactions = int(len(sales_df))
    active_days = int(sales_df["Date"].dt.date.nunique())
    unique_skus = int(sales_df["SKUs"].nunique())
    active_stores = int(sales_df["Retailer Key"].nunique())
    active_brands = int(sales_df["Brand Partner Canonical"].nunique())

    if scope_type in {"portfolio", "brand"}:
        entity_orders = sales_df.groupby("Retailer Key").size()
        entity_count = active_stores
    elif scope_type in {"retailer", "retailer_group"}:
        entity_orders = sales_df.groupby("Brand Partner Canonical").size()
        entity_count = active_brands
    else:
        entity_orders = pd.Series([transactions], dtype=float)
        entity_count = 1 if transactions else 0

    repeat_entities = int((entity_orders > 1).sum()) if len(entity_orders) else 0
    single_entities = int((entity_orders == 1).sum()) if len(entity_orders) else 0
    repeat_rate = round((repeat_entities / entity_count) * 100, 2) if entity_count else 0.0
    avg_revenue_per_entity = round(revenue / max(entity_count, 1), 2)

    top_store = (
        sales_df.groupby("Retailer Key")["Sales_Value"].sum().sort_values(ascending=False)
        if not sales_df.empty else pd.Series(dtype=float)
    )
    top_brand = (
        sales_df.groupby("Brand Partner Canonical")["Sales_Value"].sum().sort_values(ascending=False)
        if not sales_df.empty else pd.Series(dtype=float)
    )

    return {
        "revenue": revenue,
        "quantity": quantity,
        "transactions": transactions,
        "active_days": active_days,
        "unique_skus": unique_skus,
        "active_stores": active_stores,
        "active_brands": active_brands,
        "repeat_rate": repeat_rate,
        "repeat_entities": repeat_entities,
        "single_entities": single_entities,
        "avg_revenue_per_entity": avg_revenue_per_entity,
        "top_store_name": str(top_store.index[0]) if len(top_store) else None,
        "top_store_revenue": _money(top_store.iloc[0]) if len(top_store) else 0.0,
        "top_brand_name": str(top_brand.index[0]) if len(top_brand) else None,
        "top_brand_revenue": _money(top_brand.iloc[0]) if len(top_brand) else 0.0,
    }


def _monthly_history(scope_df: pd.DataFrame, scope_type: str, limit: int | None = 12) -> list[dict[str, Any]]:
    sales_df = scope_df[scope_df["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    periods = sorted(sales_df["YearMonth"].dropna().unique())
    if limit:
        periods = periods[-limit:]
    history: list[dict[str, Any]] = []
    previous = None
    for period in periods:
        period_frame = sales_df[sales_df["YearMonth"] == period].copy()
        metrics = _metrics_for_scope_frame(period_frame, scope_type)
        row = {
            "month_label": period.strftime("%b %Y"),
            "period_start": f"{period.year:04d}-{period.month:02d}-01",
            "period_end": f"{period.year:04d}-{period.month:02d}-{monthrange(period.year, period.month)[1]:02d}",
            **metrics,
        }
        if previous:
            row["revenue_mom"] = _pct_delta(row["revenue"], previous["revenue"])
            row["repeat_rate_delta"] = _count_delta(row["repeat_rate"], previous["repeat_rate"])
        else:
            row["revenue_mom"] = None
            row["repeat_rate_delta"] = 0.0
        history.append(row)
        previous = row
    return history


def _top_brands(frame: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    grouped = (
        sales_df.groupby("Brand Partner Canonical")
        .agg(
            revenue=("Sales_Value", "sum"),
            quantity=("Quantity", "sum"),
            transactions=("Vch No.", "nunique"),
            active_days=("Date", lambda s: s.dt.date.nunique()),
        )
        .sort_values("revenue", ascending=False)
        .head(limit)
        .reset_index()
    )
    total = float(grouped["revenue"].sum() or 0)
    rows = []
    for _, row in grouped.iterrows():
        rows.append({
            "brand_name": str(row["Brand Partner Canonical"]),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_days": int(row["active_days"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
        })
    return rows


def _top_products(frame: pd.DataFrame, limit: int = 12) -> list[dict[str, Any]]:
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    grouped = (
        sales_df.groupby("SKUs")
        .agg(revenue=("Sales_Value", "sum"), quantity=("Quantity", "sum"), transactions=("Vch No.", "nunique"))
        .sort_values("revenue", ascending=False)
        .head(limit)
        .reset_index()
    )
    return [
        {
            "sku": str(row["SKUs"]),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
        }
        for _, row in grouped.iterrows()
    ]


def _top_retailers_for_brand(current_frame: pd.DataFrame, previous_frame: pd.DataFrame, limit: int = 10):
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty and previous_sales.empty:
        return []
    current_grouped = current_sales.groupby("Retailer Key").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        unique_skus=("SKUs", "nunique"),
    )
    previous_grouped = previous_sales.groupby("Retailer Key").agg(revenue=("Sales_Value", "sum"))
    rows = []
    total = float(current_grouped["revenue"].sum() or 0)
    for retailer_name, row in current_grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        previous_revenue = float(previous_grouped.loc[retailer_name]["revenue"]) if retailer_name in previous_grouped.index else 0.0
        rows.append({
            "retailer_code": str(retailer_name),
            "retailer_name": str(retailer_name),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "unique_skus": int(row["unique_skus"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "revenue_mom": _pct_delta(float(row["revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
        })
    return rows


def _branch_rows_for_group(current_frame: pd.DataFrame, previous_frame: pd.DataFrame, limit: int = 24):
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty:
        return []
    current_grouped = current_sales.groupby("Retailer Key").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_brands=("Brand Partner Canonical", "nunique"),
        unique_skus=("SKUs", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
    )
    previous_grouped = previous_sales.groupby("Retailer Key").agg(revenue=("Sales_Value", "sum"))
    total = float(current_grouped["revenue"].sum() or 0)
    rows = []
    for retailer_code, row in current_grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        previous_revenue = float(previous_grouped.loc[retailer_code]["revenue"]) if retailer_code in previous_grouped.index else 0.0
        identity = _retailer_identity(str(retailer_code))
        retailer_frame = current_sales[current_sales["Retailer Key"] == retailer_code].copy()
        brand_orders = retailer_frame.groupby("Brand Partner Canonical").size()
        repeat_brands = int((brand_orders > 1).sum()) if len(brand_orders) else 0
        repeat_rate = round((repeat_brands / max(int(row["active_brands"]), 1)) * 100, 2)
        rows.append({
            "retailer_code": str(retailer_code),
            "retailer_name": identity["retailer_name"],
            "branch_label": identity["branch_label"],
            "location_label": identity["location_label"],
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_brands": int(row["active_brands"]),
            "unique_skus": int(row["unique_skus"]),
            "active_days": int(row["active_days"]),
            "repeat_rate": repeat_rate,
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "revenue_mom": _pct_delta(float(row["revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
        })
    return rows


def _all_time_branch_rows(ds, frame: pd.DataFrame, limit: int = 40):
    sales_df = frame[frame["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return []
    grouped = sales_df.groupby("Retailer Key").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_brands=("Brand Partner Canonical", "nunique"),
        unique_skus=("SKUs", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
        first_seen=("Date", "min"),
        last_seen=("Date", "max"),
    )
    total = float(grouped["revenue"].sum() or 0)
    profile_map = {row["retailer_code"]: row for row in ds.list_retailer_profiles(limit=2000)}
    rows = []
    for retailer_code, row in grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        profile = profile_map.get(str(retailer_code), {})
        identity = _retailer_identity(
            profile.get("retailer_name") or str(retailer_code),
            city=profile.get("city"),
            state=profile.get("state"),
        )
        rows.append({
            "retailer_code": str(retailer_code),
            "retailer_name": identity["retailer_name"],
            "location_label": identity["location_label"],
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_brands": int(row["active_brands"]),
            "unique_skus": int(row["unique_skus"]),
            "active_days": int(row["active_days"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "first_seen": row["first_seen"].strftime("%Y-%m-%d") if pd.notna(row["first_seen"]) else None,
            "last_seen": row["last_seen"].strftime("%Y-%m-%d") if pd.notna(row["last_seen"]) else None,
        })
    return rows


def _history_summary(scope_df: pd.DataFrame, scope_type: str) -> dict[str, Any]:
    sales_df = scope_df[scope_df["Vch Type"] == "Sales"].copy()
    if sales_df.empty:
        return {
            "first_seen": None,
            "last_seen": None,
            "active_months": 0,
            "metrics": _metrics_for_scope_frame(scope_df.iloc[0:0].copy(), scope_type),
            "monthly_history": [],
            "top_brands": [],
            "top_products": [],
            "cumulative_revenue": 0.0,
            "cumulative_quantity": 0.0,
            "avg_monthly_revenue": 0.0,
            "peak_period_label": None,
            "peak_period_revenue": 0.0,
            "low_period_label": None,
            "low_period_revenue": 0.0,
        }
    first_seen = sales_df["Date"].min()
    last_seen = sales_df["Date"].max()
    monthly_history = _monthly_history(sales_df, scope_type, limit=None)
    peak_period = max(monthly_history, key=lambda row: row.get("revenue") or 0, default={})
    low_period = min(monthly_history, key=lambda row: row.get("revenue") or 0, default={})
    cumulative_revenue = _money(sales_df["Sales_Value"].sum())
    cumulative_quantity = _money(sales_df["Quantity"].sum())
    cumulative_transactions = int(sales_df["Vch No."].nunique())
    cumulative_active_days = int(sales_df["Date"].dt.date.nunique())
    active_branches_total = int(sales_df["Retailer Key"].nunique())
    active_brands_total = int(sales_df["Brand Partner Canonical"].nunique())
    active_skus_total = int(sales_df["SKUs"].nunique())
    avg_repeat_rate = round(sum((row.get("repeat_rate") or 0) for row in monthly_history) / max(len(monthly_history), 1), 2)
    first_period = monthly_history[0] if monthly_history else {}
    last_period = monthly_history[-1] if monthly_history else {}
    history_growth_pct = _pct_delta(last_period.get("revenue") or 0, first_period.get("revenue") or 0) if first_period else None
    return {
        "first_seen": first_seen.strftime("%Y-%m-%d") if pd.notna(first_seen) else None,
        "last_seen": last_seen.strftime("%Y-%m-%d") if pd.notna(last_seen) else None,
        "active_months": int(sales_df["YearMonth"].nunique()),
        "metrics": _metrics_for_scope_frame(sales_df, scope_type),
        "monthly_history": monthly_history,
        "top_brands": _top_brands(sales_df, limit=16),
        "top_products": _top_products(sales_df, limit=16),
        "cumulative_revenue": cumulative_revenue,
        "cumulative_quantity": cumulative_quantity,
        "cumulative_transactions": cumulative_transactions,
        "cumulative_active_days": cumulative_active_days,
        "active_branches_total": active_branches_total,
        "active_brands_total": active_brands_total,
        "active_skus_total": active_skus_total,
        "avg_monthly_revenue": round(cumulative_revenue / max(int(sales_df["YearMonth"].nunique()), 1), 2),
        "avg_repeat_rate": avg_repeat_rate,
        "history_growth_pct": history_growth_pct,
        "peak_period_label": peak_period.get("month_label"),
        "peak_period_revenue": _money(peak_period.get("revenue") or 0),
        "low_period_label": low_period.get("month_label"),
        "low_period_revenue": _money(low_period.get("revenue") or 0),
    }


def _current_history_mix_rows(current_rows: list[dict[str, Any]], history_rows: list[dict[str, Any]], key: str, limit: int = 16):
    current_map = {str(row.get(key)): row for row in current_rows}
    history_map = {str(row.get(key)): row for row in history_rows}
    merged_keys = []
    for row in history_rows:
        merged_keys.append(str(row.get(key)))
    for row in current_rows:
        row_key = str(row.get(key))
        if row_key not in merged_keys:
            merged_keys.append(row_key)

    rows = []
    for row_key in merged_keys[:limit]:
        current = current_map.get(row_key, {})
        history = history_map.get(row_key, {})
        rows.append({
            key: row_key,
            "current_revenue": _money(current.get("revenue") or 0),
            "current_quantity": _money(current.get("quantity") or 0),
            "current_share_pct": round(float(current.get("share_pct") or 0), 2),
            "current_mom": current.get("revenue_mom"),
            "history_revenue": _money(history.get("revenue") or 0),
            "history_quantity": _money(history.get("quantity") or 0),
            "history_share_pct": round(float(history.get("share_pct") or 0), 2),
            "transactions": int((current or history).get("transactions") or 0),
        })
    return rows


def _branch_network_rows(current_rows: list[dict[str, Any]], history_rows: list[dict[str, Any]], limit: int = 24):
    current_map = {str(row.get("retailer_code")): row for row in current_rows}
    rows = []
    for history in history_rows[:limit]:
        current = current_map.get(str(history.get("retailer_code")), {})
        rows.append({
            "retailer_code": history.get("retailer_code"),
            "retailer_name": history.get("retailer_name"),
            "location_label": history.get("location_label"),
            "first_seen": history.get("first_seen"),
            "last_seen": history.get("last_seen"),
            "all_time_revenue": _money(history.get("revenue") or 0),
            "all_time_share_pct": round(float(history.get("share_pct") or 0), 2),
            "all_time_transactions": int(history.get("transactions") or 0),
            "all_time_brands": int(history.get("active_brands") or 0),
            "all_time_skus": int(history.get("unique_skus") or 0),
            "current_revenue": _money(current.get("revenue") or 0),
            "current_share_pct": round(float(current.get("share_pct") or 0), 2),
            "current_repeat_rate": round(float(current.get("repeat_rate") or 0), 2) if current else 0.0,
            "current_transactions": int(current.get("transactions") or 0),
            "revenue_mom": current.get("revenue_mom"),
        })
    return rows


def _retailer_group_story(detail: dict[str, Any]) -> dict[str, Any]:
    history = detail.get("history") or {}
    comparisons = detail.get("comparisons") or {}
    network_rows = detail.get("branch_network_rows") or []
    current_brand_rows = detail.get("brand_rows") or []
    current_product_rows = detail.get("current_top_products") or []
    opportunity_rows = detail.get("opportunity_brands") or []

    declining_branches = [row for row in network_rows if row.get("revenue_mom") is not None and row.get("revenue_mom") <= -10][:6]
    growing_branches = [row for row in network_rows if row.get("revenue_mom") is not None and row.get("revenue_mom") >= 10][:6]
    weak_repeat_branches = [row for row in network_rows if row.get("current_revenue") and (row.get("current_repeat_rate") or 0) < 35][:6]
    declining_brands = [row for row in current_brand_rows if row.get("revenue_mom") is not None and row.get("revenue_mom") < 0][:6]
    growing_brands = [row for row in current_brand_rows if row.get("revenue_mom") is not None and row.get("revenue_mom") > 0][:6]

    risks = []
    if declining_branches:
        risks.append({
            "title": "Branch revenue pressure",
            "detail": f"{len(declining_branches)} branches are down at least 10% against the previous comparable period.",
            "items": [f'{row["retailer_name"]}: {row["revenue_mom"]:+.1f}% current-period revenue' for row in declining_branches[:4]],
        })
    if weak_repeat_branches:
        risks.append({
            "title": "Weak repeat depth in selling branches",
            "detail": f"{len(weak_repeat_branches)} branches sold this period but repeat depth stayed below 35%.",
            "items": [f'{row["retailer_name"]}: {row["current_repeat_rate"]:.1f}% repeat depth' for row in weak_repeat_branches[:4]],
        })
    if (history.get("top_three_share") or 0) >= 65:
        risks.append({
            "title": "Revenue concentration risk",
            "detail": f'The top 3 branches account for {history.get("top_three_share"):.1f}% of all-time revenue.',
            "items": [f'{row["retailer_name"]}: {row["all_time_share_pct"]:.1f}% share' for row in network_rows[:3]],
        })
    if declining_brands:
        risks.append({
            "title": "Brand weakness this period",
            "detail": f"{len(declining_brands)} active brands are down versus the previous comparable period.",
            "items": [f'{row["brand_name"]}: {row["revenue_mom"]:+.1f}% revenue' for row in declining_brands[:4]],
        })

    opportunities = []
    if growing_branches:
        opportunities.append({
            "title": "Branch momentum pockets",
            "detail": f"{len(growing_branches)} branches are accelerating and can support wider assortment or stronger fill rates.",
            "items": [f'{row["retailer_name"]}: {row["revenue_mom"]:+.1f}% current-period revenue' for row in growing_branches[:4]],
        })
    if opportunity_rows:
        opportunities.append({
            "title": "Assortment gaps",
            "detail": f'{len(opportunity_rows)} strong portfolio brands are absent from this chain in the current period.',
            "items": [f'{row["brand_name"]}: portfolio rank {row.get("portfolio_rank") or "N/A"} · peer demand ₦{row.get("revenue") or 0:,.2f}' for row in opportunity_rows[:4]],
        })
    if growing_brands:
        opportunities.append({
            "title": "Brands winning in the chain",
            "detail": f"{len(growing_brands)} brands are growing this period and can be amplified across more branches.",
            "items": [f'{row["brand_name"]}: {row["revenue_mom"]:+.1f}% revenue' for row in growing_brands[:4]],
        })
    if current_product_rows:
        opportunities.append({
            "title": "SKU leaders to protect",
            "detail": "The highest-selling SKUs this period should be protected with stock and visibility.",
            "items": [f'{row["sku"]}: ₦{row["revenue"]:,.2f} · {row["quantity"]:,.1f} packs' for row in current_product_rows[:4]],
        })

    revenue_mom_text = f'{comparisons.get("revenue_mom"):+.1f}%' if comparisons.get("revenue_mom") is not None else "N/A"
    revenue_yoy_text = f'{comparisons.get("revenue_yoy"):+.1f}%' if comparisons.get("revenue_yoy") is not None else "N/A"
    summary_lines = [
        f'All-time revenue is ₦{history.get("cumulative_revenue", 0):,.2f} across {history.get("active_months", 0)} active periods and {history.get("active_branches_total", 0)} branches.',
        f'Current-period revenue is ₦{detail.get("metrics", {}).get("revenue", 0):,.2f}, with {revenue_mom_text} versus the previous comparable period and {revenue_yoy_text} versus the same period last year.',
        f'All-time top branch is {(history.get("top_branch") or {}).get("retailer_name") or "N/A"}, and all-time top brand is {(history.get("top_brand") or {}).get("brand_name") or "N/A"}.',
    ]

    return {
        "risks": risks,
        "opportunities": opportunities,
        "declining_branches": declining_branches,
        "growing_branches": growing_branches,
        "weak_repeat_branches": weak_repeat_branches,
        "declining_brands": declining_brands,
        "growing_brands": growing_brands,
        "summary_lines": summary_lines,
    }


def _activity_for_retailer_group(ds, retailer_codes: list[str], report_id: int | None = None) -> dict[str, Any]:
    totals = {"events": 0, "issues": 0, "visits": 0, "brand_mentions": 0, "active_days": 0, "salespeople": 0}
    visits = []
    issues = []
    seen_days = set()
    seen_salespeople = set()
    for retailer_code in retailer_codes:
        summary = ds.get_retailer_activity_summary(retailer_code, report_id=report_id)
        store = summary.get("store") or {}
        if not totals.get("store") and store:
            totals["store"] = store
        inner_totals = summary.get("totals") or {}
        for key in ("events", "issues", "visits", "brand_mentions"):
            totals[key] += int(inner_totals.get(key) or 0)
        for row in summary.get("visits") or []:
            visits.append(row)
            if row.get("activity_date"):
                seen_days.add(row["activity_date"])
            if row.get("salesman_name"):
                seen_salespeople.add(row["salesman_name"])
        for row in summary.get("issues") or []:
            issues.append(row)
            if row.get("activity_date"):
                seen_days.add(row["activity_date"])
    totals["active_days"] = len(seen_days)
    totals["salespeople"] = len(seen_salespeople)
    return {
        "store": totals.get("store") or {},
        "totals": totals,
        "visit_count": len(visits),
        "issue_count": len(issues),
        "visits": visits[:24],
        "issues": issues[:24],
    }


def _brand_rows_for_retailer(current_frame: pd.DataFrame, previous_frame: pd.DataFrame, limit: int = 12):
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty:
        return []
    current_grouped = current_sales.groupby("Brand Partner Canonical").agg(
        revenue=("Sales_Value", "sum"),
        quantity=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
        unique_skus=("SKUs", "nunique"),
    )
    previous_grouped = previous_sales.groupby("Brand Partner Canonical").agg(revenue=("Sales_Value", "sum"))
    total = float(current_grouped["revenue"].sum() or 0)
    rows = []
    for brand_name, row in current_grouped.sort_values("revenue", ascending=False).head(limit).iterrows():
        previous_revenue = float(previous_grouped.loc[brand_name]["revenue"]) if brand_name in previous_grouped.index else 0.0
        rows.append({
            "brand_name": str(brand_name),
            "revenue": _money(row["revenue"]),
            "quantity": _money(row["quantity"]),
            "transactions": int(row["transactions"]),
            "active_days": int(row["active_days"]),
            "unique_skus": int(row["unique_skus"]),
            "share_pct": round((float(row["revenue"]) / total) * 100, 2) if total else 0.0,
            "revenue_mom": _pct_delta(float(row["revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
            "repeat_ready": bool(row["transactions"] > 1),
        })
    return rows


def _opportunity_brands(ds, global_frame: pd.DataFrame, current_frame: pd.DataFrame, limit: int = 8):
    current_brands = {
        str(value).strip()
        for value in current_frame["Brand Partner Canonical"].dropna().tolist()
        if str(value).strip()
    }
    portfolio_rows = _top_brands(global_frame, limit=50)
    missing = []
    for row in portfolio_rows:
        if row["brand_name"] in current_brands:
            continue
        rank, peer_count = _portfolio_brand_rank(global_frame, row["brand_name"])
        row["portfolio_rank"] = rank
        row["peer_count"] = peer_count
        missing.append(row)
        if len(missing) >= limit:
            break
    return missing


def build_scope_snapshot(ds, scope_type: str, scope_key: str | None = None,
                         report_id: int | None = None, month_value: str | None = None,
                         retailer_code: str | None = None, persist: bool = True) -> dict[str, Any]:
    df = load_sales_history()
    scope_df = _filter_scope(ds, df, scope_type, scope_key=scope_key, retailer_code=retailer_code)
    window = _report_window(ds, scope_df if not scope_df.empty else df, report_id=report_id, month_value=month_value)
    yoy_window = _same_period_last_year(window)

    global_current_frame = _between(df, window["start_date"], window["end_date"])
    current_frame = _between(scope_df, window["start_date"], window["end_date"])
    previous_frame = _between(scope_df, window["previous_start_date"], window["previous_end_date"])
    yoy_frame = _between(scope_df, yoy_window["yoy_start_date"], yoy_window["yoy_end_date"])
    current_metrics = _metrics_for_scope_frame(current_frame, scope_type)
    previous_metrics = _metrics_for_scope_frame(previous_frame, scope_type)
    yoy_metrics = _metrics_for_scope_frame(yoy_frame, scope_type)
    monthly_history = _monthly_history(scope_df, scope_type, limit=12)

    comparisons = {
        "revenue_mom": _pct_delta(current_metrics["revenue"], previous_metrics["revenue"]),
        "quantity_mom": _pct_delta(current_metrics["quantity"], previous_metrics["quantity"]),
        "repeat_rate_delta": _count_delta(current_metrics["repeat_rate"], previous_metrics["repeat_rate"]),
        "active_store_delta": _count_delta(current_metrics["active_stores"], previous_metrics["active_stores"]),
        "active_brand_delta": _count_delta(current_metrics["active_brands"], previous_metrics["active_brands"]),
        "transaction_delta": _count_delta(current_metrics["transactions"], previous_metrics["transactions"]),
        "revenue_yoy": _pct_delta(current_metrics["revenue"], yoy_metrics["revenue"]),
        "quantity_yoy": _pct_delta(current_metrics["quantity"], yoy_metrics["quantity"]),
        "repeat_rate_yoy": _count_delta(current_metrics["repeat_rate"], yoy_metrics["repeat_rate"]),
    }

    activity = {}
    if scope_type == "retailer":
        activity = ds.get_retailer_activity_summary(scope_key, report_id=window["report"]["id"] if window["report"] else None)
    elif scope_type == "retailer_group":
        activity = _activity_for_retailer_group(
            ds,
            _group_codes(df, str(scope_key or "").strip()),
            report_id=window["report"]["id"] if window["report"] else None,
        )
    elif scope_type == "brand":
        activity = ds.get_activity_brand_summary(scope_key, limit=8)

    resolved_scope_key = scope_key or "portfolio"
    if scope_type == "brand_retailer":
        resolved_scope_key = f"{ds.analytics_brand_name(scope_key or '')}::{str(retailer_code or '').strip()}"

    snapshot = {
        "scope_type": scope_type,
        "scope_key": resolved_scope_key,
        "period_type": window["period_type"],
        "period_label": window["label"],
        "period_start": window["start_date"],
        "period_end": window["end_date"],
        "report_id": window["report"]["id"] if window["report"] else None,
        "metrics": current_metrics,
        "previous_metrics": previous_metrics,
        "same_period_last_year_metrics": yoy_metrics,
        "comparisons": comparisons,
        "historical": monthly_history,
        "activity": activity,
        **_comparison_basis(window),
        **yoy_window,
    }

    if scope_type == "retailer":
        snapshot["brand_rows"] = _brand_rows_for_retailer(current_frame, previous_frame, limit=14)
        snapshot["top_products"] = _top_products(current_frame, limit=12)
        snapshot["opportunity_brands"] = _opportunity_brands(ds, global_current_frame, current_frame, limit=8)
        top_profile = activity.get("store") or {}
        ds.upsert_retailer_profile(
            scope_key,
            retailer_name=top_profile.get("retailer_name") or scope_key,
            state=top_profile.get("retailer_state"),
            city=top_profile.get("retailer_city"),
            first_seen=monthly_history[0]["period_start"] if monthly_history else None,
            last_seen=window["end_date"],
            profile={
                "latest_period_label": window["label"],
                "latest_revenue": current_metrics["revenue"],
                "active_brands": current_metrics["active_brands"],
            },
        )
        if persist:
            for row in snapshot["brand_rows"]:
                ds.save_retailer_brand_metrics(
                    scope_key,
                    row["brand_name"],
                    snapshot["period_type"],
                    snapshot["period_start"],
                    snapshot["period_end"],
                    row,
                    report_id=snapshot["report_id"],
                )
    elif scope_type == "retailer_group":
        snapshot["brand_rows"] = _brand_rows_for_retailer(current_frame, previous_frame, limit=18)
        snapshot["top_products"] = _top_products(current_frame, limit=14)
        snapshot["opportunity_brands"] = _opportunity_brands(ds, global_current_frame, current_frame, limit=10)
        snapshot["branch_rows"] = _branch_rows_for_group(current_frame, previous_frame, limit=30)
        snapshot["retailer_codes"] = _group_codes(df, str(scope_key or "").strip())
    elif scope_type == "brand":
        snapshot["retailer_rows"] = _top_retailers_for_brand(current_frame, previous_frame, limit=12)
        try:
            history_rows = list(reversed(ds.get_brand_history(scope_key, limit=18)))
            snapshot["forecast"] = build_brand_forecasts({scope_key: history_rows}).get(ds.analytics_brand_name(scope_key), {})
        except Exception:
            snapshot["forecast"] = {}

    if persist and snapshot["period_start"] and snapshot["period_end"]:
        ds.save_coach_feature_snapshot(
            scope_type=snapshot["scope_type"],
            scope_key=snapshot["scope_key"],
            period_type=snapshot["period_type"],
            period_start=snapshot["period_start"],
            period_end=snapshot["period_end"],
            feature_data=snapshot,
            report_id=snapshot["report_id"],
        )
    return snapshot


def build_retailer_index(ds, report_id: int | None = None, month_value: str | None = None,
                         limit: int | None = None) -> dict[str, Any]:
    df = load_sales_history()
    if df.empty:
        return {"rows": [], "period_label": "", "period_start": None, "period_end": None, "available_months": []}
    window = _report_window(ds, df, report_id=report_id, month_value=month_value)
    current_frame = _between(df, window["start_date"], window["end_date"])
    previous_frame = _between(df, window["previous_start_date"], window["previous_end_date"])
    current_sales = current_frame[current_frame["Vch Type"] == "Sales"].copy()
    previous_sales = previous_frame[previous_frame["Vch Type"] == "Sales"].copy()
    if current_sales.empty:
        return {"rows": [], "period_label": window["label"], "period_start": window["start_date"], "period_end": window["end_date"], "available_months": []}

    current_grouped = current_sales.groupby("Retailer Key").agg(
        total_revenue=("Sales_Value", "sum"),
        total_qty=("Quantity", "sum"),
        transactions=("Vch No.", "nunique"),
        active_brands=("Brand Partner Canonical", "nunique"),
        unique_skus=("SKUs", "nunique"),
        active_days=("Date", lambda s: s.dt.date.nunique()),
    )
    previous_grouped = previous_sales.groupby("Retailer Key").agg(total_revenue=("Sales_Value", "sum"))
    total_portfolio = float(current_grouped["total_revenue"].sum() or 0)
    profile_map = {row["retailer_code"]: row for row in ds.list_retailer_profiles(limit=1000)}
    rows = []
    for retailer_code, row in current_grouped.sort_values("total_revenue", ascending=False).iterrows():
        previous_revenue = float(previous_grouped.loc[retailer_code]["total_revenue"]) if retailer_code in previous_grouped.index else 0.0
        current_store_frame = current_sales[current_sales["Retailer Key"] == retailer_code].copy()
        brand_orders = current_store_frame.groupby("Brand Partner Canonical").size()
        repeat_brands = int((brand_orders > 1).sum()) if len(brand_orders) else 0
        repeat_rate = round((repeat_brands / max(int(row["active_brands"]), 1)) * 100, 2)
        profile = profile_map.get(str(retailer_code), {})
        identity = _retailer_identity(
            profile.get("retailer_name") or str(retailer_code),
            city=profile.get("city"),
            state=profile.get("state"),
        )
        health = _retailer_health_snapshot(
            _pct_delta(float(row["total_revenue"]), previous_revenue),
            repeat_rate,
            int(row["active_brands"]),
            int(row["transactions"]),
        )
        rows.append({
            "retailer_code": str(retailer_code),
            "retailer_name": identity["retailer_name"],
            "chain_name": identity["chain_name"],
            "branch_label": identity["branch_label"],
            "location_label": identity["location_label"],
            "group_slug": identity["group_slug"],
            "group_name": identity["group_name"],
            "is_grouped": identity["is_grouped"],
            "state": profile.get("state"),
            "city": profile.get("city"),
            "total_revenue": _money(row["total_revenue"]),
            "total_qty": _money(row["total_qty"]),
            "transactions": int(row["transactions"]),
            "active_brands": int(row["active_brands"]),
            "unique_skus": int(row["unique_skus"]),
            "active_days": int(row["active_days"]),
            "repeat_rate": repeat_rate,
            "portfolio_share_pct": round((float(row["total_revenue"]) / total_portfolio) * 100, 2) if total_portfolio else 0.0,
            "revenue_mom": _pct_delta(float(row["total_revenue"]), previous_revenue),
            "previous_revenue": _money(previous_revenue),
            **health,
        })
    if limit:
        rows = rows[:limit]
    available_months = [
        {"value": period.strftime("%Y-%m"), "label": period.strftime("%b %Y")}
        for period in sorted(df[df["Vch Type"] == "Sales"]["YearMonth"].dropna().unique())
    ]
    return {
        "rows": rows,
        "period_label": window["label"],
        "period_start": window["start_date"],
        "period_end": window["end_date"],
        "report_id": window["report"]["id"] if window["report"] else None,
        "available_months": available_months,
        "group_choices": retailer_group_choices(),
    }


def build_retailer_group_index(ds, report_id: int | None = None, month_value: str | None = None,
                               limit: int | None = None) -> dict[str, Any]:
    dataset = build_retailer_index(ds, report_id=report_id, month_value=month_value, limit=None)
    rows = dataset.get("rows", [])
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not row.get("group_slug"):
            continue
        bucket = grouped.setdefault(row["group_slug"], {
            "group_slug": row["group_slug"],
            "group_name": row["group_name"],
            "branches": [],
            "total_revenue": 0.0,
            "total_qty": 0.0,
            "transactions": 0,
            "active_days": 0,
            "repeat_rate_values": [],
            "active_brand_values": [],
            "retailer_count": 0,
            "states": set(),
            "cities": set(),
            "revenue_mom_values": [],
        })
        bucket["branches"].append(row)
        bucket["retailer_count"] += 1
        bucket["total_revenue"] += float(row.get("total_revenue") or 0)
        bucket["total_qty"] += float(row.get("total_qty") or 0)
        bucket["transactions"] += int(row.get("transactions") or 0)
        bucket["active_days"] = max(bucket["active_days"], int(row.get("active_days") or 0))
        bucket["repeat_rate_values"].append(float(row.get("repeat_rate") or 0))
        bucket["active_brand_values"].append(int(row.get("active_brands") or 0))
        if row.get("state"):
            bucket["states"].add(row["state"])
        if row.get("city"):
            bucket["cities"].add(row["city"])
        if row.get("revenue_mom") is not None:
            bucket["revenue_mom_values"].append(float(row["revenue_mom"]))
    final_rows = []
    total_group_revenue = sum(bucket["total_revenue"] for bucket in grouped.values()) or 0.0
    for bucket in grouped.values():
        branches = sorted(bucket["branches"], key=lambda item: item.get("total_revenue") or 0, reverse=True)
        avg_repeat = round(sum(bucket["repeat_rate_values"]) / max(len(bucket["repeat_rate_values"]), 1), 2)
        avg_active_brands = round(sum(bucket["active_brand_values"]) / max(len(bucket["active_brand_values"]), 1), 1)
        revenue_mom = round(sum(bucket["revenue_mom_values"]) / len(bucket["revenue_mom_values"]), 2) if bucket["revenue_mom_values"] else None
        final_rows.append({
            "group_slug": bucket["group_slug"],
            "group_name": bucket["group_name"],
            "retailer_count": bucket["retailer_count"],
            "total_revenue": _money(bucket["total_revenue"]),
            "total_qty": _money(bucket["total_qty"]),
            "transactions": bucket["transactions"],
            "active_days": bucket["active_days"],
            "repeat_rate": avg_repeat,
            "active_brands_avg": avg_active_brands,
            "revenue_mom": revenue_mom,
            "portfolio_share_pct": round((bucket["total_revenue"] / total_group_revenue) * 100, 2) if total_group_revenue else 0.0,
            "top_branch": branches[0] if branches else None,
            "branches": branches,
            "state_count": len(bucket["states"]),
            "city_count": len(bucket["cities"]),
        })
    final_rows.sort(key=lambda item: item.get("total_revenue") or 0, reverse=True)
    if limit:
        final_rows = final_rows[:limit]
    return {
        **dataset,
        "rows": final_rows,
    }


def build_retailer_detail(ds, retailer_code: str, report_id: int | None = None,
                          month_value: str | None = None) -> dict[str, Any]:
    snapshot = build_scope_snapshot(ds, "retailer", scope_key=retailer_code, report_id=report_id, month_value=month_value, persist=True)
    history = _history_summary(_filter_scope(ds, load_sales_history(), "retailer", scope_key=retailer_code), "retailer")
    profile = ds.get_retailer_profile(retailer_code) or {}
    activity_store = (snapshot.get("activity") or {}).get("store") or {}
    identity = _retailer_identity(
        profile.get("retailer_name") or activity_store.get("retailer_name") or retailer_code,
        city=profile.get("city") or activity_store.get("retailer_city"),
        state=profile.get("state") or activity_store.get("retailer_state"),
    )
    health = _retailer_health_snapshot(
        (snapshot.get("comparisons") or {}).get("revenue_mom"),
        (snapshot.get("metrics") or {}).get("repeat_rate"),
        (snapshot.get("metrics") or {}).get("active_brands"),
        (snapshot.get("metrics") or {}).get("transactions"),
    )
    activity = snapshot.get("activity") or {}
    return {
        "retailer_code": retailer_code,
        "retailer_name": identity["retailer_name"],
        "chain_name": identity["chain_name"],
        "branch_label": identity["branch_label"],
        "location_label": identity["location_label"],
        "activity_available": bool((activity.get("visits") or []) or (activity.get("issues") or [])),
        "profile": profile,
        "history": history,
        **health,
        **snapshot,
    }


def build_retailer_group_detail(ds, group_slug: str, report_id: int | None = None,
                                month_value: str | None = None) -> dict[str, Any]:
    group = retailer_group_definition(group_slug)
    if not group:
        return {}
    snapshot = build_scope_snapshot(ds, "retailer_group", scope_key=group_slug, report_id=report_id, month_value=month_value, persist=True)
    if not snapshot.get("period_start"):
        return {}
    df = load_sales_history()
    scope_df = _filter_scope(ds, df, "retailer_group", scope_key=group_slug)
    branch_rows = snapshot.get("branch_rows") or []
    top_branch = branch_rows[0] if branch_rows else None
    state_values = sorted({row.get("state") for row in branch_rows if row.get("state")})
    city_values = sorted({row.get("city") for row in branch_rows if row.get("city")})
    location_bits = []
    if city_values:
        location_bits.append(f"{len(city_values)} cities")
    if state_values:
        location_bits.append(f"{len(state_values)} states")
    location_label = " · ".join(location_bits) or "Branch location detail pending"
    health = _retailer_health_snapshot(
        (snapshot.get("comparisons") or {}).get("revenue_mom"),
        (snapshot.get("metrics") or {}).get("repeat_rate"),
        (snapshot.get("metrics") or {}).get("active_brands"),
        (snapshot.get("metrics") or {}).get("transactions"),
    )
    history = _history_summary(scope_df, "retailer_group")
    history_branch_rows = _all_time_branch_rows(ds, scope_df, limit=40)
    history["branch_rows"] = history_branch_rows
    history["top_branch"] = history_branch_rows[0] if history_branch_rows else None
    history["branch_count"] = len(history_branch_rows)
    history["top_brand"] = history["top_brands"][0] if history["top_brands"] else None
    history["top_product"] = history["top_products"][0] if history["top_products"] else None
    history["top_three_share"] = round(sum((row.get("share_pct") or 0) for row in history_branch_rows[:3]), 2)
    history["top_five_share"] = round(sum((row.get("share_pct") or 0) for row in history_branch_rows[:5]), 2)
    current_top_products = snapshot.get("top_products") or []
    branch_network_rows = _branch_network_rows(branch_rows, history_branch_rows, limit=40)
    brand_mix_rows = _current_history_mix_rows(snapshot.get("brand_rows") or [], history.get("top_brands") or [], "brand_name", limit=20)
    sku_mix_rows = _current_history_mix_rows(current_top_products, history.get("top_products") or [], "sku", limit=20)
    executive_story = _retailer_group_story({
        **snapshot,
        "history": history,
        "branch_network_rows": branch_network_rows,
        "current_top_products": current_top_products,
    })
    return {
        "retailer_code": group_slug,
        "retailer_name": group["name"],
        "group_slug": group_slug,
        "group_name": group["name"],
        "location_label": location_label,
        "branch_label": f"{len(branch_rows)} branches",
        "branch_rows": branch_rows,
        "top_branch": top_branch,
        "state_values": state_values,
        "city_values": city_values,
        "activity_available": bool((snapshot.get("activity") or {}).get("visits") or (snapshot.get("activity") or {}).get("issues")),
        "profile": {
            "retailer_name": group["name"],
            "first_seen": history.get("first_seen"),
            "last_seen": history.get("last_seen"),
        },
        "current_period_label": snapshot.get("period_label"),
        "history": history,
        "current_top_products": current_top_products,
        "branch_network_rows": branch_network_rows,
        "brand_mix_rows": brand_mix_rows,
        "sku_mix_rows": sku_mix_rows,
        "executive_story": executive_story,
        **health,
        **snapshot,
    }


def build_brand_coach_data(ds, brand_name: str, report_id: int | None = None,
                           month_value: str | None = None) -> dict[str, Any]:
    snapshot = build_scope_snapshot(ds, "brand", scope_key=brand_name, report_id=report_id, month_value=month_value, persist=True)
    retailer_rows = snapshot.get("retailer_rows", [])
    at_risk = [row for row in retailer_rows if (row.get("revenue_mom") or 0) <= -10][:5]
    growth = [row for row in retailer_rows if (row.get("revenue_mom") or 0) >= 10][:5]
    activity = snapshot.get("activity") or {}
    stores_seen = activity.get("stores_seen") or []
    current_retailers = {row.get("retailer_name") for row in retailer_rows}
    activity_mismatches = [
        {
            "retailer_code": row.get("retailer_code"),
            "retailer_name": row.get("retailer_name"),
            "mentions": row.get("mentions"),
        }
        for row in stores_seen
        if row.get("retailer_name") not in current_retailers
    ][:5]
    return {
        "snapshot": snapshot,
        "top_risks": at_risk,
        "top_opportunities": growth,
        "activity_mismatches": activity_mismatches,
    }
