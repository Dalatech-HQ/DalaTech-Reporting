from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .io_utils import MASTER_COLUMNS, coerce_master_columns, write_styled_excel
from .shared_lookups import (
    brand_from_text,
    brand_names_norm,
    clean_text,
    is_brand_row,
    is_sku_like,
    lookup_brand,
    norm,
    smart_ledger_match,
)


COMMON_SKIP_KEYWORDS = [
    "depreciation",
    "amortiz",
    "provision for",
    "salary",
    "salary & wage",
    "salary & wages",
    "return to",
    "returned due",
    "returned from",
    "batch code",
    "good return to",
    "bank charge",
    "bank charges",
    "bank reconciliation",
    "electricity bill",
    "hmo fee",
    "insurance",
    "receipt from",
    "paid directly into",
    "paid directlky into",
    "paid directily in to",
    "goods received by",
    "security expenses",
    "medical expense",
    "repair and maint",
    "out-going stock",
    "sales inventory pool",
    "stock",
    "loan",
    "drawings",
    "debt paid",
    "sales income",
    "reconcile",
    "reconciliation",
    "house rent",
    "rent fee",
    "interest fee",
    "payment",
    "expenses",
    "expense",
    "settlement",
    "transfer",
    "remittance",
    "tax",
    "payable",
]

PREPROCESS_REPLACEMENTS = [
    (re.compile(r"\b0F\b"), "OF"),
    (re.compile(r"\b0f\b"), "of"),
    (re.compile(r"\b0n\b"), "on"),
    (re.compile(r"\bBEHAVE ON BEHAVE\b", re.I), "BEHAVE"),
    (re.compile(r"([A-Z]{2,})(?=[a-z])"), r"\1 "),
    (re.compile(r"\bto(?=[A-Z])"), "to "),
]

VOUCHER_HEADER_COLS = [0, 1, 2, 3, 4, 5, 6, 7]


def preprocess_narration(text: str) -> str:
    cleaned = clean_text(text)
    for pattern, replacement in PREPROCESS_REPLACEMENTS:
        cleaned = pattern.sub(replacement, cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def contains_skip_keyword(text: str) -> bool:
    lowered = norm(text)
    return any(keyword in lowered for keyword in COMMON_SKIP_KEYWORDS)


def extract_retailer_keyword(text: str) -> str | None:
    cleaned = preprocess_narration(text)
    if not cleaned:
        return None
    if contains_skip_keyword(cleaned):
        return "__SKIP__"

    patterns = [
        r"(?i)\b(?:supplied|delivered|dispatch(?:ed|ing)?|sent)\b(?:\s+of\s+[\w'&.-]+)?\s+(?:to|for)\s+(.+?)\s+(?:on\s+(?:behalf|behave)\b|by\b|from\b|,|$)",
        r"(?i)\b(?:to|for)\s+(.+?)\s+on\s+(?:behalf|behave)\b",
        r"(?i)\b(?:supplied|delivered|dispatch(?:ed|ing)?|sent)\b(?:\s+of\s+[\w'&.-]+)?\s+(?:to|for)\s+(.+?)(?:$|\s+(?:on|by)\b|[,.;])",
        r"(?i)\b(?:goods?|product(?:s)?)\s+(?:supplied|delivered)\s+(?:to\s+)?(.+?)\s*,?\s+on\s+(?:behalf|behave)\b",
        r"(?i)\b(?:supplied|delivered|dispatch(?:ed|ing)?|sent)\s+(.+?)\s*,?\s+on\s+(?:behalf|behave)\b",
        r"(?i)\breceived\s+by\s+(.+?)(?:$|[,.;])",
        r"(?i)\bsales?\s+to\s+(.+?)(?:$|[,.;])",
        r"(?i)\b(?:supplied|delivered|dispatch(?:ed|ing)?|sent)\s+([A-Z][\w&'./,-]*(?:\s+[A-Z][\w&'./,-]*){0,5})",
        r"(?i)\b(?:inv(?:oice)?\s*no\.?|inv\s*no|invoice)\s*[\w/-]*\s*(?:\.\.\s*|\-\-\s*)([A-Z][A-Za-z0-9&'./,-]*(?:\s+[A-Z][A-Za-z0-9&'./,-]*){0,6})\s*$",
        r"(?i)\bto\s+([A-Z][A-Za-z0-9&'./,-]*(?:\s+[A-Z][A-Za-z0-9&'./,-]*){0,5})\s*$",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            candidate = match.group(1).strip(" ,.;:-")
            if candidate:
                return candidate

    tokens = cleaned.split()
    if 1 <= len(tokens) <= 5 and not contains_skip_keyword(cleaned):
        return cleaned
    return None


def _first_numeric(values) -> float | None:
    for value in values:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _is_date_row(row) -> bool:
    value = row.iloc[0]
    return pd.notna(value) and not isinstance(value, str) or (
        isinstance(value, str) and bool(re.match(r"^\d{4}-\d{2}-\d{2}", value))
    )


def _row_text(row, columns: list[int] = [1, 0, 3]) -> str:
    for idx in columns:
        if idx >= len(row):
            continue
        text = clean_text(row.iloc[idx])
        if text:
            return text
    return ""


def _extract_amount(row) -> float | None:
    value_candidates = [row.iloc[idx] for idx in [7, 6, 5, 4, 3, 2] if idx < len(row)]
    return _first_numeric(value_candidates)


def _extract_quantity(row) -> float | None:
    quantity_candidates = [row.iloc[idx] for idx in [2, 3, 4, 5, 6] if idx < len(row)]
    return _first_numeric(quantity_candidates)


def _is_structural_label(text: str) -> bool:
    lowered = norm(text)
    return lowered in {
        "out going stock",
        "stock",
        "sales inventory pool",
        "sales inventory reconciliation",
        "vch type",
        "particulars",
        "date",
        "debit",
        "credit",
        "quantity",
        "value",
    }


def _extract_group_records(group: list[pd.Series], vch_type_label: str) -> list[dict]:
    if not group:
        return []

    header = group[0]
    header_label = clean_text(header.iloc[1]) if len(header) > 1 else ""
    brand_header = brand_from_text(header_label) or header_label
    voucher_date = pd.to_datetime(header.iloc[0], errors="coerce")
    vch_no = clean_text(header.iloc[5]) if len(header) > 5 else ""
    source_amount = _first_numeric([header.iloc[idx] for idx in [6, 7] if idx < len(header)])

    narration_texts: list[str] = []
    line_items: list[dict] = []
    skip_group = False

    for row in group[1:]:
        text = _row_text(row)
        if not text:
            continue
        lowered = norm(text)
        if any(keyword in lowered for keyword in COMMON_SKIP_KEYWORDS):
            skip_group = True
            break
        if _is_structural_label(text):
            continue

        quantity = _extract_quantity(row)
        amount = _extract_amount(row)
        sku_like = is_sku_like(text) or ("mazara" in lowered and bool(re.search(r"\b\d", lowered)))

        if sku_like and quantity is not None:
            line_items.append(
                {
                    "sku": text,
                    "quantity": quantity,
                    "value": amount if amount is not None else source_amount,
                }
            )
        else:
            narration_texts.append(text)

    if skip_group:
        return []

    narration_raw = " ".join(narration_texts).strip()
    retailer_keyword = extract_retailer_keyword(narration_raw) if narration_raw else None
    if retailer_keyword == "__SKIP__":
        return []
    retailer = smart_ledger_match(retailer_keyword) if retailer_keyword else None
    retailer = retailer or ("-- CHECK RETAILER --" if line_items else None)

    records: list[dict] = []
    if line_items:
        for item in line_items:
            sku = clean_text(item["sku"])
            brand = lookup_brand(sku) or brand_header or lookup_brand(brand_header) or brand_header
            records.append(
                {
                    "Brand Partners": brand,
                    "Particulars": sku,
                    "Date": voucher_date,
                    "Retailers": retailer or "-- CHECK RETAILER --",
                    "Vch Type": vch_type_label,
                    "Vch No.": vch_no,
                    "Quantity": item["quantity"],
                    "Sales_Value": item["value"],
                }
            )
    elif retailer:
        if vch_type_label.lower() == "journal":
            if not re.search(r"\b(suppl|deliver|dispatch|sales?\s+to|received\s+by|credit note|return|on behalf)\b", narration_raw, re.I):
                return []
        amount = source_amount
        if amount is None:
            amount = 1.0
        if vch_type_label.lower() == "credit note" and amount is not None:
            amount = -abs(amount)
        records.append(
            {
                "Brand Partners": lookup_brand(header_label) or brand_header or header_label,
                "Particulars": narration_raw or header_label,
                "Date": voucher_date,
                "Retailers": retailer,
                "Vch Type": vch_type_label,
                "Vch No.": vch_no,
                "Quantity": 1.0,
                "Sales_Value": amount,
            }
        )

    return records


def clean_voucher_register(
    input_path: Path,
    output_path: Path,
    vch_type_label: str,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    if sheet_name:
        raw = pd.read_excel(input_path, sheet_name=sheet_name, header=None)
    else:
        raw = pd.read_excel(input_path, header=None)

    if raw.empty:
        df = pd.DataFrame(columns=MASTER_COLUMNS)
        write_styled_excel(df, output_path, sheet_name="Journal Register" if vch_type_label.lower() == "journal" else "Credit Note Register")
        return df

    vouchers: list[list[pd.Series]] = []
    current: list[pd.Series] = []
    for _, row in raw.iloc[1:].iterrows():
        if pd.notna(row.iloc[0]):
            if current:
                vouchers.append(current)
            current = [row]
        else:
            if current:
                current.append(row)
    if current:
        vouchers.append(current)

    records: list[dict] = []
    for group in vouchers:
        records.extend(_extract_group_records(group, vch_type_label))

    df = pd.DataFrame(records, columns=MASTER_COLUMNS)
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
        df["Sales_Value"] = pd.to_numeric(df["Sales_Value"], errors="coerce")
        df = df.sort_values(["Date", "Brand Partners", "Particulars"], kind="stable").reset_index(drop=True)

    sheet = "Journal Register" if vch_type_label.lower() == "journal" else "Credit Note Register"
    write_styled_excel(df, output_path, sheet_name=sheet)
    return df
