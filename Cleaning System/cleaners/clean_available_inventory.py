from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .io_utils import MASTER_COLUMNS, write_styled_excel
from .shared_lookups import clean_text, is_brand_row, is_sku_like, norm, brand_names_norm


def _parse_report_date(value) -> pd.Timestamp:
    text = clean_text(value)
    match = re.search(r"For\s+(\d{1,2}-[A-Za-z]{3}-\d{2})", text, flags=re.I)
    if not match:
        raise ValueError(f"Could not parse inventory report date from: {text!r}")
    return pd.to_datetime(match.group(1), format="%d-%b-%y")


def _make_vch_no(date: pd.Timestamp) -> str:
    return f"Inv:{date.day}{date.month}{str(date.year)[-2:]}"


def clean_available_inventory(
    input_path: Path,
    output_dir: Path | None = None,
    write_outputs: bool = True,
    include_zero_stock: bool = False,
) -> pd.DataFrame:
    raw = pd.read_excel(input_path, header=None)
    if raw.empty:
        df = pd.DataFrame(columns=MASTER_COLUMNS)
        if write_outputs and output_dir is not None:
            write_styled_excel(df, output_dir / "Cleaned_Available_Inventory.xlsx", sheet_name="Stock Category Summary")
        return df

    report_date = _parse_report_date(raw.iloc[1, 0])
    vch_no = _make_vch_no(report_date)

    records: list[dict] = []
    current_brand = None
    footer_markers = ("grand total", "printed by", "order generation", "head office")

    for _, row in raw.iloc[6:].iterrows():
        first = clean_text(row.iloc[0] if len(row) > 0 else "")
        first_norm = norm(first)
        if not first:
            continue
        if any(marker in first_norm for marker in footer_markers):
            break
        if any(marker in norm(clean_text(cell)) for cell in row.tolist() for marker in footer_markers):
            break

        quantity = pd.to_numeric(row.iloc[2] if len(row) > 2 else None, errors="coerce")
        value = pd.to_numeric(row.iloc[3] if len(row) > 3 else None, errors="coerce")

        if is_brand_row(first, brand_names_norm):
            current_brand = first
            continue

        if current_brand is None:
            current_brand = first
            continue

        if pd.isna(quantity) and pd.isna(value) and not include_zero_stock:
            continue

        records.append(
            {
                "Brand Partners": current_brand,
                "Particulars": first,
                "Date": report_date,
                "Retailers": current_brand,
                "Vch Type": "Available Inventory",
                "Vch No.": vch_no,
                "Quantity": quantity,
                "Sales_Value": value,
            }
        )

    df = pd.DataFrame(records, columns=MASTER_COLUMNS)
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
        df["Sales_Value"] = pd.to_numeric(df["Sales_Value"], errors="coerce")
        df = df.reset_index(drop=True)

    if write_outputs and output_dir is not None:
        write_styled_excel(df, output_dir / "Cleaned_Available_Inventory.xlsx", sheet_name="Stock Category Summary")
    return df


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    input_candidates = [
        base_dir / "inputs" / "Dirty_Available_Inventory.xls",
        base_dir / "Dirty" / "Available Inventory March Week 3 Dirty Data.xls",
    ]
    for candidate in input_candidates:
        if candidate.exists():
            input_path = candidate
            break
    else:
        raise FileNotFoundError("Could not find an available inventory input file.")

    output_dir = base_dir / "outputs"
    df = clean_available_inventory(input_path, output_dir=output_dir, write_outputs=True)
    print(f"Cleaned_Available_Inventory.xlsx: {len(df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

