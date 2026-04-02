from __future__ import annotations

from pathlib import Path

import pandas as pd

from .io_utils import MASTER_COLUMNS, write_styled_excel
from .shared_lookups import clean_text, lookup_brand


ITEMWISE_COLUMNS = [
    "Brand Partners",
    "Particulars",
    "Date",
    "Retailers",
    "Vch Type",
    "Vch No.",
    "Quantity",
    "Sales_Value",
]


def _resolve_header_row(raw: pd.DataFrame) -> int:
    for idx, row in raw.iterrows():
        values = [clean_text(value).lower() for value in row.tolist()]
        if "vch type" in values and "sales_value" in values:
            return idx
    return 1


def clean_itemwise(input_path: Path, output_dir: Path | None = None, write_outputs: bool = True) -> dict[str, pd.DataFrame]:
    raw = pd.read_excel(input_path, header=None)
    if raw.empty:
        frames = {
            "Cleaned_Sales.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
            "Cleaned_Inventory_Pickup.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
            "Cleaned_Inventory_Supplied.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
        }
        if write_outputs and output_dir is not None:
            for filename, frame in frames.items():
                write_styled_excel(frame, output_dir / filename, sheet_name="Itemwise Stock Details")
        return frames

    header_idx = _resolve_header_row(raw)
    data = raw.iloc[header_idx + 1 :].copy().reset_index(drop=True)
    if not data.empty and data.columns[0] == 0:
        data = data.drop(columns=[0])
    elif 0 in data.columns:
        data = data.drop(columns=[0])

    data = data.rename(
        columns={
            1: "Brand Partners",
            2: "Particulars",
            3: "Date",
            4: "Retailers",
            5: "Vch Type",
            6: "Vch No.",
            7: "Quantity",
            8: "Sales_Value",
        }
    )
    for column in ITEMWISE_COLUMNS:
        if column not in data.columns:
            data[column] = pd.NA

    data = data[ITEMWISE_COLUMNS].copy()
    data["Brand Partners"] = data["Brand Partners"].apply(lambda value: clean_text(value) or lookup_brand(clean_text(value)) or value)
    data["Particulars"] = data["Particulars"].apply(clean_text)
    data["Retailers"] = data["Retailers"].apply(clean_text)
    data["Vch Type"] = data["Vch Type"].apply(clean_text)
    data["Vch No."] = data["Vch No."].apply(clean_text)
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data["Quantity"] = pd.to_numeric(data["Quantity"], errors="coerce")
    data["Sales_Value"] = pd.to_numeric(data["Sales_Value"], errors="coerce")

    data = data.dropna(how="all")
    data = data[data["Vch Type"].isin(["Sales", "Inventory Pickup by Dala", "Inventory Supplied by Brands"])].copy()
    data = data.reset_index(drop=True)

    outputs = {
        "Cleaned_Sales.xlsx": data[data["Vch Type"] == "Sales"].copy(),
        "Cleaned_Inventory_Pickup.xlsx": data[data["Vch Type"] == "Inventory Pickup by Dala"].copy(),
        "Cleaned_Inventory_Supplied.xlsx": data[data["Vch Type"] == "Inventory Supplied by Brands"].copy(),
    }

    if write_outputs and output_dir is not None:
        for filename, frame in outputs.items():
            write_styled_excel(frame, output_dir / filename, sheet_name="Itemwise Stock Details")

    return outputs


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    input_candidates = [
        base_dir / "inputs" / "Dirty_Itemwise_Stock_Details.xls",
        base_dir / "Dirty" / "March week 3 dirty Report.xls",
    ]
    for candidate in input_candidates:
        if candidate.exists():
            input_path = candidate
            break
    else:
        raise FileNotFoundError("Could not find an itemwise stock details input file.")

    output_dir = base_dir / "outputs"
    frames = clean_itemwise(input_path, output_dir=output_dir, write_outputs=True)
    for filename, frame in frames.items():
        print(f"{filename}: {len(frame)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

