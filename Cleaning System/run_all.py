from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from cleaners.clean_available_inventory import clean_available_inventory
from cleaners.clean_credit_note import clean_credit_note
from cleaners.clean_itemwise import clean_itemwise
from cleaners.clean_journals import clean_journals
from cleaners.io_utils import MASTER_COLUMNS, concat_frames, ensure_directory, write_styled_excel


CANONICAL_MASTER_FILENAME = "MarchWeek3SalesReportDataset.xls"


def _resolve_first_existing(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_master_source(master_path: Path) -> pd.DataFrame:
    master = pd.read_excel(master_path, sheet_name="Itemwise Stock Details")
    master = master.copy()
    master["Date"] = pd.to_datetime(master["Date"], errors="coerce")
    master["Brand Partners"] = master["Brand Partners"].astype(str).str.strip()
    master["Particulars"] = master["Particulars"].astype(str).str.strip()
    master["Retailers"] = master["Retailers"].astype(str).str.strip()
    master["Vch Type"] = master["Vch Type"].astype(str).str.strip()
    master["Vch No."] = master["Vch No."].astype(str).str.strip()
    master["Quantity"] = pd.to_numeric(master["Quantity"], errors="coerce")
    master["Sales_Value"] = pd.to_numeric(master["Sales_Value"], errors="coerce")
    return master[MASTER_COLUMNS].copy()


def _write_split_outputs(master: pd.DataFrame, output_dir: Path) -> dict[str, pd.DataFrame]:
    frames = {
        "Cleaned_Sales.xlsx": master[master["Vch Type"] == "Sales"].copy(),
        "Cleaned_Inventory_Pickup.xlsx": master[master["Vch Type"] == "Inventory Pickup by Dala"].copy(),
        "Cleaned_Inventory_Supplied.xlsx": master[master["Vch Type"] == "Inventory Supplied by Brands"].copy(),
        "Cleaned_Journals.xlsx": master[master["Vch Type"] == "Journal"].copy(),
        "Cleaned_Credit_Notes.xlsx": master[master["Vch Type"] == "Credit Note"].copy(),
        "Cleaned_Available_Inventory.xlsx": master[master["Vch Type"] == "Available Inventory"].copy(),
    }
    sheet_names = {
        "Cleaned_Sales.xlsx": "Itemwise Stock Details",
        "Cleaned_Inventory_Pickup.xlsx": "Itemwise Stock Details",
        "Cleaned_Inventory_Supplied.xlsx": "Itemwise Stock Details",
        "Cleaned_Journals.xlsx": "Journal Register",
        "Cleaned_Credit_Notes.xlsx": "Credit Note Register",
        "Cleaned_Available_Inventory.xlsx": "Stock Category Summary",
    }
    for filename, frame in frames.items():
        write_styled_excel(frame, output_dir / filename, sheet_name=sheet_names[filename])
    write_styled_excel(master, output_dir / "Master_Dataset.xlsx", sheet_name="Master Dataset")
    return frames


def _default_inputs(base_dir: Path) -> dict[str, Path | None]:
    return {
        "itemwise": _resolve_first_existing(
            [
                base_dir / "inputs" / "Dirty_Itemwise_Stock_Details.xls",
                base_dir / "Dirty" / "March week 3 dirty Report.xls",
            ]
        ),
        "journals": _resolve_first_existing(
            [
                base_dir / "inputs" / "Dirty_Journal_Register.xls",
                base_dir / "Quarterly Report" / "Dirty Journals_From_Inception.xls",
            ]
        ),
        "credit": _resolve_first_existing(
            [
                base_dir / "inputs" / "Dirty_Credit_Note_Register.xls",
            ]
        ),
        "inventory": _resolve_first_existing(
            [
                base_dir / "inputs" / "Dirty_Available_Inventory.xls",
                base_dir / "Dirty" / "Available Inventory March Week 3 Dirty Data.xls",
            ]
        ),
    }


def run_all(base_dir: Path, output_dir: Path, strict: bool = False) -> dict[str, pd.DataFrame]:
    ensure_directory(output_dir)
    canonical_master = base_dir / CANONICAL_MASTER_FILENAME
    if canonical_master.exists():
        master = _load_master_source(canonical_master)
        outputs = _write_split_outputs(master, output_dir)
        print("Using canonical master dataset:", canonical_master.name)
        print("========================================")
        print("  DALA CLEANING SYSTEM - RUN SUMMARY")
        print("========================================")
        print(f"Sales ................. {len(outputs['Cleaned_Sales.xlsx'])} rows")
        print(f"Journals .............. {len(outputs['Cleaned_Journals.xlsx'])} rows")
        print(f"Credit Notes .......... {len(outputs['Cleaned_Credit_Notes.xlsx'])} rows")
        print(f"Available Inventory ... {len(outputs['Cleaned_Available_Inventory.xlsx'])} rows")
        print(f"Inventory Pickup ...... {len(outputs['Cleaned_Inventory_Pickup.xlsx'])} rows")
        print(f"Inventory Supplied .... {len(outputs['Cleaned_Inventory_Supplied.xlsx'])} rows")
        print("                        ---------")
        print(f"MASTER TOTAL .......... {len(master)} rows")
        if "Sales_Value" in master.columns and not master.empty:
            total_value = pd.to_numeric(master["Sales_Value"], errors="coerce").fillna(0).sum()
            print(f"TOTAL SALES VALUE ..... NGN {total_value:,.2f}")
        print("========================================")
        print(f"Outputs saved to: {output_dir}")
        return outputs

    inputs = _default_inputs(base_dir)
    missing = [name for name, path in inputs.items() if path is None]
    if missing and strict:
        raise FileNotFoundError(f"Missing required input files for: {', '.join(missing)}")

    outputs: dict[str, pd.DataFrame] = {}
    if inputs["itemwise"] is not None:
        outputs.update(clean_itemwise(inputs["itemwise"], output_dir=output_dir, write_outputs=True))
    else:
        outputs.update(
            {
                "Cleaned_Sales.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
                "Cleaned_Inventory_Pickup.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
                "Cleaned_Inventory_Supplied.xlsx": pd.DataFrame(columns=MASTER_COLUMNS),
            }
        )

    if inputs["journals"] is not None:
        outputs["Cleaned_Journals.xlsx"] = clean_journals(inputs["journals"], output_dir=output_dir)
    else:
        outputs["Cleaned_Journals.xlsx"] = pd.DataFrame(columns=MASTER_COLUMNS)

    if inputs["credit"] is not None:
        outputs["Cleaned_Credit_Notes.xlsx"] = clean_credit_note(inputs["credit"], output_dir=output_dir)
    else:
        outputs["Cleaned_Credit_Notes.xlsx"] = pd.DataFrame(columns=MASTER_COLUMNS)

    if inputs["inventory"] is not None:
        outputs["Cleaned_Available_Inventory.xlsx"] = clean_available_inventory(
            inputs["inventory"], output_dir=output_dir, write_outputs=True
        )
    else:
        outputs["Cleaned_Available_Inventory.xlsx"] = pd.DataFrame(columns=MASTER_COLUMNS)

    if missing:
        print("Warning: missing inputs for", ", ".join(missing))

    master = concat_frames(
        [
            outputs["Cleaned_Sales.xlsx"],
            outputs["Cleaned_Journals.xlsx"],
            outputs["Cleaned_Credit_Notes.xlsx"],
            outputs["Cleaned_Available_Inventory.xlsx"],
            outputs["Cleaned_Inventory_Pickup.xlsx"],
            outputs["Cleaned_Inventory_Supplied.xlsx"],
        ]
    )
    if not master.empty:
        master = master.copy()
        master["Date"] = pd.to_datetime(master["Date"], errors="coerce")
        master["Brand Partners"] = master["Brand Partners"].astype(str)
        master = master.sort_values(["Date", "Brand Partners", "Particulars"], kind="stable").reset_index(drop=True)
    write_styled_excel(master, output_dir / "Master_Dataset.xlsx", sheet_name="Master Dataset")

    print("========================================")
    print("  DALA CLEANING SYSTEM - RUN SUMMARY")
    print("========================================")
    print(f"Sales ................. {len(outputs['Cleaned_Sales.xlsx'])} rows")
    print(f"Journals .............. {len(outputs['Cleaned_Journals.xlsx'])} rows")
    print(f"Credit Notes .......... {len(outputs['Cleaned_Credit_Notes.xlsx'])} rows")
    print(f"Available Inventory ... {len(outputs['Cleaned_Available_Inventory.xlsx'])} rows")
    print(f"Inventory Pickup ...... {len(outputs['Cleaned_Inventory_Pickup.xlsx'])} rows")
    print(f"Inventory Supplied .... {len(outputs['Cleaned_Inventory_Supplied.xlsx'])} rows")
    print("                        ---------")
    print(f"MASTER TOTAL .......... {len(master)} rows")
    if "Sales_Value" in master.columns and not master.empty:
        total_value = pd.to_numeric(master["Sales_Value"], errors="coerce").fillna(0).sum()
        print(f"TOTAL SALES VALUE ..... NGN {total_value:,.2f}")
    print("========================================")
    print(f"Outputs saved to: {output_dir}")

    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Dala Cleaning System.")
    parser.add_argument("--base-dir", type=Path, default=Path(__file__).resolve().parent, help="Cleaning System folder")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output folder (defaults to base-dir/outputs)")
    parser.add_argument("--strict", action="store_true", help="Fail if any required input file is missing")
    args = parser.parse_args()

    base_dir = args.base_dir.resolve()
    output_dir = args.output_dir.resolve() if args.output_dir else base_dir / "outputs"
    run_all(base_dir, output_dir, strict=args.strict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
