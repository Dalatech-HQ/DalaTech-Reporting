from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"
REPORT_PATH = OUTPUT_DIR / "comparison_report.md"


@dataclass
class Comparison:
    title: str
    source_path: Path
    output_path: Path
    source_rows: int | None
    output_rows: int | None
    source_columns: list[str]
    output_columns: list[str]
    notes: list[str]


def _read_frame(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    if sheet_name:
        return pd.read_excel(path, sheet_name=sheet_name)
    return pd.read_excel(path)


def _safe_read(path: Path, sheet_name: str | None = None) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return _read_frame(path, sheet_name=sheet_name)
    except Exception:
        return None


def _columns(df: pd.DataFrame | None) -> list[str]:
    if df is None:
        return []
    return [str(col).strip() for col in df.columns.tolist()]


def _rows(df: pd.DataFrame | None) -> int | None:
    if df is None:
        return None
    return int(len(df))


def _make_comparison(title: str, source: Path, output: Path, source_sheet: str | None = None, output_sheet: str | None = None) -> Comparison:
    source_df = _safe_read(source, source_sheet)
    output_df = _safe_read(output, output_sheet)
    source_columns = _columns(source_df)
    output_columns = _columns(output_df)
    notes: list[str] = []

    if source_df is None:
        notes.append("source file missing or unreadable")
    if output_df is None:
        notes.append("output file missing or unreadable")

    if source_df is not None and output_df is not None:
        if source_columns == output_columns:
            notes.append("column layout matches")
        else:
            shared = [col for col in source_columns if col in output_columns]
            notes.append(f"shared columns: {', '.join(shared) if shared else 'none'}")
            missing_from_output = [col for col in source_columns if col not in output_columns]
            extra_in_output = [col for col in output_columns if col not in source_columns]
            if missing_from_output:
                notes.append(f"missing from output: {', '.join(missing_from_output[:8])}")
            if extra_in_output:
                notes.append(f"extra in output: {', '.join(extra_in_output[:8])}")
        if len(source_df) == len(output_df):
            notes.append("row counts match")
        else:
            notes.append(f"row count delta: source {len(source_df)} vs output {len(output_df)}")

    return Comparison(
        title=title,
        source_path=source,
        output_path=output,
        source_rows=_rows(source_df),
        output_rows=_rows(output_df),
        source_columns=source_columns,
        output_columns=output_columns,
        notes=notes,
    )


def build_report() -> str:
    comparisons = [
        _make_comparison(
            "Weekly sales source vs cleaned sales output",
            BASE_DIR / "Quarterly Report" / "Q1SalesReport.xls",
            OUTPUT_DIR / "Cleaned_Sales.xlsx",
            source_sheet="Itemwise Stock Details",
            output_sheet="Itemwise Stock Details",
        ),
        _make_comparison(
            "Weekly dirty activity source vs cleaned sales output",
            BASE_DIR / "Dirty" / "March week 3 dirty Report.xls",
            OUTPUT_DIR / "Cleaned_Sales.xlsx",
            source_sheet="Itemwise Stock Details",
            output_sheet="Itemwise Stock Details",
        ),
        _make_comparison(
            "Dirty available inventory source vs cleaned inventory output",
            BASE_DIR / "Dirty" / "Available Inventory March Week 3 Dirty Data.xls",
            OUTPUT_DIR / "Cleaned_Available_Inventory.xlsx",
            source_sheet="Stock Category Summary",
            output_sheet="Stock Category Summary",
        ),
        _make_comparison(
            "Quarterly journal reference vs weekly cleaned journal output",
            BASE_DIR / "Quarterly Report" / "Cleaned_Journals_From_Inception.xlsx",
            OUTPUT_DIR / "Cleaned_Journals.xlsx",
            source_sheet="Journal Register",
            output_sheet="Journal Register",
        ),
        _make_comparison(
            "Quarterly credit-note reference vs weekly cleaned credit-note output",
            BASE_DIR / "Quarterly Report" / "Cleaned_Credit_Note_From_Inception.xlsx",
            OUTPUT_DIR / "Cleaned_Credit_Notes.xlsx",
            source_sheet="Credit Note Register",
            output_sheet="Credit Note Register",
        ),
    ]

    lines = [
        "# Cleaning System Comparison Report",
        "",
        f"Generated from `{BASE_DIR}`",
        "",
    ]

    for item in comparisons:
        lines.extend(
            [
                f"## {item.title}",
                f"- Source: `{item.source_path}`",
                f"- Output: `{item.output_path}`",
                f"- Source rows: {item.source_rows if item.source_rows is not None else 'unavailable'}",
                f"- Output rows: {item.output_rows if item.output_rows is not None else 'unavailable'}",
                f"- Source columns: {', '.join(item.source_columns) if item.source_columns else 'unavailable'}",
                f"- Output columns: {', '.join(item.output_columns) if item.output_columns else 'unavailable'}",
            ]
        )
        for note in item.notes:
            lines.append(f"- Note: {note}")
        lines.append("")

    lines.extend(
        [
            "## Interpretation",
            "- The weekly sales pipeline is intentionally narrower than the quarterly sales source.",
            "- The quarterly journal and credit-note references are inception-to-date outputs, so their row counts are expected to exceed the weekly run.",
            "- The raw dirty files keep their Tally-native layout, while the cleaned outputs are normalized for the reporting system.",
        ]
    )

    return "\n".join(lines).strip() + "\n"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report()
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved comparison report to {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
