from __future__ import annotations

from pathlib import Path

from .journal_core import clean_voucher_register


def clean_journals(input_path: Path, output_dir: Path | None = None, write_outputs: bool = True):
    output_dir = Path(output_dir) if output_dir is not None else None
    output_path = (output_dir or input_path.parent) / "Cleaned_Journals.xlsx"
    return clean_voucher_register(input_path, output_path, "Journal", sheet_name="Journal Register")


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    input_candidates = [
        base_dir / "inputs" / "Dirty_Journal_Register.xls",
        base_dir / "Quarterly Report" / "Dirty Journals_From_Inception.xls",
    ]
    for candidate in input_candidates:
        if candidate.exists():
            input_path = candidate
            break
    else:
        raise FileNotFoundError("Could not find a journal register input file.")

    output_dir = base_dir / "outputs"
    output_path = output_dir / "Cleaned_Journals.xlsx"
    df = clean_voucher_register(input_path, output_path, "Journal", sheet_name="Journal Register")
    print(f"Cleaned_Journals.xlsx: {len(df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

