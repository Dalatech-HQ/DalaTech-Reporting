import io
from pathlib import Path
import unittest

import pandas as pd

import app as app_module
from modules.weekly_consolidator import consolidate_weekly_files, write_consolidated_workbook


ROOT_DIR = Path(__file__).resolve().parents[3]
UNCLEAN_DIR = ROOT_DIR / "Unclean"


class WeeklyConsolidatorTests(unittest.TestCase):
    def _source_files(self):
        required = {
            "sales": UNCLEAN_DIR / "salesrepweek3.xls",
            "inventory": UNCLEAN_DIR / "AvailableInventoryAprilWeek3.xls",
            "journal": UNCLEAN_DIR / "JournalAprilWeek3.xls",
            "credit": UNCLEAN_DIR / "creditnoteAprilWeek3.xls",
        }
        missing = [str(path) for path in required.values() if not path.exists()]
        if missing:
            self.skipTest(f"Weekly consolidation fixtures missing: {missing}")
        return required

    def test_consolidates_four_weekly_exports_to_reference_shape(self):
        files = self._source_files()

        result = consolidate_weekly_files(
            sales_file=files["sales"],
            inventory_file=files["inventory"],
            journal_file=files["journal"],
            credit_file=files["credit"],
        )

        self.assertEqual(result.start_date.isoformat(), "2026-04-17")
        self.assertEqual(result.end_date.isoformat(), "2026-04-23")
        self.assertEqual(len(result.dataframe), 1193)
        self.assertEqual(
            result.dataframe["Vch Type"].value_counts().to_dict(),
            {
                "Sales": 960,
                "Available Inventory": 126,
                "Inventory Pickup by Dala": 37,
                "Journal": 34,
                "Credit Note": 26,
                "Inventory Supplied by Brands": 10,
            },
        )
        self.assertEqual(
            list(result.dataframe.columns),
            ["Brand Partners", "Particulars", "Date", "Retailers", "Vch Type", "Vch No.", "Quantity", "Sales_Value"],
        )

    def test_writes_reference_style_single_sheet_workbook(self):
        files = self._source_files()
        result = consolidate_weekly_files(
            sales_file=files["sales"],
            inventory_file=files["inventory"],
            journal_file=files["journal"],
            credit_file=files["credit"],
        )

        workbook_bytes = write_consolidated_workbook(result.dataframe)
        workbook = pd.ExcelFile(io.BytesIO(workbook_bytes))
        df = pd.read_excel(io.BytesIO(workbook_bytes), sheet_name="Itemwise Stock Details")

        self.assertEqual(workbook.sheet_names, ["Itemwise Stock Details"])
        self.assertEqual(len(df), 1193)
        self.assertEqual(list(df.columns), list(result.dataframe.columns))

    def test_download_endpoint_accepts_four_uploaded_files(self):
        files = self._source_files()
        client = app_module.app.test_client()

        with files["sales"].open("rb") as sales, files["inventory"].open("rb") as inventory, files["journal"].open("rb") as journal, files["credit"].open("rb") as credit:
            response = client.post(
                "/api/weekly-consolidation/download",
                data={
                    "sales_file": (sales, "salesrepweek3.xls"),
                    "inventory_file": (inventory, "AvailableInventoryAprilWeek3.xls"),
                    "journal_file": (journal, "JournalAprilWeek3.xls"),
                    "credit_file": (credit, "creditnoteAprilWeek3.xls"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.headers["Content-Type"],
        )
        workbook = pd.ExcelFile(io.BytesIO(response.data))
        self.assertEqual(workbook.sheet_names, ["Itemwise Stock Details"])

    def test_generate_endpoint_submits_consolidated_workbook_to_async_pipeline(self):
        files = self._source_files()
        client = app_module.app.test_client()
        calls = {}
        original_submit = app_module._submit_generation_job

        def fake_submit_generation_job(**kwargs):
            calls.update(kwargs)
            return "job-weekly-123"

        app_module._submit_generation_job = fake_submit_generation_job
        try:
            with files["sales"].open("rb") as sales, files["inventory"].open("rb") as inventory, files["journal"].open("rb") as journal, files["credit"].open("rb") as credit:
                response = client.post(
                    "/api/weekly-consolidation/generate",
                    data={
                        "sales_file": (sales, "salesrepweek3.xls"),
                        "inventory_file": (inventory, "AvailableInventoryAprilWeek3.xls"),
                        "journal_file": (journal, "JournalAprilWeek3.xls"),
                        "credit_file": (credit, "creditnoteAprilWeek3.xls"),
                    },
                    content_type="multipart/form-data",
                )
        finally:
            app_module._submit_generation_job = original_submit

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["job_id"], "job-weekly-123")
        self.assertEqual(calls["start_date"], "2026-04-17")
        self.assertEqual(calls["end_date"], "2026-04-23")
        self.assertEqual(calls["report_type"], "weekly")
        self.assertEqual(len(calls["data_frame"]), 1193)
        self.assertGreater(len(calls["file_bytes"]), 1000)
        self.assertGreater(len(calls["archive_bytes"]), 1000)
