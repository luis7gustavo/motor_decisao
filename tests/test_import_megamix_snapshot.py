from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import import_megamix_catalog as importer


class MegaMixSnapshotTest(unittest.TestCase):
    def test_missing_source_date_stops_before_database_access(self) -> None:
        with patch.object(importer.engine, "begin") as begin:
            with self.assertRaisesRegex(ValueError, "explicit source snapshot date"):
                importer.import_megamix_catalog()
            begin.assert_not_called()

    def test_reused_file_with_different_date_stops_before_run_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "catalog.json"
            path.write_text(json.dumps([{"nome": "Mouse", "codigo": "1"}]), encoding="utf-8")
            connection = MagicMock()
            connection.execute.return_value.first.return_value = (1,)
            transaction = MagicMock()
            transaction.__enter__.return_value = connection
            with (
                patch.object(importer.engine, "begin", return_value=transaction),
                patch.object(importer, "create_pipeline_run") as create_run,
                patch.object(importer, "get_settings") as settings,
            ):
                settings.return_value.load_project_config.return_value = {}
                with self.assertRaisesRegex(ValueError, "another snapshot date"):
                    importer.import_megamix_catalog(path=path, snapshot_date=date(2026, 6, 1))
                create_run.assert_not_called()
                params = connection.execute.call_args.args[1]
                self.assertEqual(params["snapshot_date"], "2026-06-01")
                self.assertEqual(len(params["source_sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
