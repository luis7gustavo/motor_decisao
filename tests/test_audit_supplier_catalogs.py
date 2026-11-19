import json
import tempfile
import unittest
from pathlib import Path

from scripts.audit_supplier_catalogs import audit


class SupplierCatalogAuditTests(unittest.TestCase):
    def _catalog(self, rows, *, bom=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "catalog.json"
        path.write_text(json.dumps(rows), encoding="utf-8-sig" if bom else "utf-8")
        return path

    def test_reconciles_bom_and_keeps_missing_price_as_unknown(self):
        path = self._catalog([
            {"reference": "A", "description": "Mouse", "price": None},
            {"reference": "B", "description": "Teclado", "price": 20.0135},
        ], bom=True)
        result = audit(path, "coletek", expected_count=2)
        self.assertEqual(result["source_records"], 2)
        self.assertEqual(result["records_without_price"], 1)
        self.assertEqual(result["errors"], [])

    def test_rejects_duplicate_identity_and_invalid_price(self):
        path = self._catalog([
            {"codigo": "A", "nome": "Mouse", "preco": 5},
            {"codigo": "A", "nome": "Teclado", "preco": -1},
        ])
        result = audit(path, "megamix", expected_count=2)
        self.assertEqual(result["unique_source_ids"], 1)
        self.assertEqual(len(result["errors"]), 2)

    def test_rejects_wrong_count_and_missing_title(self):
        path = self._catalog([{"codigo": "A", "nome": "", "preco": 1}])
        result = audit(path, "megamix", expected_count=2)
        self.assertEqual(len(result["errors"]), 2)


if __name__ == "__main__":
    unittest.main()
