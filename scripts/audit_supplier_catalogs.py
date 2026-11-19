"""Reconcile versioned local supplier files without touching a database or network."""

from __future__ import annotations

import argparse
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path


SOURCES = {
    "megamix": ("codigo", "nome", "preco", 2360),
    "coletek": ("reference", "description", "price", 735),
}


def audit(path: Path, supplier: str, expected_count: int | None = None) -> dict:
    id_key, title_key, price_key, default_count = SOURCES[supplier]
    raw = path.read_bytes()
    rows = json.loads(raw.decode("utf-8-sig"), parse_float=Decimal)
    if not isinstance(rows, list):
        raise ValueError(f"{supplier}: top-level JSON must be a list")

    ids: set[str] = set()
    errors: list[str] = []
    missing_price = 0
    for number, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            errors.append(f"row {number}: not an object")
            continue
        source_id = row.get(id_key)
        title = row.get(title_key)
        if not isinstance(source_id, str) or not source_id.strip():
            errors.append(f"row {number}: missing {id_key}")
        elif source_id in ids:
            errors.append(f"row {number}: duplicate {id_key}")
        else:
            ids.add(source_id)
        if not isinstance(title, str) or not title.strip():
            errors.append(f"row {number}: missing {title_key}")
        price = row.get(price_key)
        if price is None:
            missing_price += 1
        else:
            try:
                amount = Decimal(str(price))
                if not amount.is_finite() or amount < 0:
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                errors.append(f"row {number}: invalid {price_key}")

    required = default_count if expected_count is None else expected_count
    if len(rows) != required:
        errors.append(f"count {len(rows)} differs from expected {required}")
    return {
        "supplier": supplier,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "source_records": len(rows),
        "unique_source_ids": len(ids),
        "records_without_price": missing_price,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--megamix", type=Path, default=Path("data/megamix_catalog_raw.json"))
    parser.add_argument("--coletek", type=Path, default=Path("data/coletek_catalog_raw.json"))
    args = parser.parse_args()
    results = [audit(args.megamix, "megamix"), audit(args.coletek, "coletek")]
    print(json.dumps({"catalogs": results, "total_source_records": sum(r["source_records"] for r in results)}, indent=2))
    return 1 if any(r["errors"] for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
