from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import engine
from app.core.settings import get_settings
from pipelines.common.run_manager import (
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)
from pipelines.common.serialization import payload_hash, to_json_text


DEFAULT_CATALOG_PATH = Path(__file__).resolve().parents[1] / "data" / "coletek_catalog_raw.json"


@dataclass(frozen=True)
class ColetekImportResult:
    pipeline_run_id: str
    source_run_id: str
    extracted: int
    loaded: int
    skipped: int
    path: str


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _to_stock(value: Any) -> int | None:
    text_value = _clean_text(value)
    if not text_value:
        return None
    match = re.search(r"\d+", text_value)
    return int(match.group()) if match else None


def _to_date(value: Any) -> date:
    text_value = _clean_text(value)
    if not text_value:
        return date.today()
    return date.fromisoformat(text_value)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("Coletek catalog must be a JSON list.")
    rows = [row for row in data if isinstance(row, dict)]
    if len(rows) != len(data):
        raise ValueError("Coletek catalog contains non-object rows.")
    return rows


def import_coletek_catalog(
    *,
    path: Path = DEFAULT_CATALOG_PATH,
    triggered_by: str = "local_cli_coletek_import",
) -> ColetekImportResult:
    path = path.resolve()
    rows = _load_rows(path)
    project_config = get_settings().load_project_config()
    metadata = {
        "supplier_slug": "coletek",
        "source_file": str(path),
        "row_count": len(rows),
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="coletek_catalog_import",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name="coletek",
            source_type="manual",
            raw_table_name="bronze.supplier_products_raw",
            metadata=metadata,
        )

    loaded = 0
    skipped = 0
    try:
        with engine.begin() as connection:
            for row in rows:
                reference = _clean_text(row.get("reference"))
                title = _clean_text(row.get("description"))
                if not reference or not title:
                    skipped += 1
                    continue
                payload = {
                    "reference": reference,
                    "description": title,
                    "brand": _clean_text(row.get("brand")),
                    "ncm": _clean_text(row.get("ncm")),
                    "product_type": _clean_text(row.get("product_type")),
                    "price": _to_float(row.get("price")),
                    "stock_text": _clean_text(row.get("stock_text")),
                    "phase_out": bool(row.get("phase_out", False)),
                    "catalog_sections": row.get("catalog_sections") or [],
                    "source_snapshot_date": _clean_text(row.get("source_snapshot_date")),
                    "source_file": _clean_text(row.get("source_file")),
                }
                insert_row = connection.execute(
                    text(
                        """
                        INSERT INTO bronze.supplier_products_raw (
                            source_run_id,
                            supplier_slug,
                            source_url,
                            raw_title,
                            raw_price,
                            currency_id,
                            raw_stock,
                            sku,
                            payload,
                            payload_hash,
                            fetched_date
                        )
                        VALUES (
                            :source_run_id,
                            'coletek',
                            :source_url,
                            :raw_title,
                            :raw_price,
                            'BRL',
                            :raw_stock,
                            :sku,
                            CAST(:payload AS jsonb),
                            :payload_hash,
                            :fetched_date
                        )
                        ON CONFLICT (supplier_slug, payload_hash, fetched_date) DO NOTHING
                        RETURNING id
                        """
                    ),
                    {
                        "source_run_id": source_run_id,
                        "source_url": _clean_text(row.get("source_url")) or str(path),
                        "raw_title": title,
                        "raw_price": payload["price"],
                        "raw_stock": _to_stock(payload["stock_text"]),
                        "sku": reference,
                        "payload": to_json_text(payload),
                        "payload_hash": payload_hash(payload),
                        "fetched_date": _to_date(row.get("source_snapshot_date")),
                    },
                ).first()
                if insert_row is None:
                    skipped += 1
                else:
                    loaded += 1
        status = "success" if loaded > 0 else "partial"
        error_message = None
    except Exception as exc:
        status = "failed"
        error_message = str(exc)

    with engine.begin() as connection:
        finish_source_run(
            connection,
            source_run_id=source_run_id,
            status=status,
            records_extracted=len(rows),
            records_loaded=loaded,
            records_skipped=skipped,
            metadata=metadata,
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="bronze",
            table_name="supplier_products_raw",
            check_name="coletek_rows_loaded_gt_zero",
            status="passed" if loaded > 0 else "warning",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={"records_extracted": len(rows), "records_skipped": skipped},
            message=None if loaded > 0 else "No new Coletek rows loaded.",
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=status,
            metadata={**metadata, "records_loaded": loaded, "records_skipped": skipped},
            error_message=error_message,
        )

    if error_message:
        raise RuntimeError(error_message)

    return ColetekImportResult(
        pipeline_run_id=str(pipeline_run_id),
        source_run_id=str(source_run_id),
        extracted=len(rows),
        loaded=loaded,
        skipped=skipped,
        path=str(path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa catalogo Coletek para Bronze.")
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_CATALOG_PATH,
        help="Caminho do coletek_catalog_raw.json.",
    )
    args = parser.parse_args()
    result = import_coletek_catalog(path=args.path)
    print(
        f"Coletek importado: {result.loaded} carregados / "
        f"{result.extracted} extraidos / {result.skipped} duplicados ou invalidos"
    )
    print(f"pipeline_run_id={result.pipeline_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
