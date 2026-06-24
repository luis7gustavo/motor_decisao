from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

try:
    from scripts import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # type: ignore  # noqa: F401

from motor_decisao.app.core.database import engine
from motor_decisao.app.core.settings import get_settings
from motor_decisao.pipelines.common.run_manager import (
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)
from motor_decisao.pipelines.common.serialization import payload_hash, to_json_text


DEFAULT_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "raw" / "megamix_catalog_raw.json"
)


@dataclass(frozen=True)
class MegaMixImportResult:
    pipeline_run_id: str
    source_run_id: str
    extracted: int
    loaded: int
    skipped: int
    path: str


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError("MegaMix catalog must be a JSON list.")
    rows = [row for row in data if isinstance(row, dict)]
    if len(rows) != len(data):
        raise ValueError("MegaMix catalog contains non-object rows.")
    return rows


def import_megamix_catalog(
    *,
    path: Path = DEFAULT_CATALOG_PATH,
    triggered_by: str = "local_cli_megamix_import",
) -> MegaMixImportResult:
    path = path.resolve()
    rows = _load_rows(path)
    project_config = get_settings().load_project_config()
    metadata = {
        "supplier_slug": "megamix",
        "source_file": str(path),
        "row_count": len(rows),
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="megamix_catalog_import",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name="megamix",
            source_type="manual",
            raw_table_name="bronze.supplier_products_raw",
            metadata=metadata,
        )

    loaded = 0
    skipped = 0
    try:
        with engine.begin() as connection:
            for row in rows:
                title = _clean_text(row.get("nome"))
                if not title:
                    skipped += 1
                    continue
                payload = {
                    "codigo": _clean_text(row.get("codigo")),
                    "barcode": _clean_text(row.get("barcode")),
                    "nome": title,
                    "modelo": _clean_text(row.get("modelo")),
                    "preco": _to_float(row.get("preco")),
                    "unidade": _clean_text(row.get("unidade")),
                    "fonte": _clean_text(row.get("fonte")) or "megamix",
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
                            sku,
                            ean,
                            payload,
                            payload_hash
                        )
                        VALUES (
                            :source_run_id,
                            'megamix',
                            :source_url,
                            :raw_title,
                            :raw_price,
                            'BRL',
                            :sku,
                            :ean,
                            CAST(:payload AS jsonb),
                            :payload_hash
                        )
                        ON CONFLICT (supplier_slug, payload_hash, fetched_date) DO NOTHING
                        RETURNING id
                        """
                    ),
                    {
                        "source_run_id": source_run_id,
                        "source_url": str(path),
                        "raw_title": title,
                        "raw_price": payload["preco"],
                        "sku": payload["codigo"],
                        "ean": payload["barcode"],
                        "payload": to_json_text(payload),
                        "payload_hash": payload_hash(payload),
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
            check_name="megamix_rows_loaded_gt_zero",
            status="passed" if loaded > 0 else "warning",
            metric_name="records_loaded",
            metric_value=loaded,
            threshold_value=1,
            details={"records_extracted": len(rows), "records_skipped": skipped},
            message=None if loaded > 0 else "No new MegaMix rows loaded.",
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

    return MegaMixImportResult(
        pipeline_run_id=str(pipeline_run_id),
        source_run_id=str(source_run_id),
        extracted=len(rows),
        loaded=loaded,
        skipped=skipped,
        path=str(path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa catalogo MegaMix para a Bronze.")
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_CATALOG_PATH,
        help="Caminho do megamix_catalog_raw.json.",
    )
    args = parser.parse_args()
    result = import_megamix_catalog(path=args.path)
    print(
        f"MegaMix importado: {result.loaded} carregados / "
        f"{result.extracted} extraidos / {result.skipped} duplicados ou invalidos"
    )
    print(f"pipeline_run_id={result.pipeline_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
