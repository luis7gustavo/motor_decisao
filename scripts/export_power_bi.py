"""Exporta tabelas analiticas do SILLO para consumo no Power BI.

Os CSVs usam ponto e virgula como separador e UTF-8 com BOM para facilitar a
importacao no Power BI Desktop em ambientes pt-BR.
"""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text

try:
    from scripts import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # type: ignore  # noqa: F401

from motor_decisao.app.core.database import engine
from motor_decisao.app.core.settings import get_settings


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "processed" / "power_bi"


@dataclass(frozen=True)
class QueryExport:
    filename: str
    sql: str
    params: dict[str, Any] | None = None


QUERY_EXPORTS = (
    QueryExport(
        filename="fato_oportunidades_atuais.csv",
        sql="""
            SELECT
                id::text AS opportunity_id,
                decision_run_id::text AS decision_run_id,
                supplier_product_id::text AS supplier_product_id,
                supplier_raw_id::text AS supplier_raw_id,
                supplier_slug,
                product_title,
                supplier_price,
                estimated_market_price,
                estimated_net_profit,
                net_margin_pct,
                total_fee_pct,
                market_offer_count,
                market_source_count,
                price_history_count,
                mercado_livre_count,
                demand_score,
                match_confidence,
                decision_score,
                recommendation,
                confidence_level,
                scoring_version,
                array_to_string(risk_flags, ' | ') AS risk_flags,
                cardinality(risk_flags) AS risk_flag_count,
                generated_at,
                updated_at
            FROM gold.decision_opportunities
            ORDER BY
                CASE recommendation
                    WHEN 'comprar_teste' THEN 1
                    WHEN 'revisar' THEN 2
                    ELSE 3
                END,
                decision_score DESC,
                supplier_slug,
                product_title
        """,
    ),
    QueryExport(
        filename="fato_historico_oportunidades.csv",
        sql="""
            SELECT
                id::text AS snapshot_id,
                decision_run_id::text AS decision_run_id,
                supplier_product_id::text AS supplier_product_id,
                supplier_raw_id::text AS supplier_raw_id,
                supplier_slug,
                product_title,
                supplier_price,
                estimated_market_price,
                estimated_net_profit,
                net_margin_pct,
                total_fee_pct,
                market_offer_count,
                market_source_count,
                price_history_count,
                mercado_livre_count,
                demand_score,
                match_confidence,
                decision_score,
                recommendation,
                confidence_level,
                scoring_version,
                array_to_string(risk_flags, ' | ') AS risk_flags,
                cardinality(risk_flags) AS risk_flag_count,
                generated_at
            FROM gold.decision_opportunity_snapshots
            ORDER BY generated_at DESC, decision_score DESC
            LIMIT :history_limit
        """,
        params={"history_limit": 100000},
    ),
    QueryExport(
        filename="fato_execucoes_motor.csv",
        sql="""
            SELECT
                id::text AS decision_run_id,
                pipeline_run_id::text AS pipeline_run_id,
                scoring_version,
                status,
                started_at,
                finished_at,
                EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))::integer
                    AS duration_seconds,
                error_message
            FROM gold.decision_engine_runs
            ORDER BY started_at DESC
            LIMIT :run_limit
        """,
        params={"run_limit": 1000},
    ),
    QueryExport(
        filename="fato_execucoes_fontes.csv",
        sql="""
            SELECT
                source.id::text AS source_run_id,
                source.pipeline_run_id::text AS pipeline_run_id,
                pipeline.pipeline_name,
                source.source_name,
                source.source_type,
                source.status,
                source.raw_table_name,
                source.records_extracted,
                source.records_loaded,
                source.records_skipped,
                source.started_at,
                source.finished_at,
                EXTRACT(EPOCH FROM (
                    COALESCE(source.finished_at, NOW()) - source.started_at
                ))::integer AS duration_seconds,
                source.error_message
            FROM control.source_runs AS source
            LEFT JOIN control.pipeline_runs AS pipeline
                ON pipeline.id = source.pipeline_run_id
            ORDER BY source.started_at DESC
            LIMIT :source_run_limit
        """,
        params={"source_run_limit": 10000},
    ),
    QueryExport(
        filename="fato_qualidade_dados.csv",
        sql="""
            SELECT
                id::text AS quality_check_id,
                pipeline_run_id::text AS pipeline_run_id,
                source_run_id::text AS source_run_id,
                schema_name,
                table_name,
                check_name,
                status,
                metric_name,
                metric_value,
                threshold_value,
                message,
                created_at
            FROM control.data_quality_checks
            ORDER BY created_at DESC
            LIMIT :quality_limit
        """,
        params={"quality_limit": 10000},
    ),
    QueryExport(
        filename="fato_ml_oportunidades_atuais.csv",
        sql="""
            SELECT
                id::text AS ml_score_id,
                model_run_id::text AS model_run_id,
                decision_run_id::text AS decision_run_id,
                supplier_product_id::text AS supplier_product_id,
                supplier_slug,
                product_title,
                heuristic_score,
                heuristic_decision,
                ml_score,
                ml_prediction,
                ml_decision,
                final_score,
                final_decision,
                score_difference,
                decision_source,
                confidence_level,
                explanation,
                model_version,
                scored_at
            FROM gold.ml_opportunity_scores_latest
            ORDER BY
                CASE final_decision
                    WHEN 'comprar_teste' THEN 1
                    WHEN 'revisar' THEN 2
                    ELSE 3
                END,
                final_score DESC,
                supplier_slug,
                product_title
        """,
    ),
    QueryExport(
        filename="fato_execucoes_ml.csv",
        sql="""
            SELECT
                id::text AS model_run_id,
                model_version,
                model_type,
                status,
                training_rows,
                positive_rows,
                artifact_path,
                trained_at,
                error_message,
                created_at
            FROM gold.ml_model_runs
            ORDER BY created_at DESC
            LIMIT :ml_run_limit
        """,
        params={"ml_run_limit": 1000},
    ),
)


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "sim" if value else "nao"
    if isinstance(value, Decimal):
        return format(value, "f").replace(".", ",")
    if isinstance(value, float):
        return format(value, ".10g").replace(".", ",")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple, set)):
        return " | ".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _supplier_rows(config: dict[str, Any]) -> list[dict[str, Any]]:
    portfolio = config.get("portfolio") or {}
    supplier_rules = portfolio.get("supplier_rules") or {}
    rows = []
    for supplier_slug, supplier in supplier_rules.items():
        rows.append(
            {
                "supplier_slug": supplier_slug,
                "supplier_name": supplier.get("display_name") or supplier_slug.title(),
                "min_order_total": supplier.get("min_order_total", 0),
                "catalog_mode": supplier.get("catalog_mode", ""),
                "catalog_snapshot_date": supplier.get("catalog_snapshot_date", ""),
                "enabled_for_recommendation": supplier.get("enabled_for_recommendation", True),
                "notes": supplier.get("notes", ""),
            }
        )
    return sorted(rows, key=lambda row: str(row["supplier_slug"]))


def _shipping_scenario_rows(config: dict[str, Any]) -> list[dict[str, Any]]:
    portfolio = config.get("portfolio") or {}
    inbound_shipping = portfolio.get("inbound_shipping") or {}
    default_scenario = inbound_shipping.get("default_scenario", "base")
    scenarios = inbound_shipping.get("scenarios") or {}
    rows = []
    for index, (scenario, shipping_pct) in enumerate(scenarios.items(), start=1):
        rows.append(
            {
                "shipping_scenario": scenario,
                "shipping_pct": shipping_pct,
                "display_order": index,
                "is_default": scenario == default_scenario,
            }
        )
    return rows


def export_power_bi_data(*, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    config = get_settings().load_project_config()
    outputs: dict[str, dict[str, Any]] = {}

    with engine.connect() as connection:
        for export in QUERY_EXPORTS:
            rows = [
                dict(row)
                for row in connection.execute(
                    text(export.sql),
                    export.params or {},
                ).mappings()
            ]
            fieldnames = list(rows[0]) if rows else []
            path = output_dir / export.filename
            _write_csv(path, rows, fieldnames)
            outputs[export.filename] = {"path": str(path), "rows": len(rows)}

    dimensions = (
        (
            "dim_fornecedores.csv",
            _supplier_rows(config),
            [
                "supplier_slug",
                "supplier_name",
                "min_order_total",
                "catalog_mode",
                "catalog_snapshot_date",
                "enabled_for_recommendation",
                "notes",
            ],
        ),
        (
            "dim_cenarios_frete.csv",
            _shipping_scenario_rows(config),
            ["shipping_scenario", "shipping_pct", "display_order", "is_default"],
        ),
    )
    for filename, rows, fieldnames in dimensions:
        path = output_dir / filename
        _write_csv(path, rows, fieldnames)
        outputs[filename] = {"path": str(path), "rows": len(rows)}

    manifest = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "output_dir": str(output_dir),
        "csv_delimiter": ";",
        "csv_encoding": "utf-8-sig",
        "outputs": outputs,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Exporta a camada analitica para o Power BI.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Pasta de saida. Padrao: data/processed/power_bi.",
    )
    args = parser.parse_args()
    manifest = export_power_bi_data(output_dir=args.output_dir)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
