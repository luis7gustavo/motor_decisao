from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query
from sqlalchemy import text

from app.core.database import engine
from pipelines.decision_engine.build import build_decision_opportunities
from scripts.import_coletek_catalog import (
    DEFAULT_CATALOG_PATH as DEFAULT_COLETEK_CATALOG_PATH,
    import_coletek_catalog,
)
from scripts.import_megamix_catalog import DEFAULT_CATALOG_PATH, import_megamix_catalog


router = APIRouter(prefix="/decision-engine", tags=["decision-engine"])


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value.__class__.__name__ == "UUID":
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _serialize(value) for key, value in row.items()}


@router.post("/run")
def run_decision_engine(
    import_megamix: bool = Query(default=True),
    megamix_path: str | None = Query(default=None),
    import_coletek: bool = Query(default=True),
    coletek_path: str | None = Query(default=None),
) -> dict[str, Any]:
    import_summary = None
    coletek_import_summary = None
    if import_megamix:
        path = Path(megamix_path) if megamix_path else DEFAULT_CATALOG_PATH
        import_summary = import_megamix_catalog(
            path=path,
            triggered_by="api_decision_engine",
        )
    if import_coletek:
        path = Path(coletek_path) if coletek_path else DEFAULT_COLETEK_CATALOG_PATH
        coletek_import_summary = import_coletek_catalog(
            path=path,
            triggered_by="api_decision_engine",
        )

    result = build_decision_opportunities(triggered_by="api_decision_engine")
    return {
        "import_megamix": _serialize(import_summary.__dict__) if import_summary else None,
        "import_coletek": (
            _serialize(coletek_import_summary.__dict__) if coletek_import_summary else None
        ),
        "decision_engine": _serialize(result.__dict__),
    }


@router.get("/summary")
def decision_summary() -> dict[str, Any]:
    sql = text(
        """
        SELECT
            supplier_slug,
            recommendation,
            confidence_level,
            COUNT(*) AS products,
            ROUND(AVG(decision_score), 2) AS avg_decision_score,
            ROUND(AVG(demand_score), 2) AS avg_demand_score,
            ROUND(AVG(match_confidence), 2) AS avg_match_confidence,
            ROUND(AVG(net_margin_pct), 4) AS avg_net_margin_pct,
            MAX(generated_at) AS last_generated_at
        FROM gold.decision_opportunities
        GROUP BY supplier_slug, recommendation, confidence_level
        ORDER BY supplier_slug, recommendation, confidence_level
        """
    )
    totals_sql = text(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE recommendation = 'comprar_teste') AS comprar_teste,
            COUNT(*) FILTER (WHERE recommendation = 'revisar') AS revisar,
            COUNT(*) FILTER (WHERE recommendation = 'ignorar') AS ignorar,
            COUNT(*) FILTER (WHERE confidence_level = 'alta') AS alta_confianca,
            COUNT(*) FILTER (WHERE confidence_level = 'media') AS media_confianca,
            COUNT(*) FILTER (WHERE confidence_level = 'baixa') AS baixa_confianca,
            MAX(generated_at) AS last_generated_at
        FROM gold.decision_opportunities
        """
    )
    latest_run_sql = text(
        """
        SELECT
            id::text AS id,
            pipeline_run_id::text AS pipeline_run_id,
            scoring_version,
            status,
            metadata,
            started_at,
            finished_at,
            error_message
        FROM gold.decision_engine_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    with engine.connect() as connection:
        totals = connection.execute(totals_sql).mappings().one()
        rows = connection.execute(sql).mappings().all()
        latest_run = connection.execute(latest_run_sql).mappings().first()
    return {
        "totals": _serialize_row(dict(totals)),
        "latest_run": _serialize_row(dict(latest_run)) if latest_run else None,
        "by_supplier_recommendation": [_serialize_row(dict(row)) for row in rows],
    }


@router.get("/opportunities")
def list_opportunities(
    recommendation: str | None = Query(default=None, max_length=40),
    supplier_slug: str | None = Query(default=None, max_length=120),
    confidence_level: str | None = Query(default=None, max_length=20),
    min_score: float = Query(default=0, ge=0, le=100),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    sql = text(
        """
        SELECT
            id::text AS id,
            decision_run_id::text AS decision_run_id,
            scoring_version,
            supplier_slug,
            product_title,
            supplier_price,
            estimated_market_price,
            estimated_net_profit,
            net_margin_pct,
            market_offer_count,
            market_source_count,
            price_history_count,
            mercado_livre_count,
            demand_score,
            match_confidence,
            decision_score,
            recommendation,
            confidence_level,
            risk_flags,
            evidence,
            generated_at
        FROM gold.decision_opportunities
        WHERE (CAST(:recommendation AS varchar) IS NULL OR recommendation = CAST(:recommendation AS varchar))
          AND (CAST(:supplier_slug AS varchar) IS NULL OR supplier_slug = CAST(:supplier_slug AS varchar))
          AND (CAST(:confidence_level AS varchar) IS NULL OR confidence_level = CAST(:confidence_level AS varchar))
          AND decision_score >= :min_score
        ORDER BY
            CASE recommendation
                WHEN 'comprar_teste' THEN 1
                WHEN 'revisar' THEN 2
                ELSE 3
            END,
            decision_score DESC,
            demand_score DESC,
            estimated_net_profit DESC NULLS LAST
        LIMIT :limit
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(
            sql,
            {
                "recommendation": recommendation,
                "supplier_slug": supplier_slug,
                "confidence_level": confidence_level,
                "min_score": min_score,
                "limit": limit,
            },
        ).mappings().all()
    return {
        "count": len(rows),
        "opportunities": [_serialize_row(dict(row)) for row in rows],
    }


@router.get("/runs")
def list_decision_runs(
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    sql = text(
        """
        SELECT
            id::text AS id,
            pipeline_run_id::text AS pipeline_run_id,
            scoring_version,
            status,
            metadata,
            started_at,
            finished_at,
            error_message
        FROM gold.decision_engine_runs
        ORDER BY started_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(sql, {"limit": limit}).mappings().all()
    return {
        "count": len(rows),
        "runs": [_serialize_row(dict(row)) for row in rows],
    }
