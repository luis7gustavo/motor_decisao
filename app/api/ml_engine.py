from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Query
from sqlalchemy import text

from app.core.database import engine
from pipelines.ml.compare import build_comparison_summary
from pipelines.ml.predict_opportunities import predict_current_opportunities
from pipelines.ml.train_model import train_models


router = APIRouter(prefix="/ml-engine", tags=["ml-engine"])


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


@router.post("/train")
def train_ml_engine() -> dict[str, Any]:
    return _serialize(train_models().__dict__)


@router.post("/predict")
def predict_ml_engine() -> dict[str, Any]:
    return _serialize(predict_current_opportunities().__dict__)


@router.get("/summary")
def ml_engine_summary() -> dict[str, Any]:
    return _serialize(build_comparison_summary())


@router.get("/opportunities")
def list_ml_opportunities(
    final_decision: str | None = Query(default=None, max_length=40),
    supplier_slug: str | None = Query(default=None, max_length=120),
    min_score: float = Query(default=0, ge=0, le=1),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    sql = text(
        """
        SELECT
            supplier_slug,
            product_title,
            heuristic_score,
            heuristic_decision,
            ml_score,
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
        WHERE (CAST(:final_decision AS varchar) IS NULL OR final_decision = CAST(:final_decision AS varchar))
          AND (CAST(:supplier_slug AS varchar) IS NULL OR supplier_slug = CAST(:supplier_slug AS varchar))
          AND final_score >= :min_score
        ORDER BY
            CASE final_decision
                WHEN 'comprar_teste' THEN 1
                WHEN 'revisar' THEN 2
                ELSE 3
            END,
            final_score DESC
        LIMIT :limit
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(
            sql,
            {
                "final_decision": final_decision,
                "supplier_slug": supplier_slug,
                "min_score": min_score,
                "limit": limit,
            },
        ).mappings()
        opportunities = [_serialize(dict(row)) for row in rows]
    return {"count": len(opportunities), "opportunities": opportunities}
