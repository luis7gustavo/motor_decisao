from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sqlalchemy import text

from app.core.database import engine
from pipelines.common.serialization import to_json_text
from pipelines.ml.build_features import fetch_current_feature_records, records_as_matrix
from pipelines.ml.config import ROOT, get_ml_config
from pipelines.ml.explain_predictions import explain_prediction
from pipelines.ml.hybrid import combine_decisions


@dataclass(frozen=True)
class PredictResult:
    model_run_id: str
    model_version: str
    scored: int
    comprar_teste: int
    revisar: int
    ignorar: int
    disagreements: int
    comparison_path: str


def _latest_model_run() -> dict[str, Any]:
    sql = text(
        """
        SELECT
            id::text AS model_run_id,
            model_version,
            model_type,
            artifact_path
        FROM gold.ml_model_runs
        WHERE status = 'success'
        ORDER BY trained_at DESC NULLS LAST, created_at DESC
        LIMIT 1
        """
    )
    with engine.connect() as connection:
        row = connection.execute(sql).mappings().first()
    if row is None:
        raise RuntimeError("No successful ML model found. Run scripts/ml_train.py first.")
    return dict(row)


def _positive_scores(model, x_values) -> np.ndarray:
    probabilities = model.predict_proba(x_values)
    classes = list(model.classes_)
    return probabilities[:, classes.index(1)]


def _write_comparison(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "supplier_product_id",
        "supplier_slug",
        "product_title",
        "heuristic_score",
        "heuristic_decision",
        "ml_score",
        "ml_decision",
        "final_score",
        "final_decision",
        "score_difference",
        "decision_source",
        "confidence_level",
        "explanation",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(
            {fieldname: row.get(fieldname) for fieldname in fieldnames}
            for row in rows
        )


def predict_current_opportunities() -> PredictResult:
    config = get_ml_config()
    if not config.enabled:
        raise RuntimeError("ML layer is disabled in config.")

    model_run = _latest_model_run()
    artifact_path = ROOT / str(model_run["artifact_path"])
    model = joblib.load(artifact_path)
    records = fetch_current_feature_records()
    x_values = np.asarray(records_as_matrix(records), dtype=float)
    ml_scores = _positive_scores(model, x_values)
    rows: list[dict[str, Any]] = []
    counts = {"comprar_teste": 0, "revisar": 0, "ignorar": 0}
    disagreements = 0

    for record, ml_score_raw in zip(records, ml_scores):
        ml_score = float(ml_score_raw)
        hybrid = combine_decisions(
            heuristic_score=record.heuristic_score,
            heuristic_decision=record.heuristic_decision,
            ml_score=ml_score,
            config=config,
        )
        explanation = explain_prediction(
            heuristic_decision=record.heuristic_decision,
            ml_decision=hybrid.ml_decision,
            final_decision=hybrid.final_decision,
            features=record.features,
        )
        if record.heuristic_decision != hybrid.ml_decision:
            disagreements += 1
        counts[hybrid.final_decision] += 1
        rows.append(
            {
                "model_run_id": model_run["model_run_id"],
                "decision_run_id": record.decision_run_id,
                "supplier_product_id": record.supplier_product_id,
                "supplier_raw_id": record.supplier_raw_id,
                "supplier_slug": record.supplier_slug,
                "product_title": record.product_title,
                "heuristic_score": round(record.heuristic_score, 4),
                "heuristic_decision": record.heuristic_decision,
                "ml_score": round(ml_score, 4),
                "ml_prediction": 1 if ml_score >= config.review_threshold else 0,
                "ml_decision": hybrid.ml_decision,
                "final_score": hybrid.final_score,
                "final_decision": hybrid.final_decision,
                "score_difference": round(ml_score - record.heuristic_score, 4),
                "decision_source": hybrid.decision_source,
                "confidence_level": hybrid.confidence_level,
                "explanation": explanation,
                "model_version": model_run["model_version"],
                "features_snapshot": record.features,
            }
        )

    upsert_sql = text(
        """
        INSERT INTO gold.ml_opportunity_scores (
            model_run_id,
            decision_run_id,
            supplier_product_id,
            supplier_raw_id,
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
            features_snapshot
        )
        VALUES (
            CAST(:model_run_id AS uuid),
            CAST(:decision_run_id AS uuid),
            CAST(:supplier_product_id AS uuid),
            CAST(:supplier_raw_id AS uuid),
            :supplier_slug,
            :product_title,
            :heuristic_score,
            :heuristic_decision,
            :ml_score,
            :ml_prediction,
            :ml_decision,
            :final_score,
            :final_decision,
            :score_difference,
            :decision_source,
            :confidence_level,
            :explanation,
            :model_version,
            CAST(:features_snapshot AS jsonb)
        )
        ON CONFLICT (model_run_id, supplier_product_id) DO UPDATE
        SET
            decision_run_id = EXCLUDED.decision_run_id,
            supplier_raw_id = EXCLUDED.supplier_raw_id,
            supplier_slug = EXCLUDED.supplier_slug,
            product_title = EXCLUDED.product_title,
            heuristic_score = EXCLUDED.heuristic_score,
            heuristic_decision = EXCLUDED.heuristic_decision,
            ml_score = EXCLUDED.ml_score,
            ml_prediction = EXCLUDED.ml_prediction,
            ml_decision = EXCLUDED.ml_decision,
            final_score = EXCLUDED.final_score,
            final_decision = EXCLUDED.final_decision,
            score_difference = EXCLUDED.score_difference,
            decision_source = EXCLUDED.decision_source,
            confidence_level = EXCLUDED.confidence_level,
            explanation = EXCLUDED.explanation,
            features_snapshot = EXCLUDED.features_snapshot,
            scored_at = NOW()
        """
    )
    with engine.begin() as connection:
        for row in rows:
            connection.execute(
                upsert_sql,
                {
                    **row,
                    "features_snapshot": to_json_text(row["features_snapshot"]),
                },
            )

    comparison_path = config.report_dir / "heuristic_vs_ml_comparison.csv"
    _write_comparison(comparison_path, rows)
    summary_path = config.report_dir / "prediction_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "model_run_id": model_run["model_run_id"],
                "model_version": model_run["model_version"],
                "scored": len(rows),
                "recommendations": counts,
                "heuristic_ml_disagreements": disagreements,
                "guardrail": "ML cannot autonomously promote a product to comprar_teste.",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return PredictResult(
        model_run_id=str(model_run["model_run_id"]),
        model_version=str(model_run["model_version"]),
        scored=len(rows),
        comprar_teste=counts["comprar_teste"],
        revisar=counts["revisar"],
        ignorar=counts["ignorar"],
        disagreements=disagreements,
        comparison_path=str(comparison_path),
    )
