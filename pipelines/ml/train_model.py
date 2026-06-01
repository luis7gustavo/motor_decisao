from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from app.core.database import engine
from pipelines.common.serialization import to_json_text
from pipelines.ml.build_features import FEATURE_NAMES, records_as_matrix
from pipelines.ml.build_training_dataset import build_training_dataset
from pipelines.ml.config import ROOT, get_ml_config
from pipelines.ml.evaluate_model import evaluate_binary_classifier


@dataclass(frozen=True)
class TrainModelResult:
    model_run_id: str
    model_version: str
    selected_model: str
    artifact_path: str
    training_rows: int
    positive_rows: int
    metrics_path: str
    report_path: str
    feature_importance_path: str
    metrics: dict[str, Any]


def _build_candidates(random_state: int) -> dict[str, Pipeline]:
    return {
        "logistic_regression": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "classifier",
                    LogisticRegression(
                        class_weight="balanced",
                        max_iter=1500,
                        random_state=random_state,
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "classifier",
                    RandomForestClassifier(
                        n_estimators=300,
                        max_depth=10,
                        min_samples_leaf=3,
                        class_weight="balanced",
                        random_state=random_state,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
        "hist_gradient_boosting": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "classifier",
                    HistGradientBoostingClassifier(
                        max_iter=180,
                        max_depth=7,
                        learning_rate=0.08,
                        random_state=random_state,
                    ),
                ),
            ]
        ),
    }


def _positive_scores(model: Pipeline, x_values) -> np.ndarray:
    probabilities = model.predict_proba(x_values)
    classes = list(model.classes_)
    return probabilities[:, classes.index(1)]


def _fit_candidate(
    *,
    model_name: str,
    model: Pipeline,
    x_train,
    y_train,
    sample_weights,
) -> None:
    if model_name == "hist_gradient_boosting":
        model.fit(x_train, y_train, classifier__sample_weight=sample_weights)
    else:
        model.fit(x_train, y_train)


def _model_importances(
    *,
    model: Pipeline,
    x_test,
    y_test,
    random_state: int,
) -> list[float]:
    classifier = model.named_steps["classifier"]
    if hasattr(classifier, "coef_"):
        return np.abs(classifier.coef_[0]).astype(float).tolist()
    if hasattr(classifier, "feature_importances_"):
        return classifier.feature_importances_.astype(float).tolist()
    result = permutation_importance(
        model,
        x_test,
        y_test,
        scoring="average_precision",
        n_repeats=5,
        random_state=random_state,
        n_jobs=-1,
    )
    return result.importances_mean.astype(float).tolist()


def _write_feature_importance(path: Path, importances: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(zip(FEATURE_NAMES, importances), key=lambda pair: pair[1], reverse=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerow(["feature", "importance"])
        writer.writerows(rows)


def _relative_path(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT.resolve())).replace("\\", "/")


def _insert_model_run(
    *,
    model_version: str,
    model_type: str,
    training_rows: int,
    positive_rows: int,
    metrics: dict[str, Any],
    metadata: dict[str, Any],
    artifact_path: str,
) -> str:
    sql = text(
        """
        INSERT INTO gold.ml_model_runs (
            model_version,
            model_type,
            status,
            training_rows,
            positive_rows,
            feature_names,
            metrics,
            metadata,
            artifact_path,
            trained_at
        )
        VALUES (
            :model_version,
            :model_type,
            'success',
            :training_rows,
            :positive_rows,
            CAST(:feature_names AS jsonb),
            CAST(:metrics AS jsonb),
            CAST(:metadata AS jsonb),
            :artifact_path,
            NOW()
        )
        RETURNING id::text
        """
    )
    with engine.begin() as connection:
        return str(
            connection.execute(
                sql,
                {
                    "model_version": model_version,
                    "model_type": model_type,
                    "training_rows": training_rows,
                    "positive_rows": positive_rows,
                    "feature_names": to_json_text(list(FEATURE_NAMES)),
                    "metrics": to_json_text(metrics),
                    "metadata": to_json_text(metadata),
                    "artifact_path": artifact_path,
                },
            ).scalar_one()
        )


def train_models() -> TrainModelResult:
    config = get_ml_config()
    if not config.enabled:
        raise RuntimeError("ML layer is disabled in config.")
    if not config.use_heuristic_as_teacher:
        raise RuntimeError("Initial ML layer requires heuristic proxy labels.")

    dataset = build_training_dataset()
    x_values = np.asarray(records_as_matrix(dataset.records), dtype=float)
    y_values = np.asarray([record.opportunity_label for record in dataset.records], dtype=int)
    weights = np.asarray([record.sample_weight for record in dataset.records], dtype=float)
    (
        x_train,
        x_test,
        y_train,
        y_test,
        weight_train,
        _weight_test,
    ) = train_test_split(
        x_values,
        y_values,
        weights,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=y_values,
    )

    candidates = _build_candidates(config.random_state)
    candidate_metrics: dict[str, dict[str, Any]] = {}
    candidate_reports: dict[str, str] = {}
    for model_name, model in candidates.items():
        _fit_candidate(
            model_name=model_name,
            model=model,
            x_train=x_train,
            y_train=y_train,
            sample_weights=weight_train,
        )
        y_score = _positive_scores(model, x_test)
        y_pred = (y_score >= 0.50).astype(int)
        metrics, report = evaluate_binary_classifier(
            y_true=y_test,
            y_pred=y_pred,
            y_score=y_score,
        )
        candidate_metrics[model_name] = metrics
        candidate_reports[model_name] = report

    if config.model_type == "best":
        selected_model = max(
            candidate_metrics,
            key=lambda name: (
                candidate_metrics[name]["average_precision"],
                candidate_metrics[name]["f1"],
            ),
        )
    elif config.model_type in candidates:
        selected_model = config.model_type
    else:
        raise RuntimeError(f"Unknown ML model_type: {config.model_type}")

    model = candidates[selected_model]
    now = datetime.now(timezone.utc)
    model_version = f"ml_proxy_v1_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    model_path = config.artifact_dir / "models" / f"{model_version}.joblib"
    metrics_path = config.report_dir / "metrics.json"
    report_path = config.report_dir / "classification_report.txt"
    feature_importance_path = config.report_dir / "feature_importance.csv"
    metadata_path = config.artifact_dir / "metadata" / f"{model_version}.json"
    feature_list_path = config.artifact_dir / "feature_lists" / f"{model_version}.json"

    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    feature_list_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)

    importances = _model_importances(
        model=model,
        x_test=x_test,
        y_test=y_test,
        random_state=config.random_state,
    )
    _write_feature_importance(feature_importance_path, importances)
    metrics_payload = {
        "model_version": model_version,
        "selected_model": selected_model,
        "teacher": "heuristic_v2_confidence_guard",
        "label_type": "proxy_binary",
        "training_rows": dataset.rows,
        "positive_rows": dataset.positive_rows,
        "test_rows": int(len(y_test)),
        "candidate_metrics": candidate_metrics,
        "selected_metrics": candidate_metrics[selected_model],
        "limitations": [
            "Labels imitate heuristic decisions and are not real sales outcomes.",
            "The positive proxy class merges revisar and comprar_teste because strong buy labels are scarce.",
            "The heuristic remains the decision guardrail while real sales labels are unavailable.",
        ],
    }
    metrics_path.write_text(
        json.dumps(metrics_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path.write_text(
        "\n\n".join(
            f"## {name}\n{candidate_reports[name]}" for name in sorted(candidate_reports)
        ),
        encoding="utf-8",
    )
    feature_list_path.write_text(
        json.dumps(list(FEATURE_NAMES), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    metadata = {
        "dataset_csv": _relative_path(dataset.csv_path),
        "dataset_metadata": _relative_path(dataset.metadata_path),
        "metrics_path": _relative_path(metrics_path),
        "classification_report_path": _relative_path(report_path),
        "feature_importance_path": _relative_path(feature_importance_path),
        "model_candidates": sorted(candidates),
        "selected_model": selected_model,
        "proxy_label": "1 revisar/comprar_teste; 0 ignorar",
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    artifact_path = _relative_path(model_path)
    model_run_id = _insert_model_run(
        model_version=model_version,
        model_type=selected_model,
        training_rows=dataset.rows,
        positive_rows=dataset.positive_rows,
        metrics=metrics_payload,
        metadata=metadata,
        artifact_path=artifact_path,
    )
    return TrainModelResult(
        model_run_id=model_run_id,
        model_version=model_version,
        selected_model=selected_model,
        artifact_path=artifact_path,
        training_rows=dataset.rows,
        positive_rows=dataset.positive_rows,
        metrics_path=str(metrics_path),
        report_path=str(report_path),
        feature_importance_path=str(feature_importance_path),
        metrics=metrics_payload,
    )
