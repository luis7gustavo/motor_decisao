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
from sklearn.base import clone
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from xgboost import XGBClassifier

from motor_decisao.app.core.database import engine
from motor_decisao.pipelines.common.serialization import to_json_text
from motor_decisao.pipelines.ml.augment_training_data import (
    SyntheticTrainingRows,
    generate_synthetic_training_rows,
    write_augmented_training_csv,
)
from motor_decisao.pipelines.ml.build_features import FEATURE_NAMES, records_as_matrix
from motor_decisao.pipelines.ml.build_training_dataset import build_training_dataset
from motor_decisao.pipelines.ml.config import ROOT, MlConfig, get_ml_config
from motor_decisao.pipelines.ml.evaluate_model import evaluate_binary_classifier


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
    augmentation_report_path: str
    model_card_path: str
    synthetic_rows: int
    used_synthetic_training: bool
    metrics: dict[str, Any]


def _positive_class_weight(y_values) -> float:
    positive_rows = int(np.sum(np.asarray(y_values) == 1))
    negative_rows = int(np.sum(np.asarray(y_values) == 0))
    if positive_rows <= 0:
        raise RuntimeError("ML dataset needs positive proxy rows.")
    return max(float(negative_rows) / float(positive_rows), 1.0)


def _effective_cv_splits(y_values, requested_splits: int) -> int:
    positive_rows = int(np.sum(np.asarray(y_values) == 1))
    negative_rows = int(np.sum(np.asarray(y_values) == 0))
    splits = min(requested_splits, positive_rows, negative_rows)
    if splits < 2:
        raise RuntimeError("ML dataset needs at least 2 rows in each class for cross-validation.")
    return splits


def _build_candidates(random_state: int, *, positive_class_weight: float) -> dict[str, Pipeline]:
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
        "xgboost": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "classifier",
                    XGBClassifier(
                        n_estimators=300,
                        max_depth=5,
                        learning_rate=0.05,
                        subsample=0.85,
                        colsample_bytree=0.85,
                        min_child_weight=2.0,
                        reg_alpha=0.10,
                        reg_lambda=2.0,
                        scale_pos_weight=positive_class_weight,
                        objective="binary:logistic",
                        eval_metric="aucpr",
                        tree_method="hist",
                        random_state=random_state,
                        n_jobs=-1,
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
    model.fit(x_train, y_train, classifier__sample_weight=sample_weights)


def _fit_values_for_variant(
    *,
    x_values,
    y_values,
    sample_weights,
    training_variant: str,
    config: MlConfig,
    random_state: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, SyntheticTrainingRows]:
    synthetic = generate_synthetic_training_rows(
        x_values=x_values,
        y_values=y_values,
        sample_weights=sample_weights,
        random_state=random_state,
        synthetic_multiplier=(
            config.augmentation_synthetic_multiplier
            if training_variant == "real_plus_synthetic"
            else 0
        ),
        max_synthetic_rows=config.augmentation_max_synthetic_rows,
        synthetic_sample_weight=config.augmentation_synthetic_sample_weight,
        noise_level=config.augmentation_noise_level,
    )
    if synthetic.rows == 0:
        return x_values, y_values, sample_weights, synthetic
    return (
        np.vstack([x_values, synthetic.x_values]),
        np.concatenate([y_values, synthetic.y_values]),
        np.concatenate([sample_weights, synthetic.sample_weights]),
        synthetic,
    )


def _cross_validate_candidate(
    *,
    model_name: str,
    model: Pipeline,
    x_values,
    y_values,
    sample_weights,
    splits: int,
    random_state: int,
    training_variant: str,
    config: MlConfig,
) -> tuple[dict[str, Any], str, int]:
    oof_scores = np.zeros(len(y_values), dtype=float)
    cv = StratifiedKFold(n_splits=splits, shuffle=True, random_state=random_state)
    synthetic_rows = 0
    for fold_index, (train_indexes, test_indexes) in enumerate(cv.split(x_values, y_values)):
        fold_model = clone(model)
        x_fit, y_fit, weights_fit, synthetic = _fit_values_for_variant(
            x_values=x_values[train_indexes],
            y_values=y_values[train_indexes],
            sample_weights=sample_weights[train_indexes],
            training_variant=training_variant,
            config=config,
            random_state=random_state + fold_index,
        )
        synthetic_rows += synthetic.rows
        _fit_candidate(
            model_name=model_name,
            model=fold_model,
            x_train=x_fit,
            y_train=y_fit,
            sample_weights=weights_fit,
        )
        oof_scores[test_indexes] = _positive_scores(fold_model, x_values[test_indexes])
    oof_predictions = (oof_scores >= 0.50).astype(int)
    metrics, report = evaluate_binary_classifier(
        y_true=y_values,
        y_pred=oof_predictions,
        y_score=oof_scores,
    )
    return metrics, report, synthetic_rows


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


def _write_model_card(
    path: Path,
    *,
    model_version: str,
    selected_model: str,
    selected_training_variant: str,
    training_rows: int,
    positive_rows: int,
    synthetic_rows: int,
    selected_metrics: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""# Model Card - SILLO Motor ML

## Objetivo

Ranquear oportunidades de compra para revenda como apoio auditavel ao motor
heuristico `heuristic_v2_confidence_guard`.

## Versao e Modelo

- Versao: `{model_version}`
- Modelo selecionado: `{selected_model}`
- Variante de treino: `{selected_training_variant}`
- Linhas reais: {training_rows}
- Rotulos proxy positivos: {positive_rows}
- Linhas sinteticas usadas no treino final: {synthetic_rows}

## Target

Classificacao binaria proxy:

- `1`: `revisar` ou `comprar_teste`;
- `0`: `ignorar`.

Os rotulos atuais imitam a heuristica. Eles ainda nao representam vendas,
margem realizada ou tempo de giro.

## Features

O modelo usa sinais intermediarios de preco, margem, demanda, match,
diversidade de fontes e risco. O score final da heuristica foi excluido das
features para evitar vazamento direto da resposta do professor.

## Dados Sinteticos

O augmentation gera perturbacoes pequenas apenas a partir de oportunidades
positivas do treino. Cada linha e marcada com `is_synthetic=true`, recebe peso
reduzido e respeita limites de preco, contagens, percentuais, scores e flags.
Os sinteticos nunca entram na avaliacao principal.

Como o snapshot atual nao contem todos os campos brutos necessarios para
reexecutar integralmente a heuristica, cada sintetico herda o rotulo proxy da
linha real que o originou. Esta e uma limitacao consciente.

## Metricas em Dados Reais

- Average precision: {selected_metrics["average_precision"]:.6f}
- Precision: {selected_metrics["precision"]:.6f}
- Recall: {selected_metrics["recall"]:.6f}
- F1: {selected_metrics["f1"]:.6f}
- ROC-AUC: {selected_metrics["roc_auc"]:.6f}
- Precision@10: {selected_metrics["precision_at_10"]:.6f}
- Recall@50: {selected_metrics["recall_at_50"]:.6f}

## Uso Recomendado

Usar como apoio ao ranking e como sinal de revisao. A trava hibrida permanece:
o ML nao promove sozinho um produto para `comprar_teste`.

## Quando Nao Confiar

- ausencia de fontes de mercado suficientes;
- match fraco entre fornecedor e mercado;
- dados antigos ou coleta incompleta;
- categorias ainda pouco representadas;
- decisao de compra sem revisao humana;
- interpretacao das metricas proxy como retorno comercial comprovado.
""",
        encoding="utf-8",
    )


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
    x_train, x_test, y_train, y_test, weight_train, _weight_test = train_test_split(
        x_values,
        y_values,
        weights,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=y_values,
    )

    positive_class_weight = _positive_class_weight(y_values)
    cv_splits = _effective_cv_splits(y_values, config.cv_splits)
    candidates = _build_candidates(
        config.random_state,
        positive_class_weight=positive_class_weight,
    )
    training_variants = ["real_only"]
    if config.augmentation_enabled:
        training_variants.append("real_plus_synthetic")
    candidate_metrics: dict[str, dict[str, dict[str, Any]]] = {}
    candidate_reports: dict[str, str] = {}
    for model_name, model in candidates.items():
        candidate_metrics[model_name] = {}
        for training_variant in training_variants:
            metrics, report, synthetic_rows_cv = _cross_validate_candidate(
                model_name=model_name,
                model=model,
                x_values=x_values,
                y_values=y_values,
                sample_weights=weights,
                splits=cv_splits,
                random_state=config.random_state,
                training_variant=training_variant,
                config=config,
            )
            metrics["synthetic_rows_across_cv_folds"] = synthetic_rows_cv
            candidate_metrics[model_name][training_variant] = metrics
            candidate_reports[f"{model_name} - {training_variant}"] = report

    eligible_experiments: list[tuple[str, str]] = []
    for model_name, metrics_by_variant in candidate_metrics.items():
        eligible_experiments.append((model_name, "real_only"))
        if "real_plus_synthetic" not in metrics_by_variant:
            continue
        real_ap = metrics_by_variant["real_only"]["average_precision"]
        synthetic_ap = metrics_by_variant["real_plus_synthetic"]["average_precision"]
        if synthetic_ap >= real_ap + config.augmentation_min_average_precision_gain:
            eligible_experiments.append((model_name, "real_plus_synthetic"))

    if config.model_type == "best":
        selected_model, selected_training_variant = max(
            eligible_experiments,
            key=lambda experiment: (
                candidate_metrics[experiment[0]][experiment[1]]["average_precision"],
                candidate_metrics[experiment[0]][experiment[1]]["f1"],
            ),
        )
    elif config.model_type in candidates:
        selected_model = config.model_type
        selected_training_variant = max(
            [
                experiment
                for experiment in eligible_experiments
                if experiment[0] == selected_model
            ],
            key=lambda experiment: (
                candidate_metrics[experiment[0]][experiment[1]]["average_precision"],
                candidate_metrics[experiment[0]][experiment[1]]["f1"],
            ),
        )[1]
    else:
        raise RuntimeError(f"Unknown ML model_type: {config.model_type}")

    x_importance, y_importance, weight_importance, _synthetic_importance = (
        _fit_values_for_variant(
            x_values=x_train,
            y_values=y_train,
            sample_weights=weight_train,
            training_variant=selected_training_variant,
            config=config,
            random_state=config.random_state,
        )
    )
    importance_model = clone(candidates[selected_model])
    _fit_candidate(
        model_name=selected_model,
        model=importance_model,
        x_train=x_importance,
        y_train=y_importance,
        sample_weights=weight_importance,
    )
    _x_augmented, _y_augmented, _weights_augmented, available_full_synthetic = (
        _fit_values_for_variant(
            x_values=x_values,
            y_values=y_values,
            sample_weights=weights,
            training_variant=(
                "real_plus_synthetic" if config.augmentation_enabled else "real_only"
            ),
            config=config,
            random_state=config.random_state,
        )
    )
    x_final, y_final, weights_final, used_full_synthetic = _fit_values_for_variant(
        x_values=x_values,
        y_values=y_values,
        sample_weights=weights,
        training_variant=selected_training_variant,
        config=config,
        random_state=config.random_state,
    )
    model = clone(candidates[selected_model])
    _fit_candidate(
        model_name=selected_model,
        model=model,
        x_train=x_final,
        y_train=y_final,
        sample_weights=weights_final,
    )
    now = datetime.now(timezone.utc)
    model_version = f"ml_proxy_v3_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    model_path = config.artifact_dir / "models" / f"{model_version}.joblib"
    metrics_path = config.report_dir / "metrics.json"
    report_path = config.report_dir / "classification_report.txt"
    feature_importance_path = config.report_dir / "feature_importance.csv"
    augmentation_report_path = config.report_dir / "augmentation_report.json"
    model_card_path = config.report_dir / "model_card.md"
    augmented_dataset_path = config.dataset_dir / "training_dataset_augmented.csv"
    metadata_path = config.artifact_dir / "metadata" / f"{model_version}.json"
    feature_list_path = config.artifact_dir / "feature_lists" / f"{model_version}.json"

    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    feature_list_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)

    importances = _model_importances(
        model=importance_model,
        x_test=x_test,
        y_test=y_test,
        random_state=config.random_state,
    )
    _write_feature_importance(feature_importance_path, importances)
    write_augmented_training_csv(
        augmented_dataset_path,
        x_real=x_values,
        y_real=y_values,
        weights_real=weights,
        synthetic=available_full_synthetic,
    )
    selected_metrics = candidate_metrics[selected_model][selected_training_variant]
    augmentation_impact = {
        model_name: {
            "real_only_average_precision": metrics_by_variant["real_only"]["average_precision"],
            "real_plus_synthetic_average_precision": (
                metrics_by_variant.get("real_plus_synthetic", {}).get("average_precision")
            ),
            "average_precision_delta": (
                metrics_by_variant.get("real_plus_synthetic", {}).get("average_precision", 0.0)
                - metrics_by_variant["real_only"]["average_precision"]
                if "real_plus_synthetic" in metrics_by_variant
                else None
            ),
        }
        for model_name, metrics_by_variant in candidate_metrics.items()
    }
    augmentation_payload = {
        "enabled": config.augmentation_enabled,
        "method": "controlled_positive_proxy_perturbation",
        "evaluation_rule": "Synthetic rows are generated inside each training fold only. Validation folds contain real rows only.",
        "label_rule": "Synthetic rows inherit the proxy label from their real positive source row because the feature snapshot is insufficient to rerun the full heuristic.",
        "synthetic_multiplier": config.augmentation_synthetic_multiplier,
        "max_synthetic_rows": config.augmentation_max_synthetic_rows,
        "synthetic_sample_weight": config.augmentation_synthetic_sample_weight,
        "noise_level": config.augmentation_noise_level,
        "min_average_precision_gain": config.augmentation_min_average_precision_gain,
        "selected_model": selected_model,
        "selected_training_variant": selected_training_variant,
        "used_synthetic_training": selected_training_variant == "real_plus_synthetic",
        "real_rows": dataset.rows,
        "synthetic_rows_available_for_final_training": available_full_synthetic.rows,
        "synthetic_rows_final_training": used_full_synthetic.rows,
        "impact_by_model": augmentation_impact,
    }
    augmentation_report_path.write_text(
        json.dumps(augmentation_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_model_card(
        model_card_path,
        model_version=model_version,
        selected_model=selected_model,
        selected_training_variant=selected_training_variant,
        training_rows=dataset.rows,
        positive_rows=dataset.positive_rows,
        synthetic_rows=used_full_synthetic.rows,
        selected_metrics=selected_metrics,
    )
    metrics_payload = {
        "model_version": model_version,
        "selected_model": selected_model,
        "teacher": "heuristic_v2_confidence_guard",
        "label_type": "proxy_binary",
        "feature_mode": "independent_intermediate_features",
        "training_rows": dataset.rows,
        "positive_rows": dataset.positive_rows,
        "evaluation_method": "stratified_out_of_fold_cross_validation",
        "evaluation_rows": dataset.rows,
        "cv_splits": cv_splits,
        "positive_class_weight": positive_class_weight,
        "feature_importance_holdout_rows": int(len(y_test)),
        "selected_training_variant": selected_training_variant,
        "used_synthetic_training": selected_training_variant == "real_plus_synthetic",
        "synthetic_rows_final_training": used_full_synthetic.rows,
        "candidate_metrics": candidate_metrics,
        "selected_metrics": selected_metrics,
        "limitations": [
            "Labels imitate heuristic decisions and are not real sales outcomes.",
            "The positive proxy class merges revisar and comprar_teste because strong buy labels are scarce.",
            "The heuristic remains the decision guardrail while real sales labels are unavailable.",
            "Synthetic rows inherit labels from real positive source rows because the snapshot cannot rerun the full heuristic.",
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
        "augmentation_report_path": _relative_path(augmentation_report_path),
        "model_card_path": _relative_path(model_card_path),
        "augmented_dataset_path": _relative_path(augmented_dataset_path),
        "model_candidates": sorted(candidates),
        "selected_model": selected_model,
        "selected_training_variant": selected_training_variant,
        "used_synthetic_training": selected_training_variant == "real_plus_synthetic",
        "synthetic_rows_final_training": used_full_synthetic.rows,
        "evaluation_method": "stratified_out_of_fold_cross_validation",
        "cv_splits": cv_splits,
        "positive_class_weight": positive_class_weight,
        "proxy_label": "1 revisar/comprar_teste; 0 ignorar",
        "feature_mode": "independent_intermediate_features",
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
        augmentation_report_path=str(augmentation_report_path),
        model_card_path=str(model_card_path),
        synthetic_rows=used_full_synthetic.rows,
        used_synthetic_training=selected_training_variant == "real_plus_synthetic",
        metrics=metrics_payload,
    )
