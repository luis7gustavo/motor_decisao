from __future__ import annotations

from typing import Any

from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


def _ranking_metrics(*, y_true, y_score, cutoffs: tuple[int, ...] = (10, 25, 50)) -> dict[str, float]:
    ranked_indexes = sorted(
        range(len(y_score)),
        key=lambda index: float(y_score[index]),
        reverse=True,
    )
    positives = int(sum(int(value) for value in y_true))
    metrics: dict[str, float] = {}
    for cutoff in cutoffs:
        selected_indexes = ranked_indexes[: min(cutoff, len(ranked_indexes))]
        selected_positives = sum(int(y_true[index]) for index in selected_indexes)
        metrics[f"precision_at_{cutoff}"] = (
            float(selected_positives / len(selected_indexes)) if selected_indexes else 0.0
        )
        metrics[f"recall_at_{cutoff}"] = (
            float(selected_positives / positives) if positives else 0.0
        )
    return metrics


def evaluate_binary_classifier(
    *,
    y_true,
    y_pred,
    y_score,
) -> tuple[dict[str, Any], str]:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "average_precision": float(average_precision_score(y_true, y_score)),
        "roc_auc": float(roc_auc_score(y_true, y_score)),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
        **_ranking_metrics(y_true=y_true, y_score=y_score),
    }
    report = classification_report(y_true, y_pred, digits=4, zero_division=0)
    return metrics, report
