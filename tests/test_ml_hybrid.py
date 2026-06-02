from pathlib import Path

import numpy as np

from pipelines.ml.augment_training_data import (
    FEATURE_INDEX,
    generate_synthetic_training_rows,
)
from pipelines.ml.build_features import build_feature_snapshot, proxy_class, proxy_label
from pipelines.ml.config import MlConfig
from pipelines.ml.evaluate_model import evaluate_binary_classifier
from pipelines.ml.hybrid import combine_decisions
from pipelines.ml.train_model import _effective_cv_splits, _positive_class_weight


def _config() -> MlConfig:
    return MlConfig(
        enabled=True,
        use_heuristic_as_teacher=True,
        use_hybrid_score=True,
        heuristic_weight=0.70,
        ml_weight=0.30,
        opportunity_threshold=0.65,
        review_threshold=0.45,
        min_training_rows=50,
        random_state=42,
        model_type="best",
        test_size=0.25,
        cv_splits=5,
        augmentation_enabled=True,
        augmentation_synthetic_multiplier=3,
        augmentation_max_synthetic_rows=5000,
        augmentation_synthetic_sample_weight=0.30,
        augmentation_noise_level=0.08,
        augmentation_min_average_precision_gain=0.001,
        artifact_dir=Path("artifacts/ml"),
        report_dir=Path("reports/ml"),
        dataset_dir=Path("data_processed/ml"),
    )


def test_proxy_labels_preserve_three_class_audit_and_binary_training_target() -> None:
    assert proxy_class("ignorar") == 0
    assert proxy_class("revisar") == 1
    assert proxy_class("comprar_teste") == 2
    assert proxy_label("ignorar") == 0
    assert proxy_label("revisar") == 1
    assert proxy_label("comprar_teste") == 1


def test_feature_snapshot_reuses_heuristic_signals_without_decision_score_leakage() -> None:
    snapshot = build_feature_snapshot(
        {
            "supplier_price": 100,
            "estimated_market_price": 200,
            "estimated_net_profit": 36,
            "net_margin_pct": 0.18,
            "total_fee_pct": 0.32,
            "market_offer_count": 8,
            "market_source_count": 3,
            "price_history_count": 2,
            "mercado_livre_count": 4,
            "demand_score": 70,
            "match_confidence": 82,
            "decision_score": 99,
            "risk_flags": ["match_revisar"],
        }
    )
    assert snapshot["has_market_price"] == 1
    assert snapshot["has_positive_margin"] == 1
    assert snapshot["has_multiple_market_sources"] == 1
    assert snapshot["risk_flag_count"] == 1
    assert "decision_score" not in snapshot


def test_ml_cannot_promote_ignored_product_directly_to_buy() -> None:
    decision = combine_decisions(
        heuristic_score=0.40,
        heuristic_decision="ignorar",
        ml_score=0.99,
        config=_config(),
    )
    assert decision.final_decision == "revisar"


def test_buy_requires_heuristic_buy_and_ml_support() -> None:
    confirmed = combine_decisions(
        heuristic_score=0.82,
        heuristic_decision="comprar_teste",
        ml_score=0.80,
        config=_config(),
    )
    challenged = combine_decisions(
        heuristic_score=0.82,
        heuristic_decision="comprar_teste",
        ml_score=0.10,
        config=_config(),
    )
    assert confirmed.final_decision == "comprar_teste"
    assert challenged.final_decision == "revisar"


def test_positive_class_weight_uses_negative_to_positive_ratio() -> None:
    assert _positive_class_weight([0, 0, 0, 1]) == 3.0


def test_cross_validation_splits_are_limited_by_rare_class() -> None:
    assert _effective_cv_splits([0, 0, 0, 1, 1], requested_splits=5) == 2


def test_synthetic_rows_are_positive_weighted_and_business_bounded() -> None:
    snapshot = build_feature_snapshot(
        {
            "supplier_price": 100,
            "estimated_market_price": 200,
            "estimated_net_profit": 40,
            "net_margin_pct": 0.20,
            "total_fee_pct": 0.32,
            "market_offer_count": 8,
            "market_source_count": 3,
            "price_history_count": 2,
            "mercado_livre_count": 4,
            "demand_score": 70,
            "match_confidence": 82,
            "risk_flags": ["match_revisar"],
        }
    )
    x_values = np.asarray(
        [
            list(snapshot.values()),
            list(snapshot.values()),
        ],
        dtype=float,
    )
    synthetic = generate_synthetic_training_rows(
        x_values=x_values,
        y_values=np.asarray([0, 1], dtype=int),
        sample_weights=np.asarray([0.50, 0.80], dtype=float),
        random_state=42,
        synthetic_multiplier=3,
        max_synthetic_rows=10,
        synthetic_sample_weight=0.30,
        noise_level=0.08,
    )

    assert synthetic.rows == 3
    assert set(synthetic.source_indexes) == {1}
    assert set(synthetic.y_values) == {1}
    assert np.allclose(synthetic.sample_weights, 0.24)
    assert np.all(synthetic.x_values[:, FEATURE_INDEX["supplier_price"]] >= 0)
    assert np.all(synthetic.x_values[:, FEATURE_INDEX["estimated_market_price"]] >= 0)
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["total_fee_pct"]] >= 0))
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["total_fee_pct"]] <= 1))
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["demand_score"]] >= 0))
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["demand_score"]] <= 100))
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["match_confidence"]] >= 0))
    assert np.all((synthetic.x_values[:, FEATURE_INDEX["match_confidence"]] <= 100))
    assert set(synthetic.x_values[:, FEATURE_INDEX["has_market_price"]]) <= {0.0, 1.0}
    assert set(synthetic.x_values[:, FEATURE_INDEX["has_positive_margin"]]) <= {0.0, 1.0}


def test_binary_evaluation_includes_ranking_metrics() -> None:
    metrics, _report = evaluate_binary_classifier(
        y_true=np.asarray([1, 0, 1, 0]),
        y_pred=np.asarray([1, 0, 1, 0]),
        y_score=np.asarray([0.90, 0.10, 0.80, 0.20]),
    )
    assert metrics["precision_at_10"] == 0.50
    assert metrics["recall_at_50"] == 1.0
