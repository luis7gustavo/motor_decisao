from pathlib import Path

from pipelines.ml.build_features import build_feature_snapshot, proxy_class, proxy_label
from pipelines.ml.config import MlConfig
from pipelines.ml.hybrid import combine_decisions


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
