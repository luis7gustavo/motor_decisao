from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from motor_decisao.app.core.settings import get_settings
from motor_decisao.paths import PROJECT_ROOT


ROOT = PROJECT_ROOT


@dataclass(frozen=True)
class MlConfig:
    enabled: bool
    use_heuristic_as_teacher: bool
    use_hybrid_score: bool
    heuristic_weight: float
    ml_weight: float
    opportunity_threshold: float
    review_threshold: float
    min_training_rows: int
    random_state: int
    model_type: str
    test_size: float
    cv_splits: int
    augmentation_enabled: bool
    augmentation_synthetic_multiplier: int
    augmentation_max_synthetic_rows: int
    augmentation_synthetic_sample_weight: float
    augmentation_noise_level: float
    augmentation_min_average_precision_gain: float
    artifact_dir: Path
    report_dir: Path
    dataset_dir: Path


def _bounded_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    number = float(value if value is not None else default)
    return max(minimum, min(maximum, number))


def get_ml_config() -> MlConfig:
    project_config = get_settings().load_project_config()
    config = project_config.get("ml") or {}
    heuristic_weight = _bounded_float(
        config.get("heuristic_weight"),
        default=0.70,
        minimum=0.0,
        maximum=1.0,
    )
    ml_weight = _bounded_float(
        config.get("ml_weight"),
        default=0.30,
        minimum=0.0,
        maximum=1.0,
    )
    weight_total = heuristic_weight + ml_weight
    if weight_total <= 0:
        heuristic_weight, ml_weight = 0.70, 0.30
    else:
        heuristic_weight /= weight_total
        ml_weight /= weight_total

    augmentation = config.get("augmentation") or {}
    return MlConfig(
        enabled=bool(config.get("enabled", True)),
        use_heuristic_as_teacher=bool(config.get("use_heuristic_as_teacher", True)),
        use_hybrid_score=bool(config.get("use_hybrid_score", True)),
        heuristic_weight=heuristic_weight,
        ml_weight=ml_weight,
        opportunity_threshold=_bounded_float(
            config.get("opportunity_threshold"),
            default=0.65,
            minimum=0.0,
            maximum=1.0,
        ),
        review_threshold=_bounded_float(
            config.get("review_threshold"),
            default=0.45,
            minimum=0.0,
            maximum=1.0,
        ),
        min_training_rows=max(int(config.get("min_training_rows", 50)), 10),
        random_state=int(config.get("random_state", 42)),
        model_type=str(config.get("model_type", "best")).strip().lower() or "best",
        test_size=_bounded_float(config.get("test_size"), default=0.25, minimum=0.10, maximum=0.45),
        cv_splits=max(2, min(int(config.get("cv_splits", 5)), 10)),
        augmentation_enabled=bool(augmentation.get("enabled", True)),
        augmentation_synthetic_multiplier=max(
            0,
            min(int(augmentation.get("synthetic_multiplier", 3)), 20),
        ),
        augmentation_max_synthetic_rows=max(
            0,
            int(augmentation.get("max_synthetic_rows", 5000)),
        ),
        augmentation_synthetic_sample_weight=_bounded_float(
            augmentation.get("synthetic_sample_weight"),
            default=0.30,
            minimum=0.01,
            maximum=1.0,
        ),
        augmentation_noise_level=_bounded_float(
            augmentation.get("noise_level"),
            default=0.08,
            minimum=0.0,
            maximum=0.30,
        ),
        augmentation_min_average_precision_gain=_bounded_float(
            augmentation.get("min_average_precision_gain"),
            default=0.001,
            minimum=0.0,
            maximum=1.0,
        ),
        artifact_dir=ROOT / str(config.get("artifact_dir", "artifacts/ml")),
        report_dir=ROOT / str(config.get("report_dir", "reports/ml")),
        dataset_dir=ROOT / str(config.get("dataset_dir", "data/processed/ml")),
    )
