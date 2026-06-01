from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.core.settings import get_settings


ROOT = Path(__file__).resolve().parents[2]


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
        artifact_dir=ROOT / str(config.get("artifact_dir", "artifacts/ml")),
        report_dir=ROOT / str(config.get("report_dir", "reports/ml")),
        dataset_dir=ROOT / str(config.get("dataset_dir", "data_processed/ml")),
    )
