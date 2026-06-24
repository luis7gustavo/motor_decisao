from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from motor_decisao.pipelines.ml.build_features import FEATURE_NAMES


FEATURE_INDEX = {name: index for index, name in enumerate(FEATURE_NAMES)}
COUNT_FEATURES = (
    "market_offer_count",
    "market_source_count",
    "price_history_count",
    "mercado_livre_count",
    "risk_flag_count",
)
PRICE_FEATURES = (
    "supplier_price",
    "estimated_market_price",
)


@dataclass(frozen=True)
class SyntheticTrainingRows:
    x_values: np.ndarray
    y_values: np.ndarray
    sample_weights: np.ndarray
    source_indexes: np.ndarray

    @property
    def rows(self) -> int:
        return int(len(self.y_values))


def _empty_synthetic_rows(columns: int) -> SyntheticTrainingRows:
    return SyntheticTrainingRows(
        x_values=np.empty((0, columns), dtype=float),
        y_values=np.empty((0,), dtype=int),
        sample_weights=np.empty((0,), dtype=float),
        source_indexes=np.empty((0,), dtype=int),
    )


def _jitter(value: float, *, rng: np.random.Generator, noise_level: float) -> float:
    if value == 0:
        return 0.0
    return float(value * (1.0 + rng.normal(0.0, noise_level)))


def perturb_positive_feature_vector(
    row: np.ndarray,
    *,
    rng: np.random.Generator,
    noise_level: float,
) -> np.ndarray:
    synthetic = np.asarray(row, dtype=float).copy()
    for feature in PRICE_FEATURES:
        index = FEATURE_INDEX[feature]
        synthetic[index] = max(0.0, _jitter(synthetic[index], rng=rng, noise_level=noise_level))

    for feature in COUNT_FEATURES:
        index = FEATURE_INDEX[feature]
        synthetic[index] = max(
            0.0,
            float(round(_jitter(synthetic[index], rng=rng, noise_level=noise_level))),
        )

    net_margin_index = FEATURE_INDEX["net_margin_pct"]
    fee_index = FEATURE_INDEX["total_fee_pct"]
    demand_index = FEATURE_INDEX["demand_score"]
    match_index = FEATURE_INDEX["match_confidence"]
    market_price_index = FEATURE_INDEX["estimated_market_price"]
    profit_index = FEATURE_INDEX["estimated_net_profit"]

    synthetic[net_margin_index] = float(
        np.clip(_jitter(synthetic[net_margin_index], rng=rng, noise_level=noise_level), -1.0, 2.0)
    )
    synthetic[fee_index] = float(
        np.clip(_jitter(synthetic[fee_index], rng=rng, noise_level=noise_level), 0.0, 1.0)
    )
    synthetic[demand_index] = float(
        np.clip(_jitter(synthetic[demand_index], rng=rng, noise_level=noise_level), 0.0, 100.0)
    )
    synthetic[match_index] = float(
        np.clip(_jitter(synthetic[match_index], rng=rng, noise_level=noise_level), 0.0, 100.0)
    )
    synthetic[profit_index] = float(
        synthetic[market_price_index] * synthetic[net_margin_index]
    )

    synthetic[FEATURE_INDEX["has_market_price"]] = (
        1.0 if synthetic[market_price_index] > 0 else 0.0
    )
    synthetic[FEATURE_INDEX["has_positive_margin"]] = (
        1.0 if synthetic[net_margin_index] > 0 else 0.0
    )
    synthetic[FEATURE_INDEX["has_multiple_market_sources"]] = (
        1.0 if synthetic[FEATURE_INDEX["market_source_count"]] >= 2 else 0.0
    )
    synthetic[FEATURE_INDEX["has_mercado_livre_signal"]] = (
        1.0 if synthetic[FEATURE_INDEX["mercado_livre_count"]] > 0 else 0.0
    )
    return synthetic


def generate_synthetic_training_rows(
    *,
    x_values: np.ndarray,
    y_values: np.ndarray,
    sample_weights: np.ndarray,
    random_state: int,
    synthetic_multiplier: int,
    max_synthetic_rows: int,
    synthetic_sample_weight: float,
    noise_level: float,
) -> SyntheticTrainingRows:
    columns = int(x_values.shape[1])
    positive_indexes = np.flatnonzero(y_values == 1)
    target_rows = min(max_synthetic_rows, len(positive_indexes) * synthetic_multiplier)
    if target_rows <= 0:
        return _empty_synthetic_rows(columns)

    rng = np.random.default_rng(random_state)
    source_indexes = rng.choice(positive_indexes, size=target_rows, replace=True)
    synthetic_values = np.vstack(
        [
            perturb_positive_feature_vector(
                x_values[source_index],
                rng=rng,
                noise_level=noise_level,
            )
            for source_index in source_indexes
        ]
    )
    return SyntheticTrainingRows(
        x_values=synthetic_values,
        y_values=np.ones(target_rows, dtype=int),
        sample_weights=np.asarray(
            [sample_weights[source_index] * synthetic_sample_weight for source_index in source_indexes],
            dtype=float,
        ),
        source_indexes=np.asarray(source_indexes, dtype=int),
    )


def write_augmented_training_csv(
    path: Path,
    *,
    x_real: np.ndarray,
    y_real: np.ndarray,
    weights_real: np.ndarray,
    synthetic: SyntheticTrainingRows,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "is_synthetic",
        "source_row_index",
        "opportunity_label",
        "sample_weight",
        *FEATURE_NAMES,
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row_index, (features, label, weight) in enumerate(
            zip(x_real, y_real, weights_real)
        ):
            writer.writerow(
                {
                    "is_synthetic": False,
                    "source_row_index": row_index,
                    "opportunity_label": int(label),
                    "sample_weight": float(weight),
                    **dict(zip(FEATURE_NAMES, features)),
                }
            )
        for features, label, weight, source_index in zip(
            synthetic.x_values,
            synthetic.y_values,
            synthetic.sample_weights,
            synthetic.source_indexes,
        ):
            writer.writerow(
                {
                    "is_synthetic": True,
                    "source_row_index": int(source_index),
                    "opportunity_label": int(label),
                    "sample_weight": float(weight),
                    **dict(zip(FEATURE_NAMES, features)),
                }
            )
