from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipelines.ml.build_features import (
    FeatureRecord,
    fetch_current_feature_records,
    write_dataset_metadata,
    write_training_csv,
)
from pipelines.ml.config import get_ml_config


@dataclass(frozen=True)
class TrainingDatasetResult:
    records: list[FeatureRecord]
    csv_path: Path
    metadata_path: Path
    rows: int
    positive_rows: int


def build_training_dataset() -> TrainingDatasetResult:
    config = get_ml_config()
    records = fetch_current_feature_records()
    positive_rows = sum(record.opportunity_label for record in records)
    if len(records) < config.min_training_rows:
        raise RuntimeError(
            f"ML dataset has {len(records)} rows; minimum is {config.min_training_rows}."
        )
    if positive_rows < 2:
        raise RuntimeError(
            "ML dataset needs at least 2 positive proxy rows from revisar/comprar_teste."
        )

    csv_path = config.dataset_dir / "training_dataset_proxy.csv"
    metadata_path = config.dataset_dir / "training_dataset_proxy_metadata.json"
    write_training_csv(csv_path, records)
    write_dataset_metadata(metadata_path, records)
    return TrainingDatasetResult(
        records=records,
        csv_path=csv_path,
        metadata_path=metadata_path,
        rows=len(records),
        positive_rows=positive_rows,
    )
