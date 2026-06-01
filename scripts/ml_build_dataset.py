from __future__ import annotations

import json

from pipelines.ml.build_training_dataset import build_training_dataset


def main() -> int:
    result = build_training_dataset()
    print(
        json.dumps(
            {
                "rows": result.rows,
                "positive_rows": result.positive_rows,
                "csv_path": str(result.csv_path),
                "metadata_path": str(result.metadata_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
