from __future__ import annotations

import argparse
import json

from pipelines.ml.compare import build_comparison_summary
from pipelines.ml.predict_opportunities import predict_current_opportunities
from pipelines.ml.train_model import train_models


def main() -> int:
    parser = argparse.ArgumentParser(description="Treina e executa a camada ML hibrida.")
    parser.add_argument(
        "--predict-only",
        action="store_true",
        help="Reutiliza o modelo mais recente e executa apenas a predicao.",
    )
    parser.add_argument(
        "--allow-missing-model",
        action="store_true",
        help="No modo predict-only, retorna sucesso se ainda nao existir modelo.",
    )
    args = parser.parse_args()

    train_result = None
    if not args.predict_only:
        train_result = train_models()
    try:
        predict_result = predict_current_opportunities()
    except RuntimeError as error:
        if not args.allow_missing_model or "No successful ML model found" not in str(error):
            raise
        print(json.dumps({"status": "skipped", "reason": str(error)}, ensure_ascii=False))
        return 0
    comparison = build_comparison_summary()
    print(
        json.dumps(
            {
                "training": train_result.__dict__ if train_result else None,
                "prediction": predict_result.__dict__,
                "comparison": comparison,
            },
            ensure_ascii=False,
            default=str,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
