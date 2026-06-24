from __future__ import annotations

import argparse
import json

try:
    from scripts import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # type: ignore  # noqa: F401

from motor_decisao.pipelines.ml.predict_opportunities import predict_current_opportunities


def main() -> int:
    parser = argparse.ArgumentParser(description="Pontua oportunidades atuais com o modelo ML.")
    parser.add_argument(
        "--allow-missing-model",
        action="store_true",
        help="Retorna sucesso sem pontuar quando ainda nao existe modelo treinado.",
    )
    args = parser.parse_args()
    try:
        result = predict_current_opportunities()
    except RuntimeError as error:
        if not args.allow_missing_model or "No successful ML model found" not in str(error):
            raise
        print(json.dumps({"status": "skipped", "reason": str(error)}, ensure_ascii=False))
        return 0
    print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
