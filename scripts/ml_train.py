from __future__ import annotations

import json

from pipelines.ml.train_model import train_models


def main() -> int:
    result = train_models()
    print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
