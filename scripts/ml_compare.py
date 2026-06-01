from __future__ import annotations

import json

from pipelines.ml.compare import build_comparison_summary


def main() -> int:
    print(json.dumps(build_comparison_summary(), ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
