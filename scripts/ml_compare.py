from __future__ import annotations

import json

try:
    from scripts import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # type: ignore  # noqa: F401

from motor_decisao.pipelines.ml.compare import build_comparison_summary


def main() -> int:
    print(json.dumps(build_comparison_summary(), ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
