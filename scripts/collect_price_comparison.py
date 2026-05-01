from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.price_history.ingest import ingest_price_comparison_search  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta comparadores de preco para Bronze.")
    parser.add_argument("--source", choices=["buscape", "zoom"], default="buscape")
    parser.add_argument("--query", required=True)
    parser.add_argument("--max-results", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = ingest_price_comparison_search(
        source_name=args.source,
        query=args.query,
        max_results=args.max_results,
    )
    print(
        json.dumps(
            {
                "pipeline_run_id": str(result.pipeline_run_id),
                "source_run_id": str(result.source_run_id),
                "status": result.status,
                "source_name": result.source_name,
                "query": result.query,
                "extracted": result.extracted,
                "loaded": result.loaded,
                "skipped": result.skipped,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

