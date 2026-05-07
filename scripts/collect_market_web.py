from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.market_web.ingest import ingest_market_web  # noqa: E402
from pipelines.market_web.sources import SOURCE_CONFIGS  # noqa: E402


DEFAULT_QUERIES = [
    "mouse gamer",
    "mouse sem fio",
    "teclado mecanico",
    "teclado gamer",
    "combo teclado mouse",
    "fone bluetooth",
    "headset gamer",
    "hub usb",
    "adaptador wifi usb",
    "cabo usb c",
    "mousepad gamer",
    "webcam",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta web com Playwright.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        choices=sorted(SOURCE_CONFIGS.keys()) + ["all"],
        default=None,
        help="Fonte a coletar. Pode repetir. Use all para todas.",
    )
    parser.add_argument("--query", action="append", dest="queries", default=None)
    parser.add_argument("--max-results", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    selected_sources = args.sources or ["shopee", "amazon", "kabum", "pichau", "terabyte", "aliexpress"]
    if "all" in selected_sources:
        selected_sources = sorted(SOURCE_CONFIGS.keys())
    result = ingest_market_web(
        sources=selected_sources,
        queries=args.queries or DEFAULT_QUERIES,
        max_results=args.max_results,
    )
    print(
        json.dumps(
            {
                "pipeline_run_id": str(result.pipeline_run_id),
                "source_run_ids": [str(source_run_id) for source_run_id in result.source_run_ids],
                "status": result.status,
                "extracted": result.extracted,
                "loaded": result.loaded,
                "skipped": result.skipped,
                "blocked": result.blocked,
                "results": result.results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
