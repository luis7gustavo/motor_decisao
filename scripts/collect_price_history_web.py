from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.price_history.ingest import ingest_price_comparison_web_history  # noqa: E402
from app.core.settings import get_settings  # noqa: E402


DEFAULT_QUERIES = [
    "mouse gamer",
    "mouse sem fio",
    "teclado mecanico",
    "teclado gamer",
    "combo teclado mouse",
    "headset gamer",
    "fone bluetooth",
    "hub usb",
    "adaptador wifi usb",
    "cabo usb c",
    "cabo hdmi",
    "mousepad gamer",
    "webcam",
    "carregador usb c",
    "suporte notebook",
    "base notebook",
    "adaptador bluetooth",
    "microfone usb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta web de historico de precos em Buscape/Zoom."
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=["buscape", "zoom", "all"],
        default=None,
        help="Fonte a coletar. Pode repetir. Use all para Buscape e Zoom.",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Termo de busca. Pode repetir. Se omitido, usa lista padrao de perifericos baratos.",
    )
    parser.add_argument("--max-results", type=int, default=8)
    parser.add_argument(
        "--product-detail-limit",
        type=int,
        default=None,
        help="Maximo de paginas de produto abertas por termo/fonte. Padrao: igual a --max-results.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Numero de tarefas paralelas. Padrao: usa config.market_sources.price_history.web_history.parallel_workers.",
    )
    return parser.parse_args()


def _sources(values: list[str] | None) -> list[str]:
    if not values or "all" in values:
        return ["buscape", "zoom"]
    return values


def _resolve_workers(arg_workers: int | None) -> int:
    if arg_workers is not None:
        return max(1, arg_workers)
    project_config = get_settings().load_project_config()
    web_history = (
        project_config.get("market_sources", {})
        .get("price_history", {})
        .get("web_history", {})
    )
    return max(1, int(web_history.get("parallel_workers", 1)))


def _run_task(
    *,
    source_name: str,
    query: str,
    max_results: int,
    product_detail_limit: int | None,
) -> dict[str, object]:
    try:
        result = ingest_price_comparison_web_history(
            source_name=source_name,
            query=query,
            max_results=max_results,
            product_detail_limit=product_detail_limit,
            triggered_by="local_cli_price_history_web",
        )
        return {
            "source_name": source_name,
            "query": query,
            "status": result.status,
            "extracted": result.extracted,
            "loaded": result.loaded,
            "skipped": result.skipped,
            "blocked": result.blocked,
            "source_run_id": str(result.source_run_id),
        }
    except Exception as error:  # noqa: BLE001 - batch collection should continue across terms.
        return {
            "source_name": source_name,
            "query": query,
            "status": "failed",
            "error": str(error),
        }


def main() -> int:
    args = parse_args()
    queries = args.queries or DEFAULT_QUERIES
    sources = _sources(args.source)
    task_specs = [
        {"order": order, "source_name": source_name, "query": query}
        for order, (source_name, query) in enumerate(
            (source_name, query)
            for source_name in sources
            for query in queries
        )
    ]
    worker_count = min(_resolve_workers(args.workers), max(1, len(task_specs)))
    results_by_order: dict[int, dict[str, object]] = {}

    if worker_count <= 1:
        for spec in task_specs:
            results_by_order[spec["order"]] = _run_task(
                source_name=spec["source_name"],
                query=spec["query"],
                max_results=args.max_results,
                product_detail_limit=args.product_detail_limit,
            )
    else:
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="price-history-cli",
        ) as executor:
            future_to_spec = {
                executor.submit(
                    _run_task,
                    source_name=spec["source_name"],
                    query=spec["query"],
                    max_results=args.max_results,
                    product_detail_limit=args.product_detail_limit,
                ): spec
                for spec in task_specs
            }
            for future in as_completed(future_to_spec):
                spec = future_to_spec[future]
                try:
                    results_by_order[spec["order"]] = future.result()
                except Exception as error:  # noqa: BLE001 - keep full batch output stable.
                    results_by_order[spec["order"]] = {
                        "source_name": spec["source_name"],
                        "query": spec["query"],
                        "status": "failed",
                        "error": str(error),
                    }

    results = [results_by_order[index] for index in range(len(task_specs))]

    summary = {
        "sources": sources,
        "queries": len(queries),
        "runs": len(results),
        "workers": worker_count,
        "success": sum(1 for result in results if result["status"] == "success"),
        "partial": sum(1 for result in results if result["status"] == "partial"),
        "failed": sum(1 for result in results if result["status"] == "failed"),
        "extracted": sum(int(result.get("extracted", 0)) for result in results),
        "loaded": sum(int(result.get("loaded", 0)) for result in results),
        "skipped": sum(int(result.get("skipped", 0)) for result in results),
        "blocked": sum(int(result.get("blocked", 0)) for result in results),
        "results": results,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
