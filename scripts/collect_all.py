"""Orquestra as coletas locais em uma unica chamada.

Uso:
    python scripts/collect_all.py                       # tudo
    python scripts/collect_all.py --skip etl            # sem ETL final
    python scripts/collect_all.py --only ml             # so ML API
    python scripts/collect_all.py --only web            # so scraping web
    python scripts/collect_all.py --only suppliers      # so fornecedores B2B
    python scripts/collect_all.py --only etl            # so export ETL
    python scripts/collect_all.py --only engine         # so recalculo Gold
    python scripts/collect_all.py --only mlengine       # so predicao ML hibrida
    python scripts/collect_all.py --only powerbi        # so export Power BI
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

STAGES = ("ml", "web", "suppliers", "etl", "engine", "mlengine", "powerbi")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _sep(char: str = "-", width: int = 60) -> None:
    print(char * width, flush=True)


def _run(label: str, script: str, *extra_args: str) -> bool:
    """Roda um script como subprocesso e propaga falhas para o exit code final."""
    _log(f"  -> {label}")
    cmd = [sys.executable, str(SCRIPTS / script), *extra_args]
    result = subprocess.run(cmd)
    ok = result.returncode == 0
    marker = "OK" if ok else "ERRO"
    _log(f"  [{marker}] {label} (exit {result.returncode})")
    return ok


def stage_ml() -> bool:
    _sep()
    _log("[ML API] Catalogo rapido + perifericos com precos")
    _sep()
    results = [
        _run("ML Catalog Fast (produtos sem preco, 4 pag/query)", "collect_mercado_livre_catalog_fast.py"),
        _run("ML Perifericos (anuncios com preco, 200 items/query)", "collect_mercado_livre_perifericos.py"),
    ]
    return all(results)


def stage_web() -> bool:
    _sep()
    _log("[WEB] Scraping de marketplaces + historico de precos")
    _sep()
    results = [
        _run("Market Web (Amazon / Kabum / Terabyte)", "collect_market_web.py"),
        _run("Price History (Buscape / Zoom, sequencial)", "collect_price_history_web.py"),
    ]
    return all(results)


def stage_suppliers() -> bool:
    _sep()
    _log("[SUPPLIERS] Coleta de fornecedores B2B (Mirao, etc.)")
    _sep()
    return _run("Suppliers B2B", "collect_suppliers.py")


def stage_etl() -> bool:
    _sep()
    _log("[ETL] Export Silver + oportunidades")
    _sep()
    return _run("Quick ETL Export", "quick_etl_exports.py")


def stage_powerbi() -> bool:
    _sep()
    _log("[POWER BI] Export da camada analitica")
    _sep()
    return _run("Power BI Export", "export_power_bi.py")


def stage_engine() -> bool:
    _sep()
    _log("[ENGINE] Importa snapshots e recalcula recomendacoes Gold")
    _sep()
    return _run(
        "Decision Engine",
        "build_decision_engine.py",
        "--import-megamix",
        "--import-coletek",
    )


def stage_mlengine() -> bool:
    _sep()
    _log("[ML ENGINE] Predicao hibrida com o ultimo modelo treinado")
    _sep()
    return _run(
        "ML Hybrid Prediction",
        "ml_run_all.py",
        "--predict-only",
        "--allow-missing-model",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Roda todas as coletas de uma vez.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--only",
        choices=STAGES,
        metavar="STAGE",
        help=f"Roda apenas um estagio ({', '.join(STAGES)}).",
    )
    group.add_argument(
        "--skip",
        choices=STAGES,
        metavar="STAGE",
        help=f"Pula um estagio ({', '.join(STAGES)}).",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        default=False,
        help="Forca execucao sequencial (padrao: ml e web em paralelo).",
    )
    return parser.parse_args()


def _run_stage_threaded(
    name: str,
    target: Callable[[], bool],
    queue: Queue[tuple[str, bool]],
) -> None:
    try:
        queue.put((name, bool(target())))
    except Exception as error:  # noqa: BLE001 - surface thread failures through exit code.
        _log(f"  [ERRO] Stage {name} falhou com excecao: {error}")
        queue.put((name, False))


def main() -> int:
    args = parse_args()

    run_ml = args.only in (None, "ml") and args.skip != "ml"
    run_web = args.only in (None, "web") and args.skip != "web"
    run_suppliers = args.only in (None, "suppliers") and args.skip != "suppliers"
    run_etl = args.only in (None, "etl") and args.skip != "etl"
    run_engine = args.only in (None, "engine") and args.skip != "engine"
    run_mlengine = args.only in (None, "mlengine") and args.skip != "mlengine"
    run_powerbi = args.only in (None, "powerbi") and args.skip != "powerbi"

    _sep("=")
    _log("COLETA COMPLETA - motor_decisao_compra")
    _log(
        f"  ML={'sim' if run_ml else 'nao'}"
        f"  WEB={'sim' if run_web else 'nao'}"
        f"  SUPPLIERS={'sim' if run_suppliers else 'nao'}"
        f"  ETL={'sim' if run_etl else 'nao'}"
        f"  ENGINE={'sim' if run_engine else 'nao'}"
        f"  MLENGINE={'sim' if run_mlengine else 'nao'}"
        f"  POWERBI={'sim' if run_powerbi else 'nao'}"
    )
    _sep("=")

    stage_results: dict[str, bool] = {}

    # ML API, web scraping e suppliers usam recursos distintos; rodam em paralelo.
    parallel_stages: list[tuple[str, object]] = []
    if run_ml:
        parallel_stages.append(("ml", stage_ml))
    if run_web:
        parallel_stages.append(("web", stage_web))
    if run_suppliers:
        parallel_stages.append(("suppliers", stage_suppliers))

    if len(parallel_stages) > 1 and not args.sequential:
        _log(f"Iniciando {', '.join(n for n, _ in parallel_stages)} em paralelo...")
        queue: Queue[tuple[str, bool]] = Queue()
        threads = [
            threading.Thread(
                target=_run_stage_threaded, args=(name, fn, queue), name=name, daemon=True
            )
            for name, fn in parallel_stages
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        while not queue.empty():
            name, ok = queue.get()
            stage_results[name] = ok
    else:
        for name, fn in parallel_stages:
            stage_results[name] = fn()  # type: ignore[operator]

    if run_etl:
        stage_results["etl"] = stage_etl()
    if run_engine:
        stage_results["engine"] = stage_engine()
    if run_mlengine:
        stage_results["mlengine"] = stage_mlengine()
    if run_powerbi:
        stage_results["powerbi"] = stage_powerbi()

    failed_stages = [name for name, ok in stage_results.items() if not ok]
    _sep("=")
    if failed_stages:
        _log(f"FINALIZADO COM ERRO nos stages: {', '.join(sorted(failed_stages))}")
        _sep("=")
        return 1

    _log("PRONTO")
    _sep("=")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
