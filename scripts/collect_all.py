"""Orquestra todas as coletas em uma única chamada.

Uso:
    python scripts/collect_all.py                   # tudo
    python scripts/collect_all.py --skip etl        # sem ETL final
    python scripts/collect_all.py --only ml         # só ML API
    python scripts/collect_all.py --only web        # só scraping web
    python scripts/collect_all.py --only etl        # só export ETL
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

STAGES = ("ml", "web", "etl")


# ── helpers ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _sep(char: str = "─", width: int = 60) -> None:
    print(char * width, flush=True)


def _run(label: str, script: str, *extra_args: str) -> bool:
    """Roda um script como subprocesso, streamando stdout/stderr ao vivo."""
    _log(f"  → {label}")
    cmd = [sys.executable, str(SCRIPTS / script), *extra_args]
    result = subprocess.run(cmd)
    ok = result.returncode == 0
    _log(f"  {'✓' if ok else '✗'} {label} (exit {result.returncode})")
    return ok


# ── stages ────────────────────────────────────────────────────────────────────

def stage_ml() -> None:
    _sep()
    _log("[ML API] Catálogo rápido + periféricos com preços")
    _sep()
    _run("ML Catalog Fast (produtos sem preço, 4 pág/query)", "collect_mercado_livre_catalog_fast.py")
    _run("ML Periféricos (anúncios com preço, 200 items/query)", "collect_mercado_livre_perifericos.py")


def stage_web() -> None:
    _sep()
    _log("[WEB] Scraping de marketplaces + histórico de preços")
    _sep()
    _run("Market Web (Amazon / Shopee / Kabum / Terabyte / AliExpress)", "collect_market_web.py")
    _run("Price History (Buscape)", "collect_price_history_web.py")


def stage_etl() -> None:
    _sep()
    _log("[ETL] Export Silver + oportunidades")
    _sep()
    _run("Quick ETL Export", "quick_etl_exports.py")


# ── entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Roda todas as coletas de uma vez.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--only",
        choices=STAGES,
        metavar="STAGE",
        help=f"Roda apenas um estágio ({', '.join(STAGES)}).",
    )
    group.add_argument(
        "--skip",
        choices=STAGES,
        metavar="STAGE",
        help=f"Pula um estágio ({', '.join(STAGES)}).",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        default=False,
        help="Força execução sequencial (padrão: ml e web em paralelo).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    run_ml = args.only in (None, "ml") and args.skip != "ml"
    run_web = args.only in (None, "web") and args.skip != "web"
    run_etl = args.only in (None, "etl") and args.skip != "etl"

    _sep("═")
    _log("COLETA COMPLETA — motor_decisao_compra")
    _log(f"  ML={'sim' if run_ml else 'não'}  WEB={'sim' if run_web else 'não'}  ETL={'sim' if run_etl else 'não'}")
    _sep("═")

    # ML API (HTTP) e web scraping (Playwright) usam recursos distintos:
    # rodam em paralelo por padrão para economizar tempo.
    if run_ml and run_web and not args.sequential:
        _log("Iniciando ML e WEB em paralelo...")
        threads = [
            threading.Thread(target=stage_ml, name="ml", daemon=True),
            threading.Thread(target=stage_web, name="web", daemon=True),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    else:
        if run_ml:
            stage_ml()
        if run_web:
            stage_web()

    if run_etl:
        stage_etl()

    _sep("═")
    _log("PRONTO")
    _sep("═")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
