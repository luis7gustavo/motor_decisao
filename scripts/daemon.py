"""Daemon de coleta continua.

Roda collect_all.py em loop enquanto o computador estiver ligado.
Aguarda um cooldown entre ciclos e detecta se o computador dormiu.

Uso:
    python scripts/daemon.py                      # ciclo a cada 4h
    python scripts/daemon.py --cooldown 6         # ciclo a cada 6h
    python scripts/daemon.py --once               # roda uma vez e sai
    python scripts/daemon.py --cooldown 0.5       # ciclo a cada 30min (testes)
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
LOG_FILE = ROOT / "data" / "daemon.log"

DEFAULT_COOLDOWN_HOURS = 4.0
# Se o tempo entre ticks for maior que isso, o PC provavelmente dormiu
SLEEP_DETECTION_THRESHOLD_SECONDS = 120


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def _run_cycle() -> bool:
    """Executa um ciclo completo de coleta. Retorna True se bem-sucedido."""
    _log("=" * 60)
    _log("INICIO DO CICLO DE COLETA")
    _log("=" * 60)
    cmd = [sys.executable, str(SCRIPTS / "collect_all.py")]
    result = subprocess.run(cmd)
    ok = result.returncode == 0
    _log(f"FIM DO CICLO — {'OK' if ok else 'ERRO'} (exit {result.returncode})")
    return ok


def _wait_with_detection(cooldown_seconds: float) -> None:
    """Aguarda cooldown em ticks de 60s, detectando se o PC dormiu."""
    tick = 60.0
    waited = 0.0
    next_run = datetime.now() + timedelta(seconds=cooldown_seconds)
    _log(f"Proximo ciclo em: {next_run.strftime('%H:%M:%S')} ({cooldown_seconds/3600:.1f}h)")

    while waited < cooldown_seconds:
        before = time.monotonic()
        time.sleep(tick)
        after = time.monotonic()
        elapsed_real = after - before

        # Detecta sono: o tempo real foi muito maior que o esperado
        if elapsed_real > tick + SLEEP_DETECTION_THRESHOLD_SECONDS:
            _log(
                f"PC acordou apos dormir ({elapsed_real:.0f}s dormido). "
                "Iniciando ciclo imediatamente."
            )
            return

        waited += tick
        remaining = cooldown_seconds - waited
        if remaining > 0 and int(remaining) % 3600 == 0:
            _log(f"Aguardando... {remaining/3600:.1f}h para o proximo ciclo.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Daemon de coleta continua.")
    parser.add_argument(
        "--cooldown",
        type=float,
        default=DEFAULT_COOLDOWN_HOURS,
        metavar="HORAS",
        help=f"Horas entre ciclos (padrao: {DEFAULT_COOLDOWN_HOURS}).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Roda um ciclo e sai.",
    )
    args = parser.parse_args()

    cooldown_seconds = args.cooldown * 3600
    _log(f"Daemon iniciado. Cooldown: {args.cooldown}h | Loop: {'nao' if args.once else 'sim'}")
    _log(f"Log: {LOG_FILE}")

    cycle = 0
    try:
        while True:
            cycle += 1
            _log(f"--- Ciclo #{cycle} ---")
            _run_cycle()

            if args.once:
                break

            _wait_with_detection(cooldown_seconds)

    except KeyboardInterrupt:
        _log("Daemon interrompido pelo usuario (Ctrl+C).")

    _log("Daemon encerrado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
