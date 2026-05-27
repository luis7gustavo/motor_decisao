"""Coleta precos de fornecedores B2B configurados em config.yaml.

Uso:
    python scripts/collect_suppliers.py                 # todos os fornecedores habilitados
    python scripts/collect_suppliers.py --supplier mirao
    python scripts/collect_suppliers.py --list          # lista fornecedores disponiveis
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from app.core.settings import get_settings
from pipelines.suppliers.ingest import ingest_supplier_html


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def _get_enabled_suppliers(project_config: dict) -> list[str]:
    supplier_sources = project_config.get("supplier_sources", {})
    if not supplier_sources.get("enabled", False):
        return []
    suppliers = supplier_sources.get("suppliers") or []
    return [s["slug"] for s in suppliers if s.get("enabled", False)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Coleta precos de fornecedores B2B.")
    parser.add_argument("--supplier", metavar="SLUG", help="Coleta apenas este fornecedor.")
    parser.add_argument("--list", action="store_true", help="Lista fornecedores disponiveis.")
    args = parser.parse_args()

    settings = get_settings()
    project_config = settings.load_project_config()
    enabled = _get_enabled_suppliers(project_config)

    if args.list:
        _log(f"Fornecedores habilitados: {enabled or '(nenhum)'}")
        return 0

    slugs = [args.supplier] if args.supplier else enabled
    if not slugs:
        _log("Nenhum fornecedor habilitado. Verifique supplier_sources.enabled em config.yaml.")
        return 1

    _log(f"Iniciando coleta de fornecedores: {', '.join(slugs)}")
    print("=" * 60, flush=True)

    failed = []
    for slug in slugs:
        _log(f"[{slug}] iniciando...")
        try:
            result = ingest_supplier_html(supplier_slug=slug, triggered_by="local_cli_suppliers")
            _log(
                f"[{slug}] OK — {result.loaded} carregados / "
                f"{result.extracted} extraidos / {result.skipped} duplicados"
            )
        except Exception as exc:  # noqa: BLE001
            _log(f"[{slug}] ERRO: {exc}")
            failed.append(slug)

    print("=" * 60, flush=True)
    if failed:
        _log(f"FINALIZADO COM ERRO: {', '.join(failed)}")
        return 1
    _log("PRONTO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
