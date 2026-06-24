from __future__ import annotations

import argparse
from pathlib import Path

try:
    from scripts import _bootstrap  # noqa: F401
except ImportError:
    import _bootstrap  # type: ignore  # noqa: F401

from motor_decisao.pipelines.decision_engine.build import build_decision_opportunities
from scripts.import_coletek_catalog import (
    DEFAULT_CATALOG_PATH as DEFAULT_COLETEK_CATALOG_PATH,
    import_coletek_catalog,
)
from scripts.import_megamix_catalog import DEFAULT_CATALOG_PATH, import_megamix_catalog


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera oportunidades do motor de decisao.")
    parser.add_argument(
        "--import-megamix",
        action="store_true",
        help="Importa o catalogo MegaMix para Bronze antes de pontuar.",
    )
    parser.add_argument(
        "--megamix-path",
        type=Path,
        default=DEFAULT_CATALOG_PATH,
        help="Caminho do megamix_catalog_raw.json.",
    )
    parser.add_argument(
        "--import-coletek",
        action="store_true",
        help="Importa o snapshot Coletek para Bronze antes de pontuar.",
    )
    parser.add_argument(
        "--coletek-path",
        type=Path,
        default=DEFAULT_COLETEK_CATALOG_PATH,
        help="Caminho do coletek_catalog_raw.json.",
    )
    args = parser.parse_args()

    if args.import_megamix:
        import_result = import_megamix_catalog(
            path=args.megamix_path,
            triggered_by="local_cli_decision_engine",
        )
        print(
            "MegaMix: "
            f"{import_result.loaded} carregados / "
            f"{import_result.extracted} extraidos / "
            f"{import_result.skipped} duplicados ou invalidos"
        )

    if args.import_coletek:
        import_result = import_coletek_catalog(
            path=args.coletek_path,
            triggered_by="local_cli_decision_engine",
        )
        print(
            "Coletek: "
            f"{import_result.loaded} carregados / "
            f"{import_result.extracted} extraidos / "
            f"{import_result.skipped} duplicados ou invalidos"
        )

    result = build_decision_opportunities(triggered_by="local_cli_decision_engine")
    print(
        "Motor de decisao: "
        f"{result.opportunities_scored} produtos pontuados / "
        f"{result.comprar_teste} comprar_teste / "
        f"{result.revisar} revisar / "
        f"{result.ignorar} ignorar"
    )
    print(f"pipeline_run_id={result.pipeline_run_id}")
    print(f"decision_run_id={result.decision_run_id}")
    print(f"scoring_version={result.scoring_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
