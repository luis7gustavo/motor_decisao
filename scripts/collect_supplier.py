from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipelines.suppliers.ingest import ingest_supplier_html  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coleta fornecedor HTML configurado para Bronze.")
    parser.add_argument("--supplier-slug", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = ingest_supplier_html(supplier_slug=args.supplier_slug)
    print(
        json.dumps(
            {
                "pipeline_run_id": str(result.pipeline_run_id),
                "source_run_id": str(result.source_run_id),
                "status": result.status,
                "supplier_slug": result.supplier_slug,
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

