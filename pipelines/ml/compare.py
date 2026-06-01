from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import engine
from pipelines.ml.config import get_ml_config


def build_comparison_summary() -> dict[str, Any]:
    totals_sql = text(
        """
        SELECT
            COUNT(*) AS scored,
            COUNT(*) FILTER (WHERE heuristic_decision = final_decision) AS final_agreements,
            COUNT(*) FILTER (WHERE heuristic_decision <> ml_decision) AS heuristic_ml_disagreements,
            COUNT(*) FILTER (WHERE final_decision = 'comprar_teste') AS comprar_teste,
            COUNT(*) FILTER (WHERE final_decision = 'revisar') AS revisar,
            COUNT(*) FILTER (WHERE final_decision = 'ignorar') AS ignorar,
            ROUND(AVG(ABS(score_difference)), 4) AS avg_abs_score_difference
        FROM gold.ml_opportunity_scores_latest
        """
    )
    by_supplier_sql = text(
        """
        SELECT
            supplier_slug,
            final_decision,
            COUNT(*) AS products,
            ROUND(AVG(final_score), 4) AS avg_final_score,
            ROUND(AVG(ABS(score_difference)), 4) AS avg_abs_score_difference
        FROM gold.ml_opportunity_scores_latest
        GROUP BY supplier_slug, final_decision
        ORDER BY supplier_slug, final_decision
        """
    )
    with engine.connect() as connection:
        totals = dict(connection.execute(totals_sql).mappings().one())
        by_supplier = [dict(row) for row in connection.execute(by_supplier_sql).mappings()]
    summary = {"totals": totals, "by_supplier_final_decision": by_supplier}
    report_path: Path = get_ml_config().report_dir / "heuristic_vs_ml_summary.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(summary, ensure_ascii=False, default=str, indent=2),
        encoding="utf-8",
    )
    return summary
