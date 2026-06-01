from __future__ import annotations

import argparse
import json

from sqlalchemy import text

from app.core.database import engine


def main() -> int:
    parser = argparse.ArgumentParser(description="Lista melhores oportunidades hibridas.")
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()
    sql = text(
        """
        SELECT
            supplier_slug,
            product_title,
            heuristic_score,
            heuristic_decision,
            ml_score,
            ml_decision,
            final_score,
            final_decision,
            decision_source,
            confidence_level,
            explanation
        FROM gold.ml_opportunity_scores_latest
        WHERE final_decision IN ('comprar_teste', 'revisar')
        ORDER BY
            CASE final_decision WHEN 'comprar_teste' THEN 1 ELSE 2 END,
            final_score DESC
        LIMIT :limit
        """
    )
    with engine.connect() as connection:
        rows = [dict(row) for row in connection.execute(sql, {"limit": max(1, args.limit)}).mappings()]
    print(json.dumps(rows, ensure_ascii=False, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
