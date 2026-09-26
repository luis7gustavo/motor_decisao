from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, exc, text

from app.schemas.review import ReviewDecisionInput, ReviewFilters


ZERO_UUID = "00000000-0000-0000-0000-000000000000"


class ReviewConflictError(RuntimeError):
    pass


class ReviewNotFoundError(RuntimeError):
    pass


class ReviewRepository:
    """Consultas PostgreSQL da fila e escrita transacional do feedback."""

    _SORT_SQL = {
        "score": "o.decision_score DESC, o.generated_at DESC, o.id",
        "margin": "o.net_margin_pct DESC NULLS LAST, o.decision_score DESC, o.id",
        "profit": "o.estimated_net_profit DESC NULLS LAST, o.decision_score DESC, o.id",
        "date": "o.generated_at DESC, o.decision_score DESC, o.id",
    }

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine

    @staticmethod
    def _filter_params(filters: ReviewFilters) -> dict[str, Any]:
        return {
            "supplier": filters.supplier,
            "q": filters.q,
            "min_margin": (
                filters.min_margin / Decimal("100")
                if filters.min_margin is not None
                else None
            ),
            "min_profit": filters.min_profit,
            "min_score": filters.min_score,
            "min_match": filters.min_match,
            "risk": filters.risk,
        }

    @staticmethod
    def _pending_predicate(alias: str = "o", snapshot_alias: str = "snap") -> str:
        return f"""
            NOT EXISTS (
                SELECT 1
                FROM feedback.opportunity_reviews AS active_review
                WHERE active_review.event_type = 'decision'
                  AND active_review.is_active = TRUE
                  AND active_review.opportunity_id = {alias}.id
                  AND COALESCE(active_review.snapshot_id, '{ZERO_UUID}'::uuid)
                      = COALESCE({snapshot_alias}.id, '{ZERO_UUID}'::uuid)
            )
        """

    @staticmethod
    def _filters_sql() -> str:
        return """
          AND (CAST(:supplier AS varchar) IS NULL OR o.supplier_slug = CAST(:supplier AS varchar))
          AND (CAST(:q AS varchar) IS NULL OR o.product_title ILIKE ('%' || CAST(:q AS varchar) || '%'))
          AND (CAST(:min_margin AS numeric) IS NULL OR o.net_margin_pct >= CAST(:min_margin AS numeric))
          AND (CAST(:min_profit AS numeric) IS NULL OR o.estimated_net_profit >= CAST(:min_profit AS numeric))
          AND (CAST(:min_score AS numeric) IS NULL OR o.decision_score >= CAST(:min_score AS numeric))
          AND (CAST(:min_match AS numeric) IS NULL OR o.match_confidence >= CAST(:min_match AS numeric))
          AND (CAST(:risk AS varchar) IS NULL OR CAST(:risk AS varchar) = ANY(o.risk_flags))
        """

    def list_pending(self, filters: ReviewFilters) -> list[dict[str, Any]]:
        order_sql = self._SORT_SQL[filters.sort]
        sql = text(
            f"""
            SELECT
                o.id,
                snap.id AS snapshot_id,
                o.supplier_slug,
                o.product_title,
                o.decision_score,
                o.net_margin_pct,
                o.estimated_net_profit,
                o.generated_at
            FROM gold.decision_opportunities AS o
            LEFT JOIN gold.decision_opportunity_snapshots AS snap
              ON snap.decision_run_id = o.decision_run_id
             AND snap.supplier_product_id = o.supplier_product_id
            WHERE o.recommendation = 'revisar'
              AND {self._pending_predicate()}
              {self._filters_sql()}
            ORDER BY {order_sql}
            """
        )
        with self.engine.connect() as connection:
            rows = connection.execute(sql, self._filter_params(filters)).mappings().all()
        return [dict(row) for row in rows]

    def get_summary(self) -> dict[str, int]:
        sql = text(
            f"""
            WITH current_pending AS (
                SELECT o.id
                FROM gold.decision_opportunities AS o
                LEFT JOIN gold.decision_opportunity_snapshots AS snap
                  ON snap.decision_run_id = o.decision_run_id
                 AND snap.supplier_product_id = o.supplier_product_id
                WHERE o.recommendation = 'revisar'
                  AND {self._pending_predicate()}
            ), active_decisions AS (
                SELECT human_decision
                FROM feedback.opportunity_reviews
                WHERE event_type = 'decision' AND is_active = TRUE
            )
            SELECT
                (SELECT COUNT(*) FROM current_pending) AS pending,
                COUNT(*) FILTER (WHERE human_decision = 'approved_test_purchase') AS approved,
                COUNT(*) FILTER (WHERE human_decision = 'needs_reanalysis') AS needs_reanalysis,
                COUNT(*) FILTER (WHERE human_decision = 'rejected') AS rejected
            FROM active_decisions
            """
        )
        with self.engine.connect() as connection:
            row = connection.execute(sql).mappings().one()
        return {key: int(value or 0) for key, value in dict(row).items()}

    def get_filter_options(self) -> dict[str, list[str]]:
        suppliers_sql = text(
            """
            SELECT DISTINCT supplier_slug
            FROM gold.decision_opportunities
            WHERE recommendation = 'revisar'
            ORDER BY supplier_slug
            """
        )
        risks_sql = text(
            """
            SELECT DISTINCT risk_flag
            FROM gold.decision_opportunities AS o,
                 LATERAL unnest(o.risk_flags) AS risk_flag
            WHERE o.recommendation = 'revisar'
            ORDER BY risk_flag
            """
        )
        with self.engine.connect() as connection:
            suppliers = list(connection.execute(suppliers_sql).scalars())
            risks = list(connection.execute(risks_sql).scalars())
        return {"suppliers": suppliers, "risks": risks}

    def get_opportunity(self, opportunity_id: UUID) -> dict[str, Any] | None:
        sql = text(
            f"""
            SELECT
                o.id,
                o.decision_run_id AS run_id,
                snap.id AS snapshot_id,
                o.supplier_product_id,
                o.supplier_raw_id,
                o.supplier_slug,
                o.product_title,
                o.normalized_title,
                o.supplier_price,
                o.estimated_market_price,
                o.estimated_net_profit,
                o.net_margin_pct,
                o.total_fee_pct,
                o.market_offer_count,
                o.market_source_count,
                o.price_history_count,
                o.mercado_livre_count,
                o.demand_score,
                o.match_confidence,
                o.decision_score,
                o.recommendation,
                o.confidence_level,
                o.scoring_version AS heuristic_version,
                o.risk_flags,
                o.evidence,
                o.generated_at,
                supplier.sku,
                supplier.ean,
                supplier.source_url,
                supplier.fetched_date,
                supplier.fetched_at,
                raw.raw_stock,
                raw.payload AS supplier_payload,
                decision_run.config_snapshot AS run_config,
                ml.ml_score,
                ml.model_version,
                active_review.id AS active_review_id,
                active_review.human_decision AS active_human_decision,
                active_review.reviewed_at AS active_reviewed_at
            FROM gold.decision_opportunities AS o
            JOIN silver.supplier_products_normalized AS supplier
              ON supplier.id = o.supplier_product_id
            LEFT JOIN bronze.supplier_products_raw AS raw
              ON raw.id = o.supplier_raw_id
            LEFT JOIN gold.decision_opportunity_snapshots AS snap
              ON snap.decision_run_id = o.decision_run_id
             AND snap.supplier_product_id = o.supplier_product_id
            LEFT JOIN gold.decision_engine_runs AS decision_run
              ON decision_run.id = o.decision_run_id
            LEFT JOIN gold.ml_opportunity_scores_latest AS ml
              ON ml.supplier_product_id = o.supplier_product_id
            LEFT JOIN LATERAL (
                SELECT review.id, review.human_decision, review.reviewed_at
                FROM feedback.opportunity_reviews AS review
                WHERE review.event_type = 'decision'
                  AND review.is_active = TRUE
                  AND review.opportunity_id = o.id
                  AND COALESCE(review.snapshot_id, '{ZERO_UUID}'::uuid)
                      = COALESCE(snap.id, '{ZERO_UUID}'::uuid)
                ORDER BY review.reviewed_at DESC
                LIMIT 1
            ) AS active_review ON TRUE
            WHERE o.id = CAST(:opportunity_id AS uuid)
              AND o.recommendation = 'revisar'
            """
        )
        with self.engine.connect() as connection:
            row = connection.execute(
                sql, {"opportunity_id": str(opportunity_id)}
            ).mappings().first()
        return dict(row) if row else None

    def enrich_evidence(self, matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not matches:
            return []
        serializable_matches = [
            {"ordinal": ordinal, **match}
            for ordinal, match in enumerate(matches, start=1)
        ]
        sql = text(
            """
            WITH wanted AS (
                SELECT *
                FROM jsonb_to_recordset(CAST(:matches AS jsonb)) AS item(
                    ordinal integer,
                    source_kind text,
                    source_name text,
                    title text,
                    price numeric,
                    match_confidence numeric,
                    position integer,
                    reviews_count integer,
                    sold_quantity integer,
                    item_url text,
                    fetched_date text
                )
            )
            SELECT
                wanted.*,
                market.image_url,
                market.shipping_text,
                COALESCE(
                    market.payload->>'availability',
                    market.payload->>'stock_status',
                    market.payload->>'stock'
                ) AS availability,
                COALESCE(wanted.item_url, market.item_url) AS resolved_item_url
            FROM wanted
            LEFT JOIN LATERAL (
                SELECT raw.image_url, raw.shipping_text, raw.payload, raw.item_url
                FROM bronze.market_web_listings_raw AS raw
                WHERE wanted.source_kind = 'market_web'
                  AND raw.source_name = wanted.source_name
                  AND raw.title IS NOT DISTINCT FROM wanted.title
                  AND raw.price IS NOT DISTINCT FROM wanted.price
                  AND raw.fetched_date::text IS NOT DISTINCT FROM wanted.fetched_date
                ORDER BY raw.fetched_at DESC NULLS LAST, raw.id DESC
                LIMIT 1
            ) AS market ON TRUE
            ORDER BY wanted.ordinal
            """
        )
        with self.engine.connect() as connection:
            rows = connection.execute(
                sql,
                {"matches": json.dumps(serializable_matches, ensure_ascii=False)},
            ).mappings().all()
        return [dict(row) for row in rows]

    def create_decision(
        self,
        opportunity_id: UUID,
        decision: ReviewDecisionInput,
    ) -> dict[str, Any]:
        opportunity_sql = text(
            """
            SELECT
                o.id,
                snap.id AS snapshot_id,
                o.supplier_product_id,
                o.decision_run_id AS run_id,
                o.recommendation,
                o.decision_score,
                o.scoring_version,
                ml.ml_score,
                ml.model_version
            FROM gold.decision_opportunities AS o
            LEFT JOIN gold.decision_opportunity_snapshots AS snap
              ON snap.decision_run_id = o.decision_run_id
             AND snap.supplier_product_id = o.supplier_product_id
            LEFT JOIN gold.ml_opportunity_scores_latest AS ml
              ON ml.supplier_product_id = o.supplier_product_id
            WHERE o.id = CAST(:opportunity_id AS uuid)
              AND o.recommendation = 'revisar'
            FOR UPDATE OF o
            """
        )
        active_sql = text(
            f"""
            SELECT id
            FROM feedback.opportunity_reviews
            WHERE event_type = 'decision'
              AND is_active = TRUE
              AND opportunity_id = CAST(:opportunity_id AS uuid)
              AND COALESCE(snapshot_id, '{ZERO_UUID}'::uuid)
                  = COALESCE(CAST(:snapshot_id AS uuid), '{ZERO_UUID}'::uuid)
            FOR UPDATE
            """
        )
        previous_sql = text(
            f"""
            SELECT id
            FROM feedback.opportunity_reviews
            WHERE event_type = 'decision'
              AND opportunity_id = CAST(:opportunity_id AS uuid)
              AND COALESCE(snapshot_id, '{ZERO_UUID}'::uuid)
                  = COALESCE(CAST(:snapshot_id AS uuid), '{ZERO_UUID}'::uuid)
            ORDER BY reviewed_at DESC
            LIMIT 1
            """
        )
        insert_sql = text(
            """
            INSERT INTO feedback.opportunity_reviews (
                opportunity_id,
                snapshot_id,
                supplier_product_id,
                event_type,
                human_decision,
                reason_code,
                notes,
                max_purchase_price,
                reviewer,
                original_heuristic_recommendation,
                original_heuristic_score,
                ml_score,
                heuristic_version,
                model_version,
                run_id,
                is_active,
                supersedes_review_id
            ) VALUES (
                CAST(:opportunity_id AS uuid),
                CAST(:snapshot_id AS uuid),
                CAST(:supplier_product_id AS uuid),
                'decision',
                :human_decision,
                :reason_code,
                :notes,
                :max_purchase_price,
                :reviewer,
                :original_heuristic_recommendation,
                :original_heuristic_score,
                :ml_score,
                :heuristic_version,
                :model_version,
                CAST(:run_id AS uuid),
                TRUE,
                CAST(:supersedes_review_id AS uuid)
            )
            RETURNING id, opportunity_id, snapshot_id, human_decision, reviewed_at
            """
        )
        try:
            with self.engine.begin() as connection:
                opportunity = connection.execute(
                    opportunity_sql, {"opportunity_id": str(opportunity_id)}
                ).mappings().first()
                if opportunity is None:
                    raise ReviewNotFoundError("Oportunidade pendente não encontrada.")

                snapshot_id = opportunity["snapshot_id"]
                pair_params = {
                    "opportunity_id": str(opportunity_id),
                    "snapshot_id": str(snapshot_id) if snapshot_id else None,
                }
                if connection.execute(active_sql, pair_params).first() is not None:
                    raise ReviewConflictError("Esta oportunidade já possui uma decisão ativa.")
                previous = connection.execute(previous_sql, pair_params).mappings().first()

                row = connection.execute(
                    insert_sql,
                    {
                        **pair_params,
                        "supplier_product_id": str(opportunity["supplier_product_id"]),
                        "human_decision": decision.decision,
                        "reason_code": decision.reason_code,
                        "notes": decision.notes,
                        "max_purchase_price": decision.max_purchase_price,
                        "reviewer": decision.reviewer,
                        "original_heuristic_recommendation": opportunity["recommendation"],
                        "original_heuristic_score": opportunity["decision_score"],
                        "ml_score": opportunity["ml_score"],
                        "heuristic_version": opportunity["scoring_version"],
                        "model_version": opportunity["model_version"],
                        "run_id": str(opportunity["run_id"]) if opportunity["run_id"] else None,
                        "supersedes_review_id": str(previous["id"]) if previous else None,
                    },
                ).mappings().one()
                return dict(row)
        except exc.IntegrityError as error:
            raise ReviewConflictError(
                "A decisão não foi duplicada; outra submissão já foi registrada."
            ) from error

    def undo_decision(
        self,
        opportunity_id: UUID,
        *,
        invalidated_by: str | None = None,
    ) -> dict[str, Any]:
        active_sql = text(
            """
            SELECT *
            FROM feedback.opportunity_reviews
            WHERE event_type = 'decision'
              AND is_active = TRUE
              AND opportunity_id = CAST(:opportunity_id AS uuid)
            ORDER BY reviewed_at DESC
            LIMIT 1
            FOR UPDATE
            """
        )
        deactivate_sql = text(
            """
            UPDATE feedback.opportunity_reviews
            SET is_active = FALSE,
                invalidated_at = NOW(),
                invalidated_by = :invalidated_by
            WHERE id = CAST(:review_id AS uuid)
            """
        )
        audit_sql = text(
            """
            INSERT INTO feedback.opportunity_reviews (
                opportunity_id,
                snapshot_id,
                supplier_product_id,
                event_type,
                human_decision,
                reason_code,
                notes,
                max_purchase_price,
                reviewer,
                original_heuristic_recommendation,
                original_heuristic_score,
                ml_score,
                heuristic_version,
                model_version,
                run_id,
                is_active,
                undoes_review_id
            ) VALUES (
                CAST(:opportunity_id AS uuid),
                CAST(:snapshot_id AS uuid),
                CAST(:supplier_product_id AS uuid),
                'undo',
                NULL,
                'undo',
                :notes,
                NULL,
                :reviewer,
                :original_heuristic_recommendation,
                :original_heuristic_score,
                :ml_score,
                :heuristic_version,
                :model_version,
                CAST(:run_id AS uuid),
                FALSE,
                CAST(:undoes_review_id AS uuid)
            )
            RETURNING id, opportunity_id, snapshot_id, undoes_review_id, reviewed_at
            """
        )
        with self.engine.begin() as connection:
            active = connection.execute(
                active_sql, {"opportunity_id": str(opportunity_id)}
            ).mappings().first()
            if active is None:
                raise ReviewNotFoundError("Não existe decisão ativa para desfazer.")
            reviewer = (invalidated_by or active["reviewer"] or "operador_local").strip()[:160]
            connection.execute(
                deactivate_sql,
                {"review_id": str(active["id"]), "invalidated_by": reviewer},
            )
            undo_row = connection.execute(
                audit_sql,
                {
                    "opportunity_id": str(active["opportunity_id"]),
                    "snapshot_id": str(active["snapshot_id"]) if active["snapshot_id"] else None,
                    "supplier_product_id": str(active["supplier_product_id"]),
                    "notes": f"Desfez a decisão {active['human_decision']}.",
                    "reviewer": reviewer,
                    "original_heuristic_recommendation": active[
                        "original_heuristic_recommendation"
                    ],
                    "original_heuristic_score": active["original_heuristic_score"],
                    "ml_score": active["ml_score"],
                    "heuristic_version": active["heuristic_version"],
                    "model_version": active["model_version"],
                    "run_id": str(active["run_id"]) if active["run_id"] else None,
                    "undoes_review_id": str(active["id"]),
                },
            ).mappings().one()
            return dict(undo_row)

    def list_history(self, *, limit: int = 200) -> list[dict[str, Any]]:
        sql = text(
            """
            SELECT
                review.*,
                opportunity.product_title,
                opportunity.supplier_slug
            FROM feedback.opportunity_reviews AS review
            LEFT JOIN gold.decision_opportunities AS opportunity
              ON opportunity.id = review.opportunity_id
            ORDER BY review.reviewed_at DESC, review.created_at DESC
            LIMIT :limit
            """
        )
        with self.engine.connect() as connection:
            rows = connection.execute(sql, {"limit": limit}).mappings().all()
        return [dict(row) for row in rows]
