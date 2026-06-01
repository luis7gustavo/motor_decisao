"""ML engine model runs and hybrid opportunity scores

Revision ID: 20260601_0008
Revises: 20260526_0007
Create Date: 2026-06-01
"""

from alembic import op


revision = "20260601_0008"
down_revision = "20260526_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.ml_model_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_version VARCHAR(120) NOT NULL UNIQUE,
            model_type VARCHAR(80) NOT NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'running',
            training_rows INTEGER NOT NULL DEFAULT 0,
            positive_rows INTEGER NOT NULL DEFAULT 0,
            feature_names JSONB NOT NULL DEFAULT '[]'::jsonb,
            metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            artifact_path TEXT,
            trained_at TIMESTAMPTZ,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (status IN ('running', 'success', 'failed'))
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_model_runs_status_created
        ON gold.ml_model_runs(status, created_at DESC);
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.ml_opportunity_scores (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_run_id UUID NOT NULL REFERENCES gold.ml_model_runs(id) ON DELETE CASCADE,
            decision_run_id UUID REFERENCES gold.decision_engine_runs(id) ON DELETE SET NULL,
            supplier_product_id UUID NOT NULL REFERENCES silver.supplier_products_normalized(id) ON DELETE CASCADE,
            supplier_raw_id UUID NOT NULL REFERENCES bronze.supplier_products_raw(id) ON DELETE CASCADE,
            supplier_slug VARCHAR(120) NOT NULL,
            product_title TEXT NOT NULL,
            heuristic_score NUMERIC(8,4) NOT NULL,
            heuristic_decision VARCHAR(40) NOT NULL,
            ml_score NUMERIC(8,4) NOT NULL,
            ml_prediction INTEGER NOT NULL,
            ml_decision VARCHAR(40) NOT NULL,
            final_score NUMERIC(8,4) NOT NULL,
            final_decision VARCHAR(40) NOT NULL,
            score_difference NUMERIC(8,4) NOT NULL,
            decision_source VARCHAR(80) NOT NULL,
            confidence_level VARCHAR(20) NOT NULL,
            explanation TEXT NOT NULL,
            model_version VARCHAR(120) NOT NULL,
            features_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
            scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (model_run_id, supplier_product_id)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_scores_model_final
        ON gold.ml_opportunity_scores(model_run_id, final_decision, final_score DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_scores_supplier
        ON gold.ml_opportunity_scores(model_run_id, supplier_slug, final_score DESC);
        """
    )
    op.execute(
        """
        CREATE OR REPLACE VIEW gold.ml_opportunity_scores_latest AS
        SELECT scores.*
        FROM gold.ml_opportunity_scores AS scores
        JOIN (
            SELECT id
            FROM gold.ml_model_runs
            WHERE status = 'success'
            ORDER BY trained_at DESC NULLS LAST, created_at DESC
            LIMIT 1
        ) AS latest_model
            ON latest_model.id = scores.model_run_id;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS gold.ml_opportunity_scores_latest;")
    op.execute("DROP TABLE IF EXISTS gold.ml_opportunity_scores;")
    op.execute("DROP TABLE IF EXISTS gold.ml_model_runs;")
