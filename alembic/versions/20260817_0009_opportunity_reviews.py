"""auditable human opportunity reviews

Revision ID: 20260817_0009
Revises: 20260812_0008
Create Date: 2026-08-17
"""

from alembic import op


revision = "20260817_0009"
down_revision = "20260812_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS feedback;")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback.opportunity_reviews (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            opportunity_id UUID NOT NULL,
            snapshot_id UUID,
            supplier_product_id UUID NOT NULL,
            event_type VARCHAR(20) NOT NULL DEFAULT 'decision',
            human_decision VARCHAR(40),
            reason_code VARCHAR(80),
            notes TEXT,
            max_purchase_price NUMERIC(12,2),
            reviewer VARCHAR(160),
            original_heuristic_recommendation VARCHAR(40) NOT NULL,
            original_heuristic_score NUMERIC(8,4) NOT NULL,
            ml_score NUMERIC(8,4),
            heuristic_version VARCHAR(120),
            model_version VARCHAR(120),
            run_id UUID,
            reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            supersedes_review_id UUID REFERENCES feedback.opportunity_reviews(id) ON DELETE RESTRICT,
            undoes_review_id UUID REFERENCES feedback.opportunity_reviews(id) ON DELETE RESTRICT,
            invalidated_at TIMESTAMPTZ,
            invalidated_by VARCHAR(160),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT ck_opportunity_reviews_event_type
                CHECK (event_type IN ('decision', 'undo')),
            CONSTRAINT ck_opportunity_reviews_human_decision
                CHECK (
                    human_decision IS NULL OR human_decision IN (
                        'approved_test_purchase',
                        'needs_reanalysis',
                        'rejected'
                    )
                ),
            CONSTRAINT ck_opportunity_reviews_event_payload
                CHECK (
                    (event_type = 'decision' AND human_decision IS NOT NULL)
                    OR
                    (event_type = 'undo' AND human_decision IS NULL AND is_active = FALSE)
                ),
            CONSTRAINT ck_opportunity_reviews_max_price
                CHECK (max_purchase_price IS NULL OR max_purchase_price > 0)
        );
        """
    )
    # A oportunidade Gold atual é mutável e pode ser removida em uma nova rodada.
    # Seus UUIDs são preservados como dados de auditoria, sem FK destrutiva.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_reviews_active_snapshot
        ON feedback.opportunity_reviews (
            opportunity_id,
            COALESCE(snapshot_id, '00000000-0000-0000-0000-000000000000'::uuid)
        )
        WHERE is_active = TRUE;
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_reviews_single_undo
        ON feedback.opportunity_reviews (undoes_review_id)
        WHERE event_type = 'undo';
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_reviews_opportunity "
        "ON feedback.opportunity_reviews(opportunity_id, reviewed_at DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_reviews_snapshot "
        "ON feedback.opportunity_reviews(snapshot_id, reviewed_at DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_reviews_decision "
        "ON feedback.opportunity_reviews(human_decision, reviewed_at DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_reviews_active "
        "ON feedback.opportunity_reviews(is_active, reviewed_at DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunity_reviews_reviewed_at "
        "ON feedback.opportunity_reviews(reviewed_at DESC);"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS feedback.opportunity_reviews;")
    op.execute("DROP SCHEMA IF EXISTS feedback;")
