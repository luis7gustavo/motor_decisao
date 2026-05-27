"""decision engine runs and snapshots

Revision ID: 20260526_0007
Revises: 20260525_0006
Create Date: 2026-05-26
"""

from alembic import op


revision = "20260526_0007"
down_revision = "20260525_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.decision_engine_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID REFERENCES control.pipeline_runs(id) ON DELETE SET NULL,
            scoring_version VARCHAR(80) NOT NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'running',
            config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CHECK (status IN ('running', 'success', 'partial', 'failed', 'cancelled'))
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_engine_runs_started
        ON gold.decision_engine_runs(started_at DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_engine_runs_status
        ON gold.decision_engine_runs(status, started_at DESC);
        """
    )

    op.execute("ALTER TABLE gold.decision_opportunities ADD COLUMN IF NOT EXISTS decision_run_id UUID;")
    op.execute(
        """
        ALTER TABLE gold.decision_opportunities
        ADD COLUMN IF NOT EXISTS scoring_version VARCHAR(80) NOT NULL DEFAULT 'heuristic_v1';
        """
    )
    op.execute(
        """
        ALTER TABLE gold.decision_opportunities
        ADD COLUMN IF NOT EXISTS confidence_level VARCHAR(20) NOT NULL DEFAULT 'baixa';
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'decision_opportunities_decision_run_id_fkey'
            ) THEN
                ALTER TABLE gold.decision_opportunities
                ADD CONSTRAINT decision_opportunities_decision_run_id_fkey
                FOREIGN KEY (decision_run_id)
                REFERENCES gold.decision_engine_runs(id)
                ON DELETE SET NULL;
            END IF;
        END $$;
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_opportunities_run
        ON gold.decision_opportunities(decision_run_id);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_opportunities_confidence
        ON gold.decision_opportunities(confidence_level, recommendation, decision_score DESC);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.decision_opportunity_snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            decision_run_id UUID NOT NULL REFERENCES gold.decision_engine_runs(id) ON DELETE CASCADE,
            supplier_product_id UUID NOT NULL REFERENCES silver.supplier_products_normalized(id) ON DELETE CASCADE,
            supplier_raw_id UUID NOT NULL REFERENCES bronze.supplier_products_raw(id) ON DELETE CASCADE,
            supplier_slug VARCHAR(120) NOT NULL,
            product_title TEXT NOT NULL,
            normalized_title TEXT NOT NULL,
            supplier_price NUMERIC(12,2),
            estimated_market_price NUMERIC(12,2),
            estimated_net_profit NUMERIC(12,2),
            net_margin_pct NUMERIC(8,4),
            total_fee_pct NUMERIC(8,4),
            market_offer_count INTEGER NOT NULL DEFAULT 0,
            market_source_count INTEGER NOT NULL DEFAULT 0,
            price_history_count INTEGER NOT NULL DEFAULT 0,
            mercado_livre_count INTEGER NOT NULL DEFAULT 0,
            demand_score NUMERIC(6,2) NOT NULL DEFAULT 0,
            match_confidence NUMERIC(6,2) NOT NULL DEFAULT 0,
            decision_score NUMERIC(6,2) NOT NULL DEFAULT 0,
            recommendation VARCHAR(40) NOT NULL DEFAULT 'ignorar',
            confidence_level VARCHAR(20) NOT NULL DEFAULT 'baixa',
            scoring_version VARCHAR(80) NOT NULL,
            risk_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
            generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (decision_run_id, supplier_product_id)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_snapshots_run_rank
        ON gold.decision_opportunity_snapshots(
            decision_run_id,
            recommendation,
            confidence_level,
            decision_score DESC
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_snapshots_supplier
        ON gold.decision_opportunity_snapshots(decision_run_id, supplier_slug, decision_score DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS gold.decision_opportunity_snapshots;")
    op.execute("DROP INDEX IF EXISTS gold.idx_decision_opportunities_confidence;")
    op.execute("DROP INDEX IF EXISTS gold.idx_decision_opportunities_run;")
    op.execute(
        """
        ALTER TABLE gold.decision_opportunities
        DROP CONSTRAINT IF EXISTS decision_opportunities_decision_run_id_fkey;
        """
    )
    op.execute("ALTER TABLE gold.decision_opportunities DROP COLUMN IF EXISTS confidence_level;")
    op.execute("ALTER TABLE gold.decision_opportunities DROP COLUMN IF EXISTS scoring_version;")
    op.execute("ALTER TABLE gold.decision_opportunities DROP COLUMN IF EXISTS decision_run_id;")
    op.execute("DROP TABLE IF EXISTS gold.decision_engine_runs;")
