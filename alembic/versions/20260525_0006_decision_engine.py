"""decision engine silver and gold tables

Revision ID: 20260525_0006
Revises: 20260501_0005
Create Date: 2026-05-25
"""

from alembic import op


revision = "20260525_0006"
down_revision = "20260501_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.supplier_products_normalized (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            supplier_raw_id UUID NOT NULL REFERENCES bronze.supplier_products_raw(id) ON DELETE CASCADE,
            supplier_slug VARCHAR(120) NOT NULL,
            source_product_key VARCHAR(240) NOT NULL,
            raw_title TEXT NOT NULL,
            normalized_title TEXT NOT NULL,
            title_tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            token_count INTEGER NOT NULL DEFAULT 0,
            supplier_price NUMERIC(12,2),
            currency_id VARCHAR(10) DEFAULT 'BRL',
            sku VARCHAR(120),
            ean VARCHAR(30),
            source_url TEXT,
            fetched_date DATE NOT NULL,
            fetched_at TIMESTAMPTZ,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (supplier_raw_id)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_supplier_norm_slug_date
        ON silver.supplier_products_normalized(supplier_slug, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_supplier_norm_price
        ON silver.supplier_products_normalized(supplier_slug, supplier_price);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_supplier_norm_tokens
        ON silver.supplier_products_normalized USING gin(title_tokens);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_supplier_norm_title_trgm
        ON silver.supplier_products_normalized(normalized_title);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.decision_opportunities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
            risk_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
            generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (supplier_product_id)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_opportunities_rank
        ON gold.decision_opportunities(recommendation, decision_score DESC, demand_score DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_opportunities_supplier
        ON gold.decision_opportunities(supplier_slug, decision_score DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decision_opportunities_margin
        ON gold.decision_opportunities(net_margin_pct DESC NULLS LAST);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS gold.decision_opportunities;")
    op.execute("DROP TABLE IF EXISTS silver.supplier_products_normalized;")
