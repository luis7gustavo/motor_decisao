"""mercado livre product price summaries

Revision ID: 20260430_0002
Revises: 20260430_0001
Create Date: 2026-04-30
"""

from alembic import op


revision = "20260430_0002"
down_revision = "20260430_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.mercado_livre_product_prices (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(50) NOT NULL DEFAULT 'mercado_livre',
            site_id VARCHAR(10) NOT NULL DEFAULT 'MLB',
            query TEXT,
            product_id VARCHAR(120) NOT NULL,
            product_name TEXT,
            domain_id VARCHAR(120),
            min_price NUMERIC(12,2),
            avg_price NUMERIC(12,2),
            max_price NUMERIC(12,2),
            offers_count INTEGER NOT NULL DEFAULT 0,
            currency_id VARCHAR(10),
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_name, product_id, query, currency_id, fetched_date)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_product_prices_query_date
        ON silver.mercado_livre_product_prices(query, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_product_prices_domain_date
        ON silver.mercado_livre_product_prices(domain_id, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_product_prices_payload_gin
        ON silver.mercado_livre_product_prices USING gin(payload);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS silver.mercado_livre_product_prices;")
