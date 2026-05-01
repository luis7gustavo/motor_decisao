"""mercado livre catalog products raw

Revision ID: 20260430_0003
Revises: 20260430_0002
Create Date: 2026-04-30
"""

from alembic import op


revision = "20260430_0003"
down_revision = "20260430_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.mercado_livre_products_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(50) NOT NULL DEFAULT 'mercado_livre',
            site_id VARCHAR(10) NOT NULL DEFAULT 'MLB',
            query TEXT,
            product_id VARCHAR(120) NOT NULL,
            product_name TEXT,
            domain_id VARCHAR(120),
            status VARCHAR(50),
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_name, product_id, query, fetched_date)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_products_raw_query_date
        ON bronze.mercado_livre_products_raw(query, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_products_raw_domain_date
        ON bronze.mercado_livre_products_raw(domain_id, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ml_products_raw_payload_gin
        ON bronze.mercado_livre_products_raw USING gin(payload);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.mercado_livre_products_raw;")
