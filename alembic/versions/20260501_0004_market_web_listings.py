"""market web listings raw

Revision ID: 20260501_0004
Revises: 20260430_0003
Create Date: 2026-05-01
"""

from alembic import op


revision = "20260501_0004"
down_revision = "20260430_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE control.source_runs
        DROP CONSTRAINT IF EXISTS source_runs_source_type_check;
        """
    )
    op.execute(
        """
        ALTER TABLE control.source_runs
        ADD CONSTRAINT source_runs_source_type_check
        CHECK (
            source_type IN (
                'marketplace_api',
                'marketplace_scraper',
                'price_history',
                'supplier_scraper',
                'manual',
                'internal'
            )
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.market_web_listings_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(80) NOT NULL,
            source_role VARCHAR(30) NOT NULL
                CHECK (source_role IN ('benchmark','supplier','hybrid')),
            query TEXT,
            position INTEGER,
            title TEXT,
            price NUMERIC(12,2),
            old_price NUMERIC(12,2),
            currency_id VARCHAR(10) DEFAULT 'BRL',
            sold_quantity_text TEXT,
            sold_quantity INTEGER,
            demand_signal_type VARCHAR(60),
            demand_signal_value NUMERIC(18,4),
            bsr_text TEXT,
            rating_text TEXT,
            reviews_count INTEGER,
            seller_text TEXT,
            shipping_text TEXT,
            installments_text TEXT,
            item_url TEXT,
            image_url TEXT,
            is_sponsored BOOLEAN,
            is_full BOOLEAN,
            is_catalog BOOLEAN,
            blocked BOOLEAN NOT NULL DEFAULT FALSE,
            block_reason TEXT,
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_name, query, payload_hash, fetched_date)
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_web_source_query_date
        ON bronze.market_web_listings_raw(source_name, query, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_web_price
        ON bronze.market_web_listings_raw(source_name, price);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_web_sold_qty
        ON bronze.market_web_listings_raw(source_name, sold_quantity);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_web_payload_gin
        ON bronze.market_web_listings_raw USING gin(payload);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bronze.market_web_listings_raw;")
    op.execute(
        """
        ALTER TABLE control.source_runs
        DROP CONSTRAINT IF EXISTS source_runs_source_type_check;
        """
    )
    op.execute(
        """
        ALTER TABLE control.source_runs
        ADD CONSTRAINT source_runs_source_type_check
        CHECK (
            source_type IN (
                'marketplace_api',
                'price_history',
                'supplier_scraper',
                'manual',
                'internal'
            )
        );
        """
    )
