"""core foundation schemas and control tables

Revision ID: 20260430_0001
Revises:
Create Date: 2026-04-30
"""

from alembic import op


revision = "20260430_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
    op.execute('CREATE EXTENSION IF NOT EXISTS "vector";')

    op.execute("CREATE SCHEMA IF NOT EXISTS control;")
    op.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    op.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    op.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS control.pipeline_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_name VARCHAR(120) NOT NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'created'
                CHECK (status IN ('created','running','success','partial','failed','cancelled')),
            triggered_by VARCHAR(120) DEFAULT 'local',
            config_snapshot JSONB DEFAULT '{}'::jsonb,
            metadata JSONB DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS control.source_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID REFERENCES control.pipeline_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(120) NOT NULL,
            source_type VARCHAR(50) NOT NULL
                CHECK (source_type IN ('marketplace_api','price_history','supplier_scraper','manual','internal')),
            status VARCHAR(30) NOT NULL DEFAULT 'created'
                CHECK (status IN ('created','running','success','partial','failed','cancelled')),
            raw_table_name VARCHAR(160),
            records_extracted INTEGER DEFAULT 0,
            records_loaded INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            metadata JSONB DEFAULT '{}'::jsonb,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS control.data_quality_checks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID REFERENCES control.pipeline_runs(id) ON DELETE SET NULL,
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            schema_name VARCHAR(80) NOT NULL,
            table_name VARCHAR(120) NOT NULL,
            check_name VARCHAR(160) NOT NULL,
            status VARCHAR(30) NOT NULL
                CHECK (status IN ('passed','warning','failed','skipped')),
            metric_name VARCHAR(120),
            metric_value NUMERIC(18,4),
            threshold_value NUMERIC(18,4),
            details JSONB DEFAULT '{}'::jsonb,
            message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.mercado_livre_items_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(50) NOT NULL DEFAULT 'mercado_livre',
            site_id VARCHAR(10) NOT NULL DEFAULT 'MLB',
            category_id VARCHAR(50),
            query TEXT,
            external_id VARCHAR(120) NOT NULL,
            title TEXT,
            price NUMERIC(12,2),
            currency_id VARCHAR(10),
            seller_id VARCHAR(120),
            permalink TEXT,
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_name, external_id, fetched_date)
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.price_history_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(80) NOT NULL,
            product_key VARCHAR(200),
            product_url TEXT,
            title TEXT,
            current_price NUMERIC(12,2),
            min_price NUMERIC(12,2),
            median_price NUMERIC(12,2),
            max_price NUMERIC(12,2),
            history_window_days INTEGER,
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (source_name, payload_hash, fetched_date)
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.supplier_products_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            supplier_slug VARCHAR(120) NOT NULL,
            source_url TEXT,
            raw_title TEXT NOT NULL,
            raw_price NUMERIC(12,2),
            currency_id VARCHAR(10) DEFAULT 'BRL',
            raw_stock INTEGER,
            sku VARCHAR(120),
            ean VARCHAR(30),
            payload JSONB NOT NULL,
            payload_hash CHAR(64) NOT NULL,
            fetched_date DATE NOT NULL DEFAULT CURRENT_DATE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (supplier_slug, payload_hash, fetched_date)
        );
        """
    )

    op.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON control.pipeline_runs(status);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_source_runs_pipeline ON control.source_runs(pipeline_run_id);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_source_runs_source_status ON control.source_runs(source_name, status);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_quality_checks_status ON control.data_quality_checks(status);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ml_raw_category_date ON bronze.mercado_livre_items_raw(category_id, fetched_date);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ml_raw_payload_gin ON bronze.mercado_livre_items_raw USING gin(payload);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_price_history_source_date ON bronze.price_history_raw(source_name, fetched_date);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_supplier_raw_supplier_date ON bronze.supplier_products_raw(supplier_slug, fetched_date);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_supplier_raw_payload_gin ON bronze.supplier_products_raw USING gin(payload);")


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS gold CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS silver CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS bronze CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS control CASCADE;")

