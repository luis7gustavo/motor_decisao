"""collection v2 observability

Revision ID: 20260812_0008
Revises: 20260601_0008
Create Date: 2026-08-12
"""

from alembic import op


revision = "20260812_0008"
down_revision = "20260601_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE control.source_runs
            ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC(14,3),
            ADD COLUMN IF NOT EXISTS requests_total INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS requests_success INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS requests_failed INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS http_requests INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS browser_requests INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS worker_id VARCHAR(160),
            ADD COLUMN IF NOT EXISTS hostname VARCHAR(255),
            ADD COLUMN IF NOT EXISTS collection_strategy VARCHAR(40),
            ADD COLUMN IF NOT EXISTS avg_response_time_ms NUMERIC(14,3),
            ADD COLUMN IF NOT EXISTS peak_memory_mb NUMERIC(14,3),
            ADD COLUMN IF NOT EXISTS avg_cpu_percent NUMERIC(7,3);
        """
    )
    op.execute(
        """
        DO $$
        DECLARE constraint_name text;
        BEGIN
            SELECT c.conname INTO constraint_name
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'control' AND t.relname = 'source_runs'
              AND c.contype = 'c' AND pg_get_constraintdef(c.oid) ILIKE '%status%';
            IF constraint_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE control.source_runs DROP CONSTRAINT %I', constraint_name);
            END IF;
        END $$;
        ALTER TABLE control.source_runs
            ADD CONSTRAINT source_runs_status_check
            CHECK (status IN ('created','running','success','partial','failed','cancelled','blocked'));
        """
    )
    op.execute(
        """
        DO $$
        DECLARE constraint_name text;
        BEGIN
            SELECT c.conname INTO constraint_name
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'control' AND t.relname = 'pipeline_runs'
              AND c.contype = 'c' AND pg_get_constraintdef(c.oid) ILIKE '%status%';
            IF constraint_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE control.pipeline_runs DROP CONSTRAINT %I', constraint_name);
            END IF;
        END $$;
        ALTER TABLE control.pipeline_runs
            ADD CONSTRAINT pipeline_runs_status_check
            CHECK (status IN ('created','running','success','partial','failed','cancelled','blocked'));
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS control.collection_alerts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID REFERENCES control.pipeline_runs(id) ON DELETE SET NULL,
            source_run_id UUID REFERENCES control.source_runs(id) ON DELETE SET NULL,
            source_name VARCHAR(120) NOT NULL,
            alert_type VARCHAR(80) NOT NULL,
            severity VARCHAR(20) NOT NULL CHECK (severity IN ('info','warning','critical')),
            status VARCHAR(20) NOT NULL DEFAULT 'open' CHECK (status IN ('open','acknowledged','resolved')),
            message TEXT NOT NULL,
            observed_value NUMERIC(18,4),
            threshold_value NUMERIC(18,4),
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_collection_alerts_open
            ON control.collection_alerts(status, severity, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_source_runs_collection_metrics
            ON control.source_runs(source_name, finished_at DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS control.collection_alerts;")
    op.execute(
        """
        ALTER TABLE control.source_runs
            DROP COLUMN IF EXISTS duration_seconds,
            DROP COLUMN IF EXISTS requests_total,
            DROP COLUMN IF EXISTS requests_success,
            DROP COLUMN IF EXISTS requests_failed,
            DROP COLUMN IF EXISTS http_requests,
            DROP COLUMN IF EXISTS browser_requests,
            DROP COLUMN IF EXISTS retry_count,
            DROP COLUMN IF EXISTS worker_id,
            DROP COLUMN IF EXISTS hostname,
            DROP COLUMN IF EXISTS collection_strategy,
            DROP COLUMN IF EXISTS avg_response_time_ms,
            DROP COLUMN IF EXISTS peak_memory_mb,
            DROP COLUMN IF EXISTS avg_cpu_percent;
        """
    )
