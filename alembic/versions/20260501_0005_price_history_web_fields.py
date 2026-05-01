"""price history web fields

Revision ID: 20260501_0005
Revises: 20260501_0004
Create Date: 2026-05-01
"""

from alembic import op


revision = "20260501_0005"
down_revision = "20260501_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE bronze.price_history_raw ADD COLUMN IF NOT EXISTS query TEXT;")
    op.execute(
        "ALTER TABLE bronze.price_history_raw ADD COLUMN IF NOT EXISTS source_mode VARCHAR(50) DEFAULT 'search_current';"
    )
    op.execute("ALTER TABLE bronze.price_history_raw ADD COLUMN IF NOT EXISTS avg_price NUMERIC(12,2);")
    op.execute(
        "ALTER TABLE bronze.price_history_raw ADD COLUMN IF NOT EXISTS blocked BOOLEAN NOT NULL DEFAULT FALSE;"
    )
    op.execute("ALTER TABLE bronze.price_history_raw ADD COLUMN IF NOT EXISTS block_reason TEXT;")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_history_source_query_mode_date
        ON bronze.price_history_raw(source_name, query, source_mode, fetched_date);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_history_current_price
        ON bronze.price_history_raw(source_name, current_price);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS bronze.idx_price_history_current_price;")
    op.execute("DROP INDEX IF EXISTS bronze.idx_price_history_source_query_mode_date;")
    op.execute("ALTER TABLE bronze.price_history_raw DROP COLUMN IF EXISTS block_reason;")
    op.execute("ALTER TABLE bronze.price_history_raw DROP COLUMN IF EXISTS blocked;")
    op.execute("ALTER TABLE bronze.price_history_raw DROP COLUMN IF EXISTS avg_price;")
    op.execute("ALTER TABLE bronze.price_history_raw DROP COLUMN IF EXISTS source_mode;")
    op.execute("ALTER TABLE bronze.price_history_raw DROP COLUMN IF EXISTS query;")
