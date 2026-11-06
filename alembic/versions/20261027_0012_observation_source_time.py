"""Keep each observation's official source time and route for safe replay.

Only isolated test/rehearsal databases may receive this additive revision.
The legacy observations remain NULL/legacy rather than gaining invented dates.
"""

from alembic import op


revision = '20261027_0012'
down_revision = '20261027_0011'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""DO $$ BEGIN
      IF current_database() !~ '^(test|rehearsal)_[0-9]{8}_[a-f0-9]{12}$' THEN
        RAISE EXCEPTION 'source observation migration is isolated-only';
      END IF;
    END $$""")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
    ALTER TABLE procurement.version_observations
      ADD COLUMN source_updated_at TIMESTAMPTZ,
      ADD COLUMN source_time_text TEXT,
      ADD COLUMN observed_resource TEXT NOT NULL DEFAULT 'legacy';
    CREATE INDEX ix_observation_source_time
      ON procurement.version_observations(procurement_id,source_updated_at DESC)
      WHERE source_updated_at IS NOT NULL;
    """)


def downgrade():
    raise RuntimeError('Observed source chronology is retained; destructive downgrade forbidden')
