"""Add bounded, shared source control without changing legacy procurement history.

Only isolated test/rehearsal databases may receive this revision. History and
attempt reservations are retained; downgrade intentionally refuses deletion.
"""

from alembic import op


revision = '20261027_0011'
down_revision = '20261013_0010'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""DO $$ BEGIN
      IF current_database() !~ '^(test|rehearsal)_[0-9]{8}_[a-f0-9]{12}$' THEN
        RAISE EXCEPTION 'source control migration is isolated-only';
      END IF;
    END $$""")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
    ALTER TABLE procurement.version_observations ADD COLUMN ordering_status VARCHAR(32)
      NOT NULL DEFAULT 'legacy' CHECK(ordering_status IN
      ('legacy','initial','same_content','source_newer','stale_replay',
       'ambiguous_equal','unknown_source_time','cross_resource_pending'));

    CREATE TABLE control.source_budgets (
      scope_key TEXT PRIMARY KEY, source TEXT NOT NULL, restriction_scope TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT false,
      attempt_limit BIGINT NOT NULL DEFAULT 0 CHECK(attempt_limit>=0),
      attempts_consumed BIGINT NOT NULL DEFAULT 0 CHECK(attempts_consumed>=0 AND attempts_consumed<=attempt_limit),
      min_interval_ms INT NOT NULL DEFAULT 1000 CHECK(min_interval_ms>=0),
      max_concurrency INT NOT NULL DEFAULT 1 CHECK(max_concurrency>0),
      not_before TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
      next_allowed_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(source,restriction_scope));

    CREATE TABLE control.source_missions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), mission_key TEXT NOT NULL UNIQUE,
      source TEXT NOT NULL, budget_scope_key TEXT NOT NULL
        REFERENCES control.source_budgets(scope_key) ON DELETE RESTRICT,
      contract_hash CHAR(64) NOT NULL CHECK(contract_hash ~ '^[0-9a-f]{64}$'),
      coverage_start DATE, fixed_end DATE NOT NULL,
      state VARCHAR(12) NOT NULL DEFAULT 'blocked' CHECK(state IN ('blocked','enabled','paused')),
      scope JSONB NOT NULL CHECK(jsonb_typeof(scope)='object'),
      attempt_limit BIGINT NOT NULL DEFAULT 0 CHECK(attempt_limit>=0),
      attempts_consumed BIGINT NOT NULL DEFAULT 0 CHECK(attempts_consumed>=0 AND attempts_consumed<=attempt_limit),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CHECK(coverage_start IS NULL OR coverage_start<=fixed_end));
    CREATE INDEX ix_source_missions_budget ON control.source_missions(budget_scope_key);

    CREATE TABLE control.source_partitions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      mission_id UUID NOT NULL REFERENCES control.source_missions(id) ON DELETE RESTRICT,
      parent_id UUID REFERENCES control.source_partitions(id) ON DELETE RESTRICT,
      scope_key CHAR(64) NOT NULL CHECK(scope_key ~ '^[0-9a-f]{64}$'),
      resource TEXT NOT NULL, modality_code BIGINT,
      window_start DATE NOT NULL, window_end DATE NOT NULL,
      pass_no INT NOT NULL DEFAULT 1 CHECK(pass_no>0),
      state VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','running','partial','complete','blocked','reconcile')),
      next_page INT NOT NULL DEFAULT 1 CHECK(next_page>0),
      priority INT NOT NULL DEFAULT 100,
      lease_owner TEXT, lease_until TIMESTAMPTZ,
      fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token>=0),
      replays_started INT NOT NULL DEFAULT 0 CHECK(replays_started>=0),
      max_replays INT NOT NULL DEFAULT 2 CHECK(max_replays>=0),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(mission_id,scope_key),
      CHECK(window_start<=window_end AND replays_started<=max_replays),
      CHECK(parent_id IS NULL OR parent_id<>id));
    CREATE INDEX ix_source_partitions_mission_state ON control.source_partitions(mission_id,state,priority,created_at);
    CREATE INDEX ix_source_partitions_parent ON control.source_partitions(parent_id);

    CREATE TABLE control.source_partition_identities (
      partition_id UUID NOT NULL REFERENCES control.source_partitions(id) ON DELETE RESTRICT,
      pass_no INT NOT NULL CHECK(pass_no>0),
      source_id TEXT NOT NULL CHECK(length(trim(source_id))>0),
      raw_response_id UUID NOT NULL REFERENCES bronze.pncp_responses(id) ON DELETE RESTRICT,
      observed_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY(partition_id,pass_no,source_id));
    CREATE INDEX ix_source_partition_identities_raw ON control.source_partition_identities(raw_response_id);

    CREATE TABLE control.source_watermarks (
      mission_id UUID NOT NULL REFERENCES control.source_missions(id) ON DELETE RESTRICT,
      resource TEXT NOT NULL, partition_key TEXT NOT NULL,
      contiguous_through DATE, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY(mission_id,resource,partition_key));

    CREATE TABLE control.source_queue (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      mission_id UUID NOT NULL REFERENCES control.source_missions(id) ON DELETE RESTRICT,
      partition_id UUID REFERENCES control.source_partitions(id) ON DELETE RESTRICT,
      work_kind TEXT NOT NULL, idempotency_key CHAR(64) NOT NULL
        CHECK(idempotency_key ~ '^[0-9a-f]{64}$'),
      identity_key TEXT, payload JSONB NOT NULL DEFAULT '{}'
        CHECK(jsonb_typeof(payload)='object' AND octet_length(payload::text)<=8192),
      state VARCHAR(16) NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','running','complete','blocked','failed')),
      priority INT NOT NULL DEFAULT 100,
      not_before TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
      lease_owner TEXT, lease_until TIMESTAMPTZ,
      fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token>=0),
      attempts INT NOT NULL DEFAULT 0 CHECK(attempts>=0),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(mission_id,work_kind,idempotency_key));
    CREATE INDEX ix_source_queue_ready ON control.source_queue(mission_id,state,not_before,priority,created_at);
    CREATE INDEX ix_source_queue_partition ON control.source_queue(partition_id);

    CREATE TABLE control.source_attempts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      budget_scope_key TEXT NOT NULL REFERENCES control.source_budgets(scope_key) ON DELETE RESTRICT,
      mission_id UUID NOT NULL REFERENCES control.source_missions(id) ON DELETE RESTRICT,
      job_id UUID REFERENCES control.ingestion_jobs(id) ON DELETE RESTRICT,
      sequence_no BIGINT NOT NULL CHECK(sequence_no>0),
      work_kind TEXT NOT NULL, request_hash CHAR(64) NOT NULL
        CHECK(request_hash ~ '^[0-9a-f]{64}$'),
      reserved_at TIMESTAMPTZ NOT NULL, lease_until TIMESTAMPTZ NOT NULL,
      CHECK(lease_until>reserved_at), UNIQUE(budget_scope_key,sequence_no));
    CREATE INDEX ix_source_attempts_mission ON control.source_attempts(mission_id,reserved_at);
    CREATE INDEX ix_source_attempts_job ON control.source_attempts(job_id);
    CREATE INDEX ix_source_attempts_active ON control.source_attempts(budget_scope_key,lease_until);

    CREATE TABLE control.source_attempt_outcomes (
      attempt_id UUID PRIMARY KEY REFERENCES control.source_attempts(id) ON DELETE RESTRICT,
      outcome TEXT NOT NULL CHECK(length(trim(outcome))>0),
      http_status SMALLINT CHECK(http_status BETWEEN 100 AND 599),
      bronze_response_id UUID REFERENCES bronze.pncp_responses(id) ON DELETE RESTRICT,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT now());
    CREATE INDEX ix_source_attempt_outcomes_bronze ON control.source_attempt_outcomes(bronze_response_id);

    CREATE TRIGGER immutable_source_attempt BEFORE UPDATE OR DELETE ON control.source_attempts
      FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    CREATE TRIGGER immutable_source_outcome BEFORE UPDATE OR DELETE ON control.source_attempt_outcomes
      FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    CREATE TRIGGER immutable_partition_identity BEFORE UPDATE OR DELETE ON control.source_partition_identities
      FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    """)


def downgrade():
    raise RuntimeError('Source history and attempt reservations have no destructive downgrade')
