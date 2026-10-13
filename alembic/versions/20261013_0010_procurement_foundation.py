"""Public persistence foundation (RT-001/014, RF-003/004 partial).

Only creates new objects. No backfill or operational activation.
Downgrade is for the owned rehearsal database only.
"""
from alembic import op

revision = '20261013_0010'
down_revision = '20260817_0009'
branch_labels = None
depends_on = None


def upgrade():
    # Operational application remains explicitly disabled for this release.
    # Runner additionally checks the creation registry, server and database OID.
    op.execute("""DO $$ BEGIN
      IF current_database() !~ '^(test|rehearsal)_[0-9]{8}_[a-f0-9]{12}$' THEN
        RAISE EXCEPTION 'public foundation is currently isolated-only';
      END IF;
    END $$""")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("CREATE SCHEMA procurement")
    op.execute("CREATE SCHEMA commercial")
    op.execute("""
    CREATE TABLE commercial.system_settings (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      namespace TEXT NOT NULL, version_no INT NOT NULL CHECK(version_no>0),
      values JSONB NOT NULL CHECK(jsonb_typeof(values)='object'), schema_version TEXT NOT NULL,
      author TEXT NOT NULL CHECK(length(trim(author))>0), effective_at TIMESTAMPTZ NOT NULL,
      content_hash CHAR(64) NOT NULL CHECK(content_hash ~ '^[0-9a-f]{64}$'),
      UNIQUE(namespace,version_no));
    CREATE INDEX ix_settings_effective ON commercial.system_settings(effective_at);
    CREATE TABLE control.ingestion_jobs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      pipeline_run_id UUID NOT NULL REFERENCES control.pipeline_runs(id) ON DELETE RESTRICT,
      scope_key TEXT NOT NULL, window_start DATE NOT NULL, window_end DATE NOT NULL,
      next_page INT NOT NULL DEFAULT 1 CHECK(next_page>=1), state VARCHAR(20) NOT NULL DEFAULT 'created'
        CHECK(state IN ('created','running','partial','success','failed','cancelled')),
      lease_until TIMESTAMPTZ, lease_owner TEXT, attempts INT NOT NULL DEFAULT 0 CHECK(attempts>=0),
      checkpoint JSONB NOT NULL DEFAULT '{}', error_code TEXT,
      source TEXT NOT NULL, resource TEXT NOT NULL,
      policy_id UUID NOT NULL REFERENCES commercial.system_settings(id) ON DELETE RESTRICT,
      request_count INT NOT NULL DEFAULT 0 CHECK(request_count>=0), bytes_seen BIGINT NOT NULL DEFAULT 0 CHECK(bytes_seen>=0),
      records_seen INT NOT NULL DEFAULT 0 CHECK(records_seen>=0), fencing_token BIGINT NOT NULL DEFAULT 0 CHECK(fencing_token>=0),
      last_success_at TIMESTAMPTZ, CHECK(window_start<=window_end), UNIQUE(pipeline_run_id,scope_key));
    CREATE INDEX ix_jobs_policy ON control.ingestion_jobs(policy_id);
    CREATE INDEX ix_jobs_lease ON control.ingestion_jobs(state,lease_until);
    CREATE INDEX ix_jobs_source ON control.ingestion_jobs(source,resource,state);
    CREATE TABLE bronze.pncp_responses (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      job_id UUID NOT NULL REFERENCES control.ingestion_jobs(id) ON DELETE RESTRICT,
      request_fingerprint CHAR(64) NOT NULL CHECK(request_fingerprint ~ '^[0-9a-f]{64}$'),
      page INT CHECK(page>=1), http_status SMALLINT NOT NULL CHECK(http_status BETWEEN 100 AND 599),
      received_at TIMESTAMPTZ NOT NULL, body BYTEA NOT NULL,
      body_sha256 CHAR(64) NOT NULL CHECK(body_sha256 ~ '^[0-9a-f]{64}$'), payload JSONB,
      headers_safe JSONB NOT NULL, contract_version TEXT NOT NULL, source TEXT NOT NULL, resource TEXT NOT NULL,
      contract_hash CHAR(64) CHECK(contract_hash ~ '^[0-9a-f]{64}$'), request_params_safe JSONB NOT NULL,
      schema_version TEXT NOT NULL, CHECK(encode(sha256(body),'hex')=body_sha256),
      UNIQUE(job_id,request_fingerprint,body_sha256));
    CREATE INDEX ix_raw_received ON bronze.pncp_responses(received_at);
    CREATE INDEX ix_raw_fingerprint ON bronze.pncp_responses(request_fingerprint);
    CREATE TABLE procurement.procurements (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      source TEXT NOT NULL CHECK(length(trim(source))>0), source_id TEXT NOT NULL CHECK(length(trim(source_id))>0),
      cnpj_original TEXT, cnpj TEXT, cnpj_validation_status TEXT NOT NULL DEFAULT 'pendente'
        CHECK(cnpj_validation_status IN ('validada','provisória','inválida','pendente')),
      cnpj_validator_version TEXT, purchase_year INT CHECK(purchase_year>0), purchase_sequence BIGINT CHECK(purchase_sequence>0),
      current_version_id UUID, UNIQUE(source,source_id),
      CHECK(cnpj_validation_status<>'validada' OR (cnpj IS NOT NULL AND cnpj_validator_version IS NOT NULL)));
    CREATE UNIQUE INDEX uq_procurement_fiscal ON procurement.procurements(source,cnpj,purchase_year,purchase_sequence)
      WHERE cnpj_validation_status='validada' AND cnpj IS NOT NULL AND purchase_year IS NOT NULL AND purchase_sequence IS NOT NULL;
    CREATE INDEX ix_procurement_cnpj_year ON procurement.procurements(cnpj,purchase_year);
    CREATE INDEX ix_procurement_current ON procurement.procurements(current_version_id);
    CREATE TABLE procurement.procurement_versions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      procurement_id UUID NOT NULL REFERENCES procurement.procurements(id) ON DELETE RESTRICT,
      raw_response_id UUID NOT NULL REFERENCES bronze.pncp_responses(id) ON DELETE RESTRICT,
      version_no INT NOT NULL CHECK(version_no>0), content_hash CHAR(64) NOT NULL CHECK(content_hash ~ '^[0-9a-f]{64}$'),
      source_updated_at TIMESTAMPTZ, source_time_text TEXT, timezone_status VARCHAR(20) NOT NULL DEFAULT 'unknown',
      observed_at TIMESTAMPTZ NOT NULL, snapshot JSONB NOT NULL, schema_version TEXT NOT NULL,
      deadline_at TIMESTAMPTZ, official_status TEXT,
      publication_state VARCHAR(20) NOT NULL DEFAULT 'rascunho' CHECK(publication_state IN ('rascunho','publicada')),
      source_manifest JSONB NOT NULL, canonicalizer_version TEXT NOT NULL,
      UNIQUE(procurement_id,version_no), UNIQUE(procurement_id,id));
    CREATE UNIQUE INDEX uq_version_content ON procurement.procurement_versions(procurement_id,content_hash) WHERE publication_state='publicada';
    CREATE INDEX ix_version_raw ON procurement.procurement_versions(raw_response_id);
    CREATE INDEX ix_version_deadline ON procurement.procurement_versions(deadline_at);
    CREATE INDEX ix_version_observed ON procurement.procurement_versions(procurement_id,observed_at);
    ALTER TABLE procurement.procurements ADD CONSTRAINT fk_current_version
      FOREIGN KEY(id,current_version_id) REFERENCES procurement.procurement_versions(procurement_id,id) ON DELETE RESTRICT;
    CREATE TABLE procurement.procurement_items (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      version_id UUID NOT NULL REFERENCES procurement.procurement_versions(id) ON DELETE RESTRICT,
      source_item_key TEXT NOT NULL, description TEXT NOT NULL,
      quantity NUMERIC(24,6) CHECK(quantity>0 AND quantity<'Infinity'::numeric), unit TEXT,
      attributes JSONB NOT NULL DEFAULT '{}', estimated_unit_price NUMERIC(24,6)
        CHECK(estimated_unit_price>0 AND estimated_unit_price<'Infinity'::numeric),
      price_status VARCHAR(20) NOT NULL, UNIQUE(version_id,source_item_key), UNIQUE(version_id,id));
    CREATE INDEX ix_item_unit ON procurement.procurement_items(unit);
    CREATE TABLE procurement.judgment_units (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      version_id UUID NOT NULL REFERENCES procurement.procurement_versions(id) ON DELETE RESTRICT,
      source_unit_key TEXT NOT NULL, kind VARCHAR(12) NOT NULL CHECK(kind IN ('item','lote','grupo','global','desconhecido')),
      criterion TEXT, composition_status VARCHAR(20) NOT NULL,
      UNIQUE(version_id,source_unit_key), UNIQUE(version_id,id));
    CREATE TABLE procurement.judgment_unit_items (
      unit_id UUID NOT NULL REFERENCES procurement.judgment_units(id) ON DELETE RESTRICT,
      item_id UUID NOT NULL REFERENCES procurement.procurement_items(id) ON DELETE RESTRICT,
      version_id UUID NOT NULL REFERENCES procurement.procurement_versions(id) ON DELETE RESTRICT,
      required BOOLEAN NOT NULL, PRIMARY KEY(unit_id,item_id),
      FOREIGN KEY(version_id,unit_id) REFERENCES procurement.judgment_units(version_id,id) ON DELETE RESTRICT,
      FOREIGN KEY(version_id,item_id) REFERENCES procurement.procurement_items(version_id,id) ON DELETE RESTRICT);
    CREATE INDEX ix_unit_item ON procurement.judgment_unit_items(item_id);
    CREATE INDEX ix_unit_item_version ON procurement.judgment_unit_items(version_id);
    CREATE TABLE procurement.version_observations (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      procurement_id UUID NOT NULL REFERENCES procurement.procurements(id) ON DELETE RESTRICT,
      version_id UUID NOT NULL REFERENCES procurement.procurement_versions(id) ON DELETE RESTRICT,
      previous_version_id UUID REFERENCES procurement.procurement_versions(id) ON DELETE RESTRICT,
      raw_response_id UUID NOT NULL REFERENCES bronze.pncp_responses(id) ON DELETE RESTRICT,
      observed_at TIMESTAMPTZ NOT NULL, actor TEXT NOT NULL CHECK(length(trim(actor))>0),
      correlation_id UUID NOT NULL, changed BOOLEAN NOT NULL,
      FOREIGN KEY(procurement_id,version_id) REFERENCES procurement.procurement_versions(procurement_id,id) ON DELETE RESTRICT,
      FOREIGN KEY(procurement_id,previous_version_id) REFERENCES procurement.procurement_versions(procurement_id,id) ON DELETE RESTRICT,
      UNIQUE(procurement_id,raw_response_id,observed_at));
    CREATE INDEX ix_observation_version ON procurement.version_observations(version_id);
    CREATE INDEX ix_observation_previous ON procurement.version_observations(previous_version_id);
    CREATE INDEX ix_observation_raw ON procurement.version_observations(raw_response_id);
    """)
    op.execute("""
    CREATE FUNCTION procurement.reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'immutable public history'; END $$;
    CREATE TRIGGER immutable_raw BEFORE UPDATE OR DELETE ON bronze.pncp_responses FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    CREATE TRIGGER immutable_settings BEFORE UPDATE OR DELETE ON commercial.system_settings FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    CREATE TRIGGER immutable_observation BEFORE UPDATE OR DELETE ON procurement.version_observations FOR EACH ROW EXECUTE FUNCTION procurement.reject_mutation();
    CREATE FUNCTION procurement.guard_version() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='INSERT' THEN
        IF NEW.publication_state<>'rascunho' THEN RAISE EXCEPTION 'insert a draft before publishing'; END IF;
        RETURN NEW;
      END IF;
      IF OLD.publication_state='publicada' THEN RAISE EXCEPTION 'published version is immutable'; END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; END IF;
      IF NEW.publication_state='publicada' AND EXISTS (
        SELECT 1 FROM procurement.judgment_units u WHERE u.version_id=NEW.id AND u.kind='item'
        AND (SELECT count(*) FROM procurement.judgment_unit_items m WHERE m.unit_id=u.id)<>1
      ) THEN RAISE EXCEPTION 'item unit requires one member'; END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER immutable_version BEFORE INSERT OR UPDATE OR DELETE ON procurement.procurement_versions FOR EACH ROW EXECUTE FUNCTION procurement.guard_version();
    CREATE FUNCTION procurement.guard_child() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE state TEXT;
    BEGIN
      IF TG_OP<>'INSERT' THEN
        SELECT publication_state INTO state FROM procurement.procurement_versions WHERE id=OLD.version_id FOR SHARE;
        IF state='publicada' THEN RAISE EXCEPTION 'published child is immutable'; END IF;
      END IF;
      IF TG_OP<>'DELETE' THEN
        SELECT publication_state INTO state FROM procurement.procurement_versions WHERE id=NEW.version_id FOR SHARE;
        IF state='publicada' THEN RAISE EXCEPTION 'published child is immutable'; END IF;
        RETURN NEW;
      END IF;
      RETURN OLD;
    END $$;
    CREATE TRIGGER immutable_item BEFORE INSERT OR UPDATE OR DELETE ON procurement.procurement_items FOR EACH ROW EXECUTE FUNCTION procurement.guard_child();
    CREATE TRIGGER immutable_unit BEFORE INSERT OR UPDATE OR DELETE ON procurement.judgment_units FOR EACH ROW EXECUTE FUNCTION procurement.guard_child();
    CREATE TRIGGER immutable_membership BEFORE INSERT OR UPDATE OR DELETE ON procurement.judgment_unit_items FOR EACH ROW EXECUTE FUNCTION procurement.guard_child();
    CREATE FUNCTION procurement.guard_current() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.current_version_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM procurement.procurement_versions WHERE id=NEW.current_version_id
          AND procurement_id=NEW.id AND publication_state='publicada'
      ) THEN RAISE EXCEPTION 'current pointer requires own published version'; END IF;
      RETURN NEW;
    END $$;
    CREATE TRIGGER valid_current BEFORE INSERT OR UPDATE ON procurement.procurements FOR EACH ROW EXECUTE FUNCTION procurement.guard_current();
    """)


def downgrade():
    # Guard against accidental use on the operational database. The coordinator
    # separately verifies creation registry, OID and server before reaching here.
    op.execute("""DO $$ BEGIN
      IF current_database() !~ '^rehearsal_[0-9]{8}_[a-f0-9]{12}$' THEN
        RAISE EXCEPTION 'downgrade requires owned rehearsal database';
      END IF;
    END $$""")
    op.execute('ALTER TABLE procurement.procurements DROP CONSTRAINT fk_current_version')
    for table in ('procurement.version_observations','procurement.judgment_unit_items',
                  'procurement.judgment_units','procurement.procurement_items',
                  'procurement.procurement_versions','procurement.procurements',
                  'bronze.pncp_responses','control.ingestion_jobs','commercial.system_settings'):
        op.execute('DROP TABLE ' + table)
    for name in ('guard_current','guard_child','guard_version','reject_mutation'):
        op.execute('DROP FUNCTION procurement.' + name + '()')
    op.execute('DROP SCHEMA procurement')
    op.execute('DROP SCHEMA commercial')
