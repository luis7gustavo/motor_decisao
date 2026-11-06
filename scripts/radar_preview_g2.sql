-- G2: additive search schema ONLY in radar_preview_20261105, never in C2.
\set ON_ERROR_STOP on
DO $$ BEGIN
  IF current_database() <> 'radar_preview_20261105' THEN
    RAISE EXCEPTION 'wrong database: %', current_database();
  END IF;
END $$;
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(20261105, 2);
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE SCHEMA IF NOT EXISTS radar_preview;
CREATE TABLE IF NOT EXISTS radar_preview.discovery_current (
  source_id text PRIMARY KEY,
  procurement_id uuid NOT NULL UNIQUE,
  version_id uuid NOT NULL UNIQUE,
  version_no integer NOT NULL CHECK (version_no > 0),
  raw_response_id uuid NOT NULL,
  object_text text NOT NULL CHECK (length(btrim(object_text)) > 0),
  uf text NOT NULL CHECK (uf ~ '^[A-Z]{2}$'),
  modality text,
  official_state_raw text,
  deadline_text_unverified text
);
CREATE INDEX IF NOT EXISTS discovery_object_trgm_idx
  ON radar_preview.discovery_current USING gin (object_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS discovery_uf_modality_state_idx
  ON radar_preview.discovery_current (uf, modality, official_state_raw, source_id);
CREATE INDEX IF NOT EXISTS discovery_state_idx
  ON radar_preview.discovery_current (official_state_raw, source_id);
COMMIT;
