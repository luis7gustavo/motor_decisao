-- G3: idempotent backfill from the restored C2 current-version pointer.
\set ON_ERROR_STOP on
DO $$ BEGIN
  IF current_database() <> 'radar_preview_20261105' THEN
    RAISE EXCEPTION 'wrong database: %', current_database();
  END IF;
END $$;
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '300s';
SELECT pg_advisory_xact_lock(20261105, 3);
-- If the current pointer lags a newer persisted version, quarantine the identity.
DELETE FROM radar_preview.discovery_current d
WHERE NOT EXISTS (
  SELECT 1 FROM procurement.procurements p
  WHERE p.id = d.procurement_id AND p.current_version_id = d.version_id
)
OR EXISTS (
  SELECT 1 FROM procurement.procurement_versions newer
  WHERE newer.procurement_id = d.procurement_id AND newer.version_no > d.version_no
);
INSERT INTO radar_preview.discovery_current
  (source_id, procurement_id, version_id, version_no, raw_response_id,
   object_text, uf, modality, official_state_raw, deadline_text_unverified)
SELECT p.source_id, p.id, v.id, v.version_no, v.raw_response_id,
       btrim(v.snapshot->>'objetoCompra'),
       upper(v.snapshot->'unidadeOrgao'->>'ufSigla'),
       v.snapshot->>'modalidadeNome',
       v.snapshot->>'situacaoCompraNome',
       v.snapshot->>'dataEncerramentoProposta'
FROM procurement.procurements p
JOIN procurement.procurement_versions v
  ON v.id = p.current_version_id AND v.procurement_id = p.id
WHERE p.source = 'pncp'
  AND v.snapshot->>'numeroControlePNCP' = p.source_id
  AND NOT EXISTS (
    SELECT 1 FROM procurement.procurement_versions newer
    WHERE newer.procurement_id = p.id AND newer.version_no > v.version_no
  )
ON CONFLICT (source_id) DO UPDATE SET
  procurement_id = EXCLUDED.procurement_id,
  version_id = EXCLUDED.version_id,
  version_no = EXCLUDED.version_no,
  raw_response_id = EXCLUDED.raw_response_id,
  object_text = EXCLUDED.object_text,
  uf = EXCLUDED.uf,
  modality = EXCLUDED.modality,
  official_state_raw = EXCLUDED.official_state_raw,
  deadline_text_unverified = EXCLUDED.deadline_text_unverified
WHERE (radar_preview.discovery_current.version_id,
       radar_preview.discovery_current.object_text,
       radar_preview.discovery_current.uf,
       radar_preview.discovery_current.modality,
       radar_preview.discovery_current.official_state_raw,
       radar_preview.discovery_current.deadline_text_unverified)
  IS DISTINCT FROM (EXCLUDED.version_id, EXCLUDED.object_text, EXCLUDED.uf,
                    EXCLUDED.modality, EXCLUDED.official_state_raw,
                    EXCLUDED.deadline_text_unverified);
DO $$
DECLARE expected bigint; actual bigint; bad bigint; total_source bigint;
BEGIN
  SELECT count(*) INTO total_source FROM procurement.procurements WHERE source = 'pncp';
  SELECT count(*) INTO expected
  FROM procurement.procurements p
  JOIN procurement.procurement_versions v ON v.id = p.current_version_id
  WHERE p.source = 'pncp'
    AND v.procurement_id = p.id
    AND v.snapshot->>'numeroControlePNCP' = p.source_id
    AND NOT EXISTS (
      SELECT 1 FROM procurement.procurement_versions newer
      WHERE newer.procurement_id = p.id AND newer.version_no > v.version_no
    );
  SELECT count(*) INTO actual FROM radar_preview.discovery_current;
  IF actual <> expected THEN
    RAISE EXCEPTION 'projection count % differs from source %', actual, expected;
  END IF;
  SELECT count(*) INTO bad
  FROM radar_preview.discovery_current d
  LEFT JOIN procurement.procurements p ON p.id = d.procurement_id
  LEFT JOIN procurement.procurement_versions v ON v.id = d.version_id
  WHERE p.id IS NULL OR v.id IS NULL
     OR p.source IS DISTINCT FROM 'pncp'
     OR p.source_id IS DISTINCT FROM d.source_id
     OR p.current_version_id IS DISTINCT FROM d.version_id
     OR v.procurement_id IS DISTINCT FROM p.id
     OR v.version_no IS DISTINCT FROM d.version_no
     OR v.raw_response_id IS DISTINCT FROM d.raw_response_id
     OR v.snapshot->>'numeroControlePNCP' IS DISTINCT FROM d.source_id
     OR btrim(v.snapshot->>'objetoCompra') IS DISTINCT FROM d.object_text
     OR upper(v.snapshot->'unidadeOrgao'->>'ufSigla') IS DISTINCT FROM d.uf
     OR v.snapshot->>'modalidadeNome' IS DISTINCT FROM d.modality
     OR v.snapshot->>'situacaoCompraNome' IS DISTINCT FROM d.official_state_raw
     OR v.snapshot->>'dataEncerramentoProposta' IS DISTINCT FROM d.deadline_text_unverified;
  IF bad <> 0 THEN RAISE EXCEPTION 'projection mismatches: %', bad; END IF;
  RAISE NOTICE 'source %, projected %, quarantined %', total_source, actual,
               total_source - actual;
END $$;
COMMIT;
ANALYZE radar_preview.discovery_current;
