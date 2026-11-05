"""Aggregate Gate A evidence from the segregated PNCP C2 target.

Run this source through the dedicated operations container, where
``target_store`` verifies the C2 target. The script sends no PNCP requests,
does not export procurement rows, and opens a read-only transaction.
"""

import json
from datetime import datetime, timezone

from sqlalchemy import text

from scripts.radar_pncp_history import target_store


FIELDS = """
SELECT count(*) AS identities,
       count(*) FILTER (WHERE v.id IS NULL) AS missing_current_version,
       count(*) FILTER (WHERE v.snapshot->>'numeroControlePNCP' IS DISTINCT FROM p.source_id)
           AS identity_mismatch,
       count(*) FILTER (WHERE nullif(v.snapshot->>'objetoCompra', '') IS NOT NULL)
           AS object_present,
       count(*) FILTER (WHERE nullif(v.snapshot->>'anoCompra', '') IS NOT NULL)
           AS year_present_raw,
       count(*) FILTER (WHERE nullif(v.snapshot->>'dataEncerramentoProposta', '') IS NOT NULL)
           AS deadline_present_raw,
       count(*) FILTER (WHERE v.deadline_at IS NOT NULL) AS deadline_normalized,
       count(*) FILTER (WHERE nullif(v.snapshot->>'valorTotalEstimado', '') IS NOT NULL)
           AS estimated_value_present,
       count(*) FILTER (WHERE nullif(v.snapshot->'unidadeOrgao'->>'ufSigla', '') IS NOT NULL)
           AS uf_present,
       count(*) FILTER (WHERE v.source_updated_at IS NOT NULL) AS source_updated_at_present,
       count(*) FILTER (WHERE v.timezone_status = 'unknown') AS timezone_unknown,
       count(DISTINCT v.snapshot->>'modalidadeId') AS modalities,
       count(DISTINCT v.snapshot->>'situacaoCompraId') AS official_states,
       count(DISTINCT v.snapshot->'unidadeOrgao'->>'ufSigla') AS ufs
FROM procurement.procurements p
LEFT JOIN procurement.procurement_versions v ON v.id = p.current_version_id
WHERE p.source = 'pncp'
"""

HISTORY = """
WITH mission AS (
    SELECT id FROM control.source_missions
    WHERE mission_key = 'pncp-history-2026-10-27'
), historical AS (
    SELECT DISTINCT i.source_id
    FROM control.source_partition_identities i
    JOIN control.source_partitions p ON p.id = i.partition_id
    JOIN mission m ON m.id = p.mission_id
    WHERE p.pass_no < 100000
)
SELECT count(*) AS historical_identities,
       count(*) FILTER (WHERE p.id IS NULL) AS missing_projection
FROM historical h
LEFT JOIN procurement.procurements p
  ON p.source = 'pncp' AND p.source_id = h.source_id
"""

REFERENTIAL = """
SELECT (SELECT count(*) FROM (
           SELECT source_id FROM procurement.procurements
           WHERE source = 'pncp' GROUP BY source_id HAVING count(*) > 1
       ) duplicates) AS duplicate_source_ids,
       (SELECT count(*) FROM procurement.procurement_versions v
        LEFT JOIN procurement.procurements p ON p.id = v.procurement_id
        WHERE p.id IS NULL) AS orphan_versions,
       (SELECT count(*) FROM procurement.procurement_versions v
        LEFT JOIN bronze.pncp_responses b ON b.id = v.raw_response_id
        WHERE b.id IS NULL) AS versions_without_bronze,
       (SELECT count(*) FROM procurement.version_observations o
        LEFT JOIN procurement.procurement_versions v ON v.id = o.version_id
        WHERE v.id IS NULL) AS orphan_observations
"""


def main():
    store, engine = target_store()
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            fields = dict(conn.execute(text(FIELDS)).mappings().one())
            history = dict(conn.execute(text(HISTORY)).mappings().one())
            referential = dict(conn.execute(text(REFERENTIAL)).mappings().one())
            extras = dict(conn.execute(text("""
                SELECT (SELECT count(*) FROM procurement.procurement_items) AS items,
                       (SELECT count(*) FROM procurement.judgment_unit_items) AS judgment_items,
                       (SELECT count(*) FROM procurement.procurement_versions
                        WHERE version_no > 1) AS later_versions
            """)).mappings().one())
            states = [dict(row) for row in conn.execute(text("""
                SELECT v.snapshot->>'situacaoCompraNome' AS name, count(*) AS identities
                FROM procurement.procurements p
                JOIN procurement.procurement_versions v ON v.id = p.current_version_id
                WHERE p.source = 'pncp'
                GROUP BY 1 ORDER BY identities DESC
            """)).mappings()]
            print(json.dumps({'observed_utc': datetime.now(timezone.utc).isoformat(),
                              'fields': fields, 'history': history,
                              'referential': referential,
                              'extras': extras, 'official_state_names': states,
                              'pncp_network_calls': 0, 'database_writes': 0},
                             sort_keys=True))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
