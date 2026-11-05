"""Bounded, read-only C2 sample for validating Radar Licita discovery fields.

Pass this file to ``python -`` in the dedicated operations container. It is
an operator diagnostic, not the product API and not an opportunity classifier.
"""

import argparse
import json
from datetime import datetime, timezone

from sqlalchemy import text

from scripts.radar_pncp_history import target_store


QUERY = """
SELECT p.source_id, v.version_no,
       v.snapshot->>'objetoCompra' AS object,
       v.snapshot->'unidadeOrgao'->>'ufSigla' AS uf,
       v.snapshot->>'modalidadeNome' AS modality,
       v.snapshot->>'situacaoCompraNome' AS official_state_raw,
       v.snapshot->>'dataEncerramentoProposta' AS deadline_text_unverified
FROM procurement.procurements p
JOIN procurement.procurement_versions v ON v.id = p.current_version_id
WHERE p.source = 'pncp'
  AND (:q = '' OR v.snapshot->>'objetoCompra' ILIKE :pattern ESCAPE '\\')
  AND (:uf = '' OR v.snapshot->'unidadeOrgao'->>'ufSigla' = :uf)
ORDER BY p.source_id
LIMIT :limit
"""


def literal_pattern(value):
    return '%' + value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--q', default='')
    parser.add_argument('--uf', default='')
    parser.add_argument('--limit', type=int, default=10)
    args = parser.parse_args()
    if len(args.q) > 80 or (args.uf and (len(args.uf) != 2 or not args.uf.isalpha())):
        parser.error('Invalid query or UF')
    if not 1 <= args.limit <= 20:
        parser.error('Limit must be 1..20')
    store, engine = target_store()
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            rows = [dict(row) for row in conn.execute(text(QUERY), {
                'q': args.q, 'pattern': literal_pattern(args.q),
                'uf': args.uf.upper(), 'limit': args.limit,
            }).mappings()]
        print(json.dumps({'observed_utc': datetime.now(timezone.utc).isoformat(),
                          'rows': rows, 'freshness': 'unknown',
                          'opportunity_open': None, 'coverage': 'partial',
                          'pncp_network_calls': 0, 'database_writes': 0},
                         ensure_ascii=False, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
