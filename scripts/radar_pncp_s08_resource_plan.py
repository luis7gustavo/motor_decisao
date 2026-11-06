"""Read-only sizing and contract audit for historical PNCP enrichment.

This command does not enqueue work, change policy, or call PNCP. The counts are
observations of an unfinished historical manifest, not a completeness claim.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from sqlalchemy import text

from app.procurement.source_control import _daily_window
from app.procurement.source_policy import load_source_policy
from scripts.radar_pncp_history import target_store


POLICY = Path('config/contracts/pncp-history-mission-policy.json')
CONTRACTS = {
    'consulta': (
        Path('config/contracts/pncp-consulta-20261028.json'),
        Path('/evidence/pncp-docs/20261028/openapi-receipt.json')),
    'integracao': (
        Path('config/contracts/pncp-integracao-20261028.json'),
        Path('/evidence/pncp-docs/20261028/integration-openapi-receipt.json')),
}
ROUTES = {
    'procurement_detail': ('consulta', '/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}'),
    'items': ('integracao', '/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens'),
    'results': ('integracao', '/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens/{numeroItem}/resultados'),
    'documents_index': ('integracao', '/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos'),
    'documents_binary': ('integracao', '/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos/{sequencialDocumento}'),
}


def contract_routes(documents):
    """Require GET and all path variables in the archived OpenAPI operations."""
    result = {}
    for kind, (source, path) in ROUTES.items():
        operation = documents[source].get('paths', {}).get(path, {}).get('get')
        if not isinstance(operation, dict):
            raise ValueError(f'Missing archived GET contract: {kind}')
        path_parameters = {
            p.get('name') for p in operation.get('parameters', [])
            if p.get('in') == 'path' and p.get('required') is True
        }
        variables = set(re.findall(r'{([^{}]+)}', path))
        if variables != path_parameters or '200' not in operation.get('responses', {}):
            raise ValueError(f'Incomplete archived GET contract: {kind}')
        result[kind] = {'contract': source, 'path': path,
                        'path_parameters': sorted(path_parameters),
                        'get_documented': True}
    return result


def load_verified_contracts(policy):
    documents, fingerprints = {}, {}
    for source, (path, receipt_path) in CONTRACTS.items():
        raw = path.read_bytes()
        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
        digest = hashlib.sha256(raw).hexdigest()
        if (receipt.get('http_status') != 200 or receipt.get('bytes') != len(raw)
                or receipt.get('sha256') != digest):
            raise ValueError(f'Archived {source} contract receipt drift')
        if source == 'consulta' and digest != policy['contract_sha256']:
            raise ValueError('Mission consultation contract hash drift')
        document = json.loads(raw)
        if document.get('openapi') != receipt.get('openapi_version'):
            raise ValueError(f'Archived {source} OpenAPI version drift')
        documents[source] = document
        fingerprints[source] = digest
    return documents, fingerprints


def observe(engine, policy):
    mission_key = policy['mission_id']
    scope_key = policy['mission_budget']['stable_budget_scope_key']
    day_start, day_end = _daily_window(datetime.now(timezone.utc), policy['timezone'])
    with engine.connect() as conn:
        conn.exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        budget = dict(conn.execute(text("""SELECT scope_key,attempt_limit,attempts_consumed,enabled
            FROM control.source_budgets WHERE scope_key=:key"""),
            {'key': scope_key}).mappings().one())
        mission = dict(conn.execute(text("""SELECT id,mission_key,attempt_limit,
            attempts_consumed,state FROM control.source_missions WHERE mission_key=:key"""),
            {'key': mission_key}).mappings().one())
        identified = dict(conn.execute(text("""WITH historical_ids AS (
            SELECT DISTINCT i.source_id FROM control.source_partition_identities i
            JOIN control.source_partitions part ON part.id=i.partition_id
            WHERE part.mission_id=:id AND part.pass_no<100000)
            SELECT count(*) AS observed_ids,
                   count(*) FILTER (WHERE p.id IS NULL) AS missing_projection,
                   count(*) FILTER (WHERE v.id IS NULL) AS missing_current_version,
                   count(*) FILTER (WHERE v.snapshot #>> '{orgaoEntidade,cnpj}' ~ '^[[:alnum:]]{14}$'
                     AND v.snapshot->>'anoCompra' ~ '^[0-9]{4}$'
                     AND v.snapshot->>'sequencialCompra' ~ '^[0-9]+$') AS detail_route_candidates
            FROM historical_ids h
            LEFT JOIN procurement.procurements p ON p.source='pncp' AND p.source_id=h.source_id
            LEFT JOIN procurement.procurement_versions v ON v.id=p.current_version_id"""),
            {'id': mission['id']}).mappings().one())
        daily_used = conn.execute(text("""SELECT count(*) FROM control.source_attempts
            WHERE budget_scope_key=:key AND reserved_at>=:start AND reserved_at<:end"""),
            {'key': scope_key, 'start': day_start, 'end': day_end}).scalar_one()
        unresolved = conn.execute(text("""SELECT count(*) FROM control.source_attempts a
            LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
            WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL"""),
            {'key': scope_key}).scalar_one()
        queued = [dict(row) for row in conn.execute(text("""SELECT work_kind,state,count(*) AS jobs
            FROM control.source_queue WHERE mission_id=:id AND work_kind LIKE 'enrich:%'
            GROUP BY work_kind,state ORDER BY work_kind,state"""),
            {'id': mission['id']}).mappings()]
    remaining = min(mission['attempt_limit'] - mission['attempts_consumed'],
                    budget['attempt_limit'] - budget['attempts_consumed'])
    daily_limit = policy['mission_budget']['daily_attempt_limit']
    return {'observed_historical': identified, 'enrichment_queue': queued,
            'budget': {'mission_consumed': mission['attempts_consumed'],
                       'mission_limit': mission['attempt_limit'],
                       'source_consumed': budget['attempts_consumed'],
                       'source_limit': budget['attempt_limit'],
                       'shared_attempts_today': daily_used,
                       'daily_limit': daily_limit,
                       'unresolved_outcomes': unresolved,
                       'remaining_cumulative_upper_bound': remaining,
                       'remaining_today_upper_bound': max(0, daily_limit - daily_used)},
            'detail_one_call_per_observed_identity_exceeds_remaining': (
                identified['detail_route_candidates'] > remaining)}


def main():
    argparse.ArgumentParser(description='Read-only PNCP S08 enrichment contract and budget sizing').parse_args()
    policy = load_source_policy(POLICY)
    documents, fingerprints = load_verified_contracts(policy)
    routes = contract_routes(documents)
    _, engine = target_store()
    try:
        observation = observe(engine, policy)
    finally:
        engine.dispose()
    print(json.dumps({'state': 'resource_gates_pending',
                      'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                      'mission': policy['mission_id'],
                      'contract_sha256': fingerprints,
                      'documented_routes': routes,
                      'resource_gates': {kind: policy['resource_gates'][kind]
                                         for kind in ('procurement_detail', 'items', 'results', 'documents')},
                      **observation, 'pncp_network_calls': 0, 'database_writes': 0},
                     sort_keys=True, default=str))


if __name__ == '__main__':
    main()
