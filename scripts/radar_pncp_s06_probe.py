"""Bounded live PNCP read probes on the verified isolated target.

No automatic retry or redirect. Every request consumes one durable source attempt
before DNS. Receipts and raw bytes go to ignored evidence, never stdout.
"""

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

from sqlalchemy import text

from app.procurement.pncp_transport import durable_write, send_get
from app.procurement.source_control import SourceControl
from scripts.radar_pncp_history import target_store


CONSULTATION = Path('config/contracts/pncp-consulta-20261028.json')
INTEGRATION = Path('config/contracts/pncp-integracao-20261028.json')
CONSULTATION_HASH = 'b2c1fb8874861f63ebf7c065756f82c04dc4a84c8def63e5b4aa641d5edb4430'
INTEGRATION_HASH = 'd721ad1c59a82f5dea3ced7ce4e92dc51da91096c2041382a567aa5773a57aa2'
BUDGET_KEY = 'pncp:public-read:shared-docker-egress'
RESTRICTION = 'unauthenticated-public-read-shared-docker-egress'
MISSION_KEY = 'pncp-s06-probe-2026-10-28'
EVIDENCE = Path('/evidence/pncp-probe/20261028')
ATTEMPT_LIMIT = 12
SOURCE_BUDGET_LIMIT = 12
MISSION_KIND = 'bounded_s06_probe'
REQUEST_TIMEOUT_SECONDS = 30


def checked_contracts():
    for path, expected in ((CONSULTATION, CONSULTATION_HASH), (INTEGRATION, INTEGRATION_HASH)):
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('Archived official contract hash drift')


def request_url(args):
    if args.command == 'catalog':
        url = 'https://pncp.gov.br/api/pncp/v1/modalidades'
        return url + '?statusAtivo=false' if getattr(args, 'inactive', False) else url
    if args.command in ('publication', 'update'):
        if not args.start or not args.end or args.modality is None:
            raise ValueError('Listing needs --start, --end and --modality')
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if start > end or end > date(2026, 10, 26):
            raise ValueError('Probe window invalid or exceeds fixed mission end')
        if args.modality < 1 or args.page < 1:
            raise ValueError('Positive modality and page required')
        path = ('publicacao' if args.command == 'publication' else 'atualizacao')
        query = urlencode({'dataInicial': start.strftime('%Y%m%d'),
                           'dataFinal': end.strftime('%Y%m%d'),
                           'codigoModalidadeContratacao': args.modality,
                           'pagina': args.page, 'tamanhoPagina': 10})
        return 'https://pncp.gov.br/api/consulta/v1/contratacoes/' + path + '?' + query
    if not args.cnpj or not args.year or not args.sequence:
        raise ValueError('Resource probe needs --cnpj, --year and --sequence')
    if len(args.cnpj) != 14 or not args.cnpj.isdigit() or args.year < 2021 or args.sequence < 1:
        raise ValueError('Invalid official procurement identity')
    base = f'https://pncp.gov.br/api/pncp/v1/orgaos/{args.cnpj}/compras/{args.year}/{args.sequence}'
    if args.command == 'detail':
        return f'https://pncp.gov.br/api/consulta/v1/orgaos/{args.cnpj}/compras/{args.year}/{args.sequence}'
    if args.command == 'items':
        return base + '/itens?' + urlencode({'pagina': args.page, 'tamanhoPagina': 10})
    if args.command == 'results':
        if args.item is None or args.item < 1:
            raise ValueError('Results needs positive --item from observed item response')
        return base + f'/itens/{args.item}/resultados'
    if args.command in ('files', 'history'):
        if args.page < 1:
            raise ValueError('Positive page required')
        suffix = 'arquivos' if args.command == 'files' else 'historico'
        return base + '/' + suffix + '?' + urlencode({'pagina': args.page, 'tamanhoPagina': 10})
    raise ValueError('Unknown probe command')


def display_status(store):
    with store.transaction() as session:
        budgets = session.execute(text('''SELECT scope_key,restriction_scope,attempt_limit,
            attempts_consumed,enabled,not_before,next_allowed_at FROM control.source_budgets
            WHERE source='pncp' ORDER BY scope_key''')).mappings().all()
        mission = session.execute(text('''SELECT id,mission_key,budget_scope_key,
            attempt_limit,attempts_consumed,state FROM control.source_missions
            WHERE mission_key=:key'''), {'key': MISSION_KEY}).mappings().one_or_none()
        attempts = None
        if mission:
            attempts = session.execute(text('''SELECT count(*) AS reserved,
                count(o.attempt_id) AS outcomes,
                count(*) FILTER (WHERE o.http_status=200) AS http_200,
                count(*) FILTER (WHERE o.http_status=204) AS http_204,
                count(*) FILTER (WHERE o.http_status=301) AS http_301
                FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.mission_id=:id'''), {'id': mission['id']}).mappings().one()
        result = {'budgets': [dict(row) for row in budgets],
                  'mission': dict(mission) if mission else None,
                  'attempts': dict(attempts) if attempts else None}
    print(json.dumps(result, sort_keys=True, default=str))


def register_probe(control, store):
    with store.transaction() as session:
        existing = session.execute(text('''SELECT scope_key,restriction_scope,attempt_limit,
            attempts_consumed,enabled FROM control.source_budgets WHERE source='pncp' '''
        )).mappings().all()
    if any(row['scope_key'] != BUDGET_KEY or row['restriction_scope'] != RESTRICTION
           for row in existing):
        raise ValueError('Another PNCP source budget exists; reconcile shared restriction first')
    control.register_budget(scope_key=BUDGET_KEY, source='pncp',
                             restriction_scope=RESTRICTION, attempt_limit=SOURCE_BUDGET_LIMIT,
                            min_interval_ms=1000, max_concurrency=1, enabled=True)
    return control.register_mission(mission_key=MISSION_KEY, source='pncp',
                                    budget_scope_key=BUDGET_KEY,
                                    contract_hash=CONSULTATION_HASH,
                                    fixed_end=date(2026, 10, 26),
                                    scope={'kind': MISSION_KIND,
                                           'integration_contract_hash': INTEGRATION_HASH,
                                           'max_attempts': ATTEMPT_LIMIT},
                                    attempt_limit=ATTEMPT_LIMIT, state='enabled')


def halt_probe(store):
    """Stop further sends without erasing consumed attempts or receipts."""
    with store.transaction() as session:
        session.execute(text('''UPDATE control.source_budgets SET enabled=false
            WHERE scope_key=:budget AND source='pncp' '''), {'budget': BUDGET_KEY})
        session.execute(text('''UPDATE control.source_missions SET state='paused'
            WHERE mission_key=:mission AND budget_scope_key=:budget'''),
            {'mission': MISSION_KEY, 'budget': BUDGET_KEY})


def resume_probe(store):
    """Resume the same capped budget after the reviewed route correction."""
    with store.transaction() as session:
        budget = session.execute(text('''SELECT * FROM control.source_budgets
            WHERE scope_key=:budget FOR UPDATE'''), {'budget': BUDGET_KEY}).mappings().one()
        mission = session.execute(text('''SELECT * FROM control.source_missions
            WHERE mission_key=:mission FOR UPDATE'''), {'mission': MISSION_KEY}).mappings().one()
        last = session.execute(text('''SELECT o.http_status FROM control.source_attempts a
            JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
            WHERE a.mission_id=:id ORDER BY a.sequence_no DESC LIMIT 1'''),
            {'id': mission['id']}).scalar_one()
        consumed = budget['attempts_consumed']
        if (budget['source'] != 'pncp' or budget['restriction_scope'] != RESTRICTION
                or budget['attempt_limit'] != ATTEMPT_LIMIT
                or budget['min_interval_ms'] != 1000 or budget['max_concurrency'] != 1
                or budget['enabled'] is not False or mission['budget_scope_key'] != BUDGET_KEY
                or mission['contract_hash'].strip() != CONSULTATION_HASH
                or mission['attempt_limit'] != ATTEMPT_LIMIT
                or mission['attempts_consumed'] != consumed or mission['state'] != 'paused'
                or (consumed, last) not in {(4, 301), (9, 204)}):
            raise ValueError('Paused probe budget or mission drift; refusing resume')
        session.execute(text('''UPDATE control.source_budgets SET enabled=true
            WHERE scope_key=:budget'''), {'budget': BUDGET_KEY})
        session.execute(text('''UPDATE control.source_missions SET state='enabled'
            WHERE mission_key=:mission'''), {'mission': MISSION_KEY})


def main():
    parser = argparse.ArgumentParser(description='PNCP S06 probe: 12 total durable attempts')
    parser.add_argument('command', choices=('status', 'pause', 'resume', 'catalog', 'publication', 'update',
                                            'detail', 'items', 'results', 'files', 'history'))
    parser.add_argument('--start')
    parser.add_argument('--end')
    parser.add_argument('--modality', type=int)
    parser.add_argument('--page', type=int, default=1)
    parser.add_argument('--cnpj')
    parser.add_argument('--year', type=int)
    parser.add_argument('--sequence', type=int)
    parser.add_argument('--item', type=int)
    parser.add_argument('--inactive', action='store_true',
                        help='Catalog only: ask for inactive modalities')
    args = parser.parse_args()
    checked_contracts()
    store, engine = target_store()
    try:
        if args.command == 'status':
            display_status(store)
            return 0
        if args.command == 'pause':
            halt_probe(store)
            display_status(store)
            return 0
        if args.command == 'resume':
            resume_probe(store)
            display_status(store)
            return 0
        url = request_url(args)
        control = SourceControl(store)
        mission = register_probe(control, store)
        reservation = control.reserve_attempt(mission['id'], work_kind='validation',
                                              request_hash=hashlib.sha256(url.encode()).hexdigest(),
                                              lease_seconds=120)
        if reservation['state'] != 'reserved':
            print(json.dumps({'state': reservation['state'],
                              'not_before': reservation.get('not_before')}, default=str))
            return 3
        attempt_id = reservation['attempt_id']
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        outcome_recorded = False
        try:
            response = send_get(url, max_bytes=4 * 1024 * 1024,
                                timeout=REQUEST_TIMEOUT_SECONDS)
            body_path = EVIDENCE / (str(attempt_id) + '.body')
            durable_write(body_path, response.body)
            receipt = {'attempt_id': str(attempt_id), 'sequence_no': reservation['sequence_no'],
                       'operation': args.command, 'url': url, 'status': response.status,
                       'headers': response.headers, 'bytes': len(response.body),
                       'body_sha256': hashlib.sha256(response.body).hexdigest(),
                       'body_file': body_path.name, 'contract_hash': CONSULTATION_HASH,
                       'integration_contract_hash': INTEGRATION_HASH}
            durable_write(EVIDENCE / (str(attempt_id) + '.json'),
                          json.dumps(receipt, sort_keys=True).encode())
            control.record_outcome(attempt_id, outcome='http_' + str(response.status),
                                   http_status=response.status)
            outcome_recorded = True
            summary = {'attempt': reservation['sequence_no'], 'operation': args.command,
                       'status': response.status, 'bytes': len(response.body),
                       'sha256': receipt['body_sha256'], 'receipt': str(attempt_id) + '.json'}
            if response.status == 200 and response.headers.get('content-type', '').startswith('application/json'):
                parsed = json.loads(response.body)
                if args.command == 'catalog' and isinstance(parsed, list):
                    summary['modalities'] = [{'id': row.get('id'), 'name': row.get('nome'),
                                              'active': row.get('statusAtivo')}
                                             for row in parsed if isinstance(row, dict)]
                elif isinstance(parsed, dict):
                    summary['top_level_keys'] = sorted(parsed)
                    summary['total_registros'] = parsed.get('totalRegistros')
                    summary['total_paginas'] = parsed.get('totalPaginas')
                    data = parsed.get('data')
                    if isinstance(data, list):
                        summary['page_records'] = len(data)
                        summary['sample_ids'] = [r.get('numeroControlePNCP') for r in data[:3]
                                                 if isinstance(r, dict)]
            print(json.dumps(summary, sort_keys=True, default=str))
            if response.status not in (200, 204):
                halt_probe(store)
            return 0 if response.status in (200, 204) else 3
        except Exception as exc:
            failure = {'attempt_id': str(attempt_id), 'sequence_no': reservation['sequence_no'],
                       'operation': args.command, 'url': url,
                       'error_type': type(exc).__name__}
            durable_write(EVIDENCE / (str(attempt_id) + '.error.json'),
                          json.dumps(failure, sort_keys=True).encode())
            if not outcome_recorded:
                control.record_outcome(attempt_id, outcome='transport_or_receipt_error')
            halt_probe(store)
            print(json.dumps({'attempt': reservation['sequence_no'], 'state': 'error',
                              'error_type': type(exc).__name__}, sort_keys=True))
            return 3
    finally:
        engine.dispose()


if __name__ == '__main__':
    raise SystemExit(main())
