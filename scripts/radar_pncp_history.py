"""S03 isolated history CLI. `plan` is a network/DB-free dry run.

Exit 0: operation completed; 3: source/mission blocked or deferred;
2: invalid arguments, target drift, or operational failure. Never prints secrets.
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
import time

import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.procurement.history import HistoryBackfill, UPDATE_RESOURCE, windows
from app.procurement.pncp import RESOURCE
from app.procurement.enrichment import EnrichmentQueue
from app.procurement.pncp import Store
from app.procurement.pncp_transport import send_get
from app.procurement.source_policy import load_source_policy
from app.procurement.source_isolation import HOLD_PATH, ensure_source_hold
from app.procurement.sync import SyncCoordinator
from uuid import UUID


POLICY = Path('config/contracts/pncp-history-mission-policy.json')
TARGET = Path('/evidence/c2/c2-20261013/target-credentials.json')
BASELINE = Path('/evidence/20261020T152131Z_e6215d7c.json')


def emit(value):
    print(json.dumps(value, sort_keys=True, default=str, allow_nan=False))


def step_after_short_source_wait(service, mission_id, *, priority_floor,
                                 resource_filter, priority_ceiling=None):
    """Wait only for the local short interval; leave longer deferrals durable."""
    for _ in range(4):
        result = service.step(mission_id, priority_floor=priority_floor,
                              priority_ceiling=priority_ceiling,
                              resource_filter=resource_filter)
        deadline = result.get('not_before')
        if result['state'] != 'deferred' or not isinstance(deadline, datetime):
            return result
        delay = (deadline - datetime.now(timezone.utc)).total_seconds()
        if delay > 2:
            return result
        time.sleep(max(0.05, delay + 0.05))
    return result


def reconcile_split_parents(store, coordinator, mission_id, *, limit=100):
    with store.transaction() as s:
        parent_ids = s.execute(text("""SELECT id FROM control.source_partitions
            WHERE mission_id=:id AND state='reconcile'
            ORDER BY (window_end-window_start),created_at,id LIMIT :limit"""),
                               {'id': mission_id, 'limit': limit}).scalars().all()
    return [coordinator.reconcile_parent(parent_id) for parent_id in parent_ids]


def policy_plan(policy):
    history = policy['history']
    budget = policy['mission_budget']
    gates = []
    if policy.get('enabled') is not True:
        gates.append('mission_disabled')
    if policy.get('contract_currently_revalidated') is not True:
        gates.append('current_contract_unverified')
    if history.get('coverage_start_status') not in ('verified', 'verified_accessible') or not history.get('coverage_start'):
        gates.append('coverage_start_unverified')
    if history.get('modality_catalog_status') not in ('verified', 'verified_accessible') or not history.get('modality_catalog'):
        gates.append('modality_catalog_unverified')
    if history.get('fixed_end_filter_semantics_validated') is not True:
        gates.append('date_filter_bounds_unverified')
    if policy.get('resource_gates', {}).get('publication') != 'enabled':
        gates.append('publication_resource_blocked')
    if policy.get('resource_gates', {}).get('global_update') != 'enabled':
        gates.append('global_update_resource_blocked')
    for key in ('effective_restriction_scope', 'stable_budget_scope_key'):
        if not isinstance(budget.get(key), str) or not budget[key].strip():
            gates.append(key + '_unresolved')
    interval = budget.get('effective_min_interval_ms')
    if type(interval) is not int or interval < 1000:
        gates.append('effective_min_interval_ms_unresolved')
    concurrency = budget.get('effective_max_concurrency')
    if type(concurrency) is not int or concurrency != 1:
        gates.append('effective_max_concurrency_unresolved')
    timeout = budget.get('request_timeout_seconds')
    if type(timeout) is not int or not 30 <= timeout <= 90:
        gates.append('request_timeout_unresolved')
    attempts = budget.get('external_attempts_authorized_now')
    source_cap = budget.get('shared_source_budget_total_cap')
    if (type(attempts) is not int or attempts < 1
            or type(source_cap) is not int or source_cap < attempts):
        gates.append('attempt_budget_zero')
    daily_cap = budget.get('daily_attempt_limit')
    if type(daily_cap) is not int or daily_cap < 1 or policy.get('timezone') != 'America/Sao_Paulo':
        gates.append('daily_attempt_limit_unresolved')
    if budget.get('adaptive_rate_test') is True:
        try:
            start = datetime.fromisoformat(budget['test_window_start_utc'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(budget['test_window_end_utc'].replace('Z', '+00:00'))
            valid_window = (start.tzinfo is not None and end.tzinfo is not None
                            and start.utcoffset() == timedelta(0)
                            and end.utcoffset() == timedelta(0)
                            and end - start == timedelta(hours=24))
        except (KeyError, ValueError, AttributeError, TypeError):
            valid_window = False
        if (not valid_window or attempts != 100000 or source_cap != 100099
                or daily_cap != 90000 or interval != 1000 or concurrency != 1):
            gates.append('finite_rate_test_unreviewed')
    days = history.get('max_window_days_effective')
    if type(days) is not int or days < 1:
        gates.append('window_limit_unresolved')
    partitions = None
    if not {'coverage_start_unverified', 'modality_catalog_unverified',
            'window_limit_unresolved'} & set(gates):
        spans = list(windows(date.fromisoformat(history['coverage_start']),
                             date.fromisoformat(history['fixed_end_local_date']), days))
        partitions = len(spans) * len(history['modality_catalog']) * 2
    return {'mission': policy['mission_id'], 'ready': not gates, 'gates': gates,
            'planned_partitions': partitions, 'fixed_end': history['fixed_end_local_date'],
            'coverage_start': history.get('coverage_start'),
            'network_calls': 0, 'database_writes': 0}


def audit_summary(partitions, resource_gaps):
    def states_for(rows):
        states = {}
        for partition in rows:
            state = partition['state']
            states[state] = states.get(state, 0) + 1
        return states

    def coverage_for(rows):
        if not rows:
            return 'not_started'
        if resource_gaps or any(partition['state'] != 'complete' for partition in rows):
            return 'partial'
        # Stable pagination alone cannot establish an integral source snapshot.
        return 'pagination_consistent_unverified'

    # Historical passes are 1-3. Sync and wide reconciliation use cycle
    # numbers from 100_000 onward and continue after the fixed historical end.
    historical = [p for p in partitions if p.get('pass_no', 1) < 100_000]
    recurring = [p for p in partitions if p.get('pass_no', 1) >= 100_000]
    return {'coverage_status': coverage_for(partitions),
            'partition_states': states_for(partitions),
            'partition_count': len(partitions),
            'historical_coverage_status': coverage_for(historical),
            'historical_partition_states': states_for(historical),
            'historical_partition_count': len(historical),
            'recurring_partition_states': states_for(recurring),
            'recurring_partition_count': len(recurring),
            'resource_gap_count': len(resource_gaps)}


def target_store():
    record = json.loads(TARGET.read_text())
    baseline = json.loads(BASELINE.read_text())
    principal = os.environ['PGDATABASE']
    if (principal != 'motor_decisao' or record['database'] == principal
            or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}', record['database'])
            or not re.fullmatch(r'radar_test_[a-f0-9]{12}', record['role'])):
        raise ValueError('Isolated target record is invalid')
    with psycopg.connect() as admin:
        admin.execute('SET TRANSACTION READ ONLY')
        server = admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        principal_revision = admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        principal_procurement = admin.execute("SELECT count(*) FROM pg_namespace WHERE nspname='procurement'").fetchone()[0]
        db_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s',
                               (record['database'],)).fetchone()
        role_oid = admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s',
                                 (record['role'],)).fetchone()
    if (server != record['server'] or server != baseline['server']
            or not db_oid or db_oid[0] != record['database_oid']
            or db_oid[0] != baseline['database_oid']
            or not role_oid or role_oid[0] != record['role_oid']
            or role_oid[0] != baseline['role_oid']
            or principal_revision != '20260817_0009' or principal_procurement != 0):
        raise ValueError('Principal or isolated target identity drift')
    with psycopg.connect(dbname=record['database']) as admin_target:
        admin_target.execute('SET TRANSACTION READ ONLY')
        revision = admin_target.execute('SELECT version_num FROM alembic_version').fetchone()[0]
    if revision != '20261027_0012':
        raise ValueError('Isolated target revision drift')
    os.environ['RADAR_TEST_DB'] = record['database']
    os.environ['RADAR_TEST_ROLE'] = record['role']
    os.environ['RADAR_TEST_OID'] = str(record['database_oid'])
    engine = create_engine(record['url'], connect_args={'connect_timeout': 10})
    store = Store(engine, expected_database=record['database'])
    with store.transaction():
        pass
    return store, engine


def main(argv=None):
    parser = argparse.ArgumentParser(description='PNCP history and sync; isolated target only')
    parser.add_argument('command', choices=('plan', 'backfill', 'sync', 'reconcile',
                                            'enrich', 'replay', 'status', 'audit',
                                            'split-exhausted', 'reconcile-splits',
                                            'probe-day', 'probe-window'))
    parser.add_argument('--steps', type=int, default=1, help='maximum one-page steps')
    parser.add_argument('--history-resource', choices=('publication', 'update'),
                        help='Limit backfill steps to one historical listing resource')
    parser.add_argument('--probe-date', help='One date within the historical mission')
    parser.add_argument('--probe-end', help='Optional end date for a bounded window')
    parser.add_argument('--probe-modality', type=int)
    parser.add_argument('--cycle-at', help='UTC or offset ISO time, fixed for idempotent cycles')
    parser.add_argument('--raw-id', help='saved Bronze UUID for replay')
    parser.add_argument('--dry-run', action='store_true', help='show plan without DB/network access')
    parser.add_argument('--summary-only', action='store_true',
                        help='omit per-partition rows from the read-only audit')
    args = parser.parse_args(argv)
    if args.steps < 1 or args.steps > 100:
        parser.error('--steps must be from 1 to 100')
    policy = load_source_policy(POLICY)
    plan = policy_plan(policy)
    coordinator_gaps = SyncCoordinator.resource_gaps(policy)
    if args.command == 'plan' or args.dry_run:
        emit({**plan, 'resource_gaps': coordinator_gaps})
        return 0 if plan['ready'] else 3
    if args.command in ('backfill', 'sync', 'reconcile', 'enrich',
                        'split-exhausted', 'reconcile-splits',
                        'probe-day', 'probe-window') and not plan['ready']:
        emit({'state': 'blocked', 'gates': plan['gates'], 'network_calls': 0})
        return 3
    if args.command in ('backfill', 'sync', 'reconcile', 'enrich',
                        'split-exhausted', 'reconcile-splits',
                        'probe-day', 'probe-window'):
        ensure_source_hold(HOLD_PATH, policy['mission_id'])
    store, engine = target_store()
    try:
        timeout = policy['mission_budget']['request_timeout_seconds']
        if type(timeout) is not int or not 30 <= timeout <= 90:
            raise ValueError('Invalid bounded history request timeout')
        service = HistoryBackfill(store, lambda url: send_get(url, timeout=timeout))
        coordinator = SyncCoordinator(service)
        with store.transaction() as s:
            row = s.execute(text('SELECT id FROM control.source_missions WHERE mission_key=:key'),
                            {'key': policy['mission_id']}).first()
        if args.command == 'backfill':
            history = policy['history']
            budget = policy['mission_budget']
            registered = service.plan(mission_key=policy['mission_id'],
                budget_scope_key=budget['stable_budget_scope_key'],
                restriction_scope=budget['effective_restriction_scope'],
                contract_hash=policy['contract_sha256'],
                coverage_start=date.fromisoformat(history['coverage_start']),
                fixed_end=date.fromisoformat(history['fixed_end_local_date']),
                modalities=history['modality_catalog'],
                window_days=history['max_window_days_effective'],
                historical_resources=(RESOURCE, UPDATE_RESOURCE),
                attempt_limit=budget['external_attempts_authorized_now'],
                source_attempt_limit=budget['shared_source_budget_total_cap'],
                daily_attempt_limit=budget['daily_attempt_limit'],
                daily_timezone=policy['timezone'],
                enabled=True, page_size=policy['official_limits']['page_size_publication_update_max'])
            mission_id = registered['mission']['id']
            results = []
            resource = (RESOURCE if args.history_resource == 'publication'
                        else UPDATE_RESOURCE if args.history_resource == 'update' else None)
            split = service.split_exhausted_windows(mission_id)
            for _ in range(args.steps):
                result = step_after_short_source_wait(
                    service, mission_id, priority_floor=100,
                    resource_filter=resource)
                results.append(result)
                if result['state'] not in ('page_committed', 'complete'):
                    break
            reconciled = reconcile_split_parents(store, coordinator, mission_id)
            emit({'results': results, 'split_windows': split,
                  'split_reconciliation': reconciled,
                  'status': service.status(mission_id)})
            return 0 if all(r['state'] in ('page_committed', 'complete', 'idle') for r in results) else 3
        if row is None:
            emit({'state': 'not_started', 'mission': policy['mission_id']})
            return 3
        if args.command in ('probe-day', 'probe-window'):
            if not args.probe_date or not args.history_resource or args.probe_modality is None:
                parser.error('probe-day requires date, modality and history resource')
            day = date.fromisoformat(args.probe_date)
            end = date.fromisoformat(args.probe_end) if args.probe_end else day
            history = policy['history']
            if (not date.fromisoformat(history['coverage_start']) <= day <= end
                    <= date.fromisoformat(history['fixed_end_local_date'])
                    or (end - day).days > 29
                    or (args.command == 'probe-day' and end != day)
                    or args.probe_modality not in history['modality_catalog']):
                raise ValueError('Probe window outside validated historical scope')
            resource = (RESOURCE if args.history_resource == 'publication'
                        else UPDATE_RESOURCE)
            part = service.control.register_partition(
                row.id, resource=resource, modality_code=args.probe_modality,
                window_start=day, window_end=end, pass_no=90000, priority=110)
            with store.transaction() as s:
                pending = s.execute(text("""SELECT id FROM control.source_partitions
                    WHERE mission_id=:id AND resource=:resource AND priority=110
                    AND state='pending'"""),
                                    {'id': row.id, 'resource': resource}).scalars().all()
            if pending != [part['id']]:
                raise ValueError('Probe day is not the sole pending probe partition')
            result = step_after_short_source_wait(
                service, row.id, priority_floor=110, priority_ceiling=110,
                resource_filter=resource)
            emit({'state': 'probe_window', 'start': day.isoformat(),
                  'end': end.isoformat(),
                  'modality': args.probe_modality, 'resource': resource,
                  'partition_id': str(part['id']), 'result': result,
                  'status': service.status(row.id)})
            return 0 if result['state'] in ('page_committed', 'complete') else 3
        if args.command == 'split-exhausted':
            split = service.split_exhausted_windows(row.id, limit=args.steps)
            emit({'state': 'split_exhausted', 'split_windows': split,
                  'status': service.status(row.id), 'network_calls': 0})
            return 0
        if args.command == 'reconcile-splits':
            results = reconcile_split_parents(store, coordinator, row.id)
            emit({'state': 'reconcile_splits', 'results': results,
                  'status': service.status(row.id), 'network_calls': 0})
            return 0
        if args.command == 'enrich':
            queue = EnrichmentQueue(store, send_get)
            results = []
            for _ in range(args.steps):
                result = queue.step(row.id)
                results.append(result)
                if result['state'] != 'raw_collected':
                    break
            emit({'results': results, 'resource_gaps': coordinator_gaps,
                  'status': service.status(row.id)})
            return 0 if all(r['state'] in ('raw_collected', 'idle') for r in results) else 3
        if args.command == 'replay':
            if not args.raw_id:
                parser.error('replay requires --raw-id')
            emit(coordinator.replay_raw(row.id, UUID(args.raw_id)))
            return 0
        if args.command in ('sync', 'reconcile'):
            cycle_at = (datetime.fromisoformat(args.cycle_at.replace('Z', '+00:00'))
                        if args.cycle_at else datetime.now(timezone.utc))
            if cycle_at.tzinfo is None or cycle_at.utcoffset() is None:
                parser.error('--cycle-at needs UTC or an explicit offset')
            if args.command == 'sync':
                planned = coordinator.plan_sync(row.id, cycle_at=cycle_at)
                results = coordinator.run_sync(row.id, max_pages=args.steps)
                reconciled = []
            else:
                planned = coordinator.plan_wide_reconcile(row.id, cycle_at=cycle_at)
                reconciled = reconcile_split_parents(store, coordinator, row.id)
                results = [service.step(row.id, priority_floor=40, priority_ceiling=99)
                           for _ in range(args.steps)]
                reconciled.extend(reconcile_split_parents(store, coordinator, row.id))
            emit({'planned_partitions': len(planned['partitions']),
                  'gaps': planned['gaps'], 'results': results,
                  'parent_reconciliation': reconciled,
                  'status': service.status(row.id)})
            return 0 if all(r['state'] in ('page_committed', 'complete', 'idle') for r in results) else 3
        if args.command == 'status':
            with store.transaction() as s:
                watermarks = [dict(r) for r in s.execute(text("""SELECT resource,partition_key,
                    contiguous_through FROM control.source_watermarks WHERE mission_id=:id
                    ORDER BY resource,partition_key"""), {'id': row.id}).mappings()]
            emit({**service.status(row.id), 'watermarks': watermarks,
                  'resource_gaps': coordinator_gaps})
        else:
            partitions = service.audit(row.id)
            emit({**audit_summary(partitions, coordinator_gaps),
                  **({} if args.summary_only else {'partitions': partitions}),
                  'resource_gaps': coordinator_gaps})
        return 0
    finally:
        engine.dispose()


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, OSError, psycopg.Error, SQLAlchemyError) as error:
        emit({'state': 'error', 'error_type': type(error).__name__})
        raise SystemExit(2)
