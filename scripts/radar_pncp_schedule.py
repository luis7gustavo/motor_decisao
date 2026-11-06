"""Prefect-only deployment lifecycle for the isolated PNCP history mission.

The mission deployment starts paused. A probe needs an explicit rehearsal gate
and targets only a known test or shared Prefect API; it never touches procurement.
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
from prefect.client.schemas.filters import (DeploymentFilter, DeploymentFilterId,
    FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType)
from prefect.client.schemas.sorting import FlowRunSort
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.client.schemas.actions import DeploymentScheduleCreate
from prefect.exceptions import ObjectNotFound
from prefect.states import StateType
from prefect.types.entrypoint import EntrypointType
from sqlalchemy import text

from app.procurement.source_policy import load_source_policy
from app.procurement.source_isolation import HOLD_PATH, require_source_hold
from pipelines.procurement.history_flow import run_history
from pipelines.procurement.history_probe import run_probe
from scripts.radar_pncp_history import policy_plan, target_store


CONFIG = Path('config/contracts/pncp-history-schedule.json')
POLICY = Path('config/contracts/pncp-history-mission-policy.json')


def live_source_ready(policy):
    require_source_hold(HOLD_PATH, policy['mission_id'])
    budget_policy = policy['mission_budget']
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT attempt_limit,attempts_consumed,enabled,
                restriction_scope FROM control.source_budgets WHERE scope_key=:key'''),
                {'key': budget_policy['stable_budget_scope_key']}).mappings().one()
            mission = session.execute(text('''SELECT attempt_limit,attempts_consumed,state,scope
                FROM control.source_missions WHERE mission_key=:key'''),
                {'key': policy['mission_id']}).mappings().one()
            consumed = session.execute(text('''SELECT coalesce(sum(attempts_consumed),0)
                FROM control.source_missions WHERE budget_scope_key=:key'''),
                {'key': budget_policy['stable_budget_scope_key']}).scalar_one()
        test_end = mission['scope'].get('test_window_end_utc')
        window_open = (test_end is None or datetime.now(timezone.utc) <
                       datetime.fromisoformat(test_end.replace('Z', '+00:00')))
        return (window_open and budget['enabled'] and mission['state'] == 'enabled'
                and budget['attempt_limit'] == budget_policy['shared_source_budget_total_cap']
                and mission['attempt_limit'] == budget_policy['external_attempts_authorized_now']
                and mission['scope'].get('daily_attempt_limit') == budget_policy['daily_attempt_limit']
                and mission['scope'].get('daily_timezone') == policy['timezone']
                and budget['restriction_scope'] == budget_policy['effective_restriction_scope']
                and budget['attempts_consumed'] == consumed)
    finally:
        engine.dispose()


def emit(value):
    print(json.dumps(value, sort_keys=True, default=str, allow_nan=False))


def configuration(rehearsal):
    config = json.loads(CONFIG.read_text(encoding='utf-8'))
    if (config['schema_version'] != 'pncp-history-schedule-1'
            or config['timezone'] != 'America/Sao_Paulo'
            or any(config[key] != 1 for key in
                   ('pool_concurrency', 'queue_concurrency', 'deployment_concurrency'))
            or config['initial_state'] != 'paused'
            or type(config.get('backfill_interval_minutes')) is not int
            or not 1 <= config['backfill_interval_minutes'] <= 60
            or type(config.get('backfill_steps')) is not int
            or not 1 <= config['backfill_steps'] <= 100):
        raise ValueError('Invalid bounded history schedule configuration')
    if rehearsal:
        api = os.environ.get('PREFECT_API_URL', '')
        isolated = api == 'http://prefect-history-test-server:4200/api'
        shared = (api == 'http://prefect-server:4200/api'
                  and os.environ.get('RADAR_HISTORY_SHARED_PROBE') == '1')
        if (os.environ.get('RADAR_HISTORY_REHEARSAL') != '1'
                or not (isolated or shared)):
            raise ValueError('Probe requires an explicitly gated Prefect server')
    return config


def names(config, rehearsal):
    if rehearsal:
        return 'radar-pncp-history-s05-test', 'probe', 'radar-pncp-s05-probe/probe'
    return config['work_pool'], config['work_queue'], config['mission_deployment']


def desired_schedules(config, rehearsal, active=False):
    if rehearsal:
        return [DeploymentScheduleCreate(
            schedule=CronSchedule(cron='* * * * *', timezone=config['timezone']),
            active=active, slug='probe-minute', parameters={'mode': 'probe'},
            max_scheduled_runs=2)]
    zone = ZoneInfo(config['timezone'])
    return [
        DeploymentScheduleCreate(
            schedule=IntervalSchedule(
                interval=timedelta(minutes=config['backfill_interval_minutes']),
                anchor_date=datetime(2026, 1, 1, tzinfo=zone),
                timezone=config['timezone']),
            active=active, slug=('backfill-continuous' if config['backfill_interval_minutes'] == 1
                                 else 'backfill-hourly'),
            parameters={'mode': 'backfill'},
            max_scheduled_runs=config['max_scheduled_runs_per_schedule']),
        DeploymentScheduleCreate(
            schedule=CronSchedule(cron=config['sync_cron'], timezone=config['timezone']),
            active=active, slug='sync-six-hours', parameters={'mode': 'sync'},
            max_scheduled_runs=config['max_scheduled_runs_per_schedule']),
        DeploymentScheduleCreate(
            schedule=CronSchedule(cron=config['reconcile_cron'], timezone=config['timezone']),
            active=active, slug='reconcile-weekly',
            parameters={'mode': 'reconcile'},
            max_scheduled_runs=config['max_scheduled_runs_per_schedule']),
    ]


async def _read(client, pool_name, queue_name, deployment_name):
    try:
        pool = await client.read_work_pool(pool_name)
    except ObjectNotFound:
        pool = None
    try:
        queue = await client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
    except ObjectNotFound:
        queue = None
    try:
        deployment = await client.read_deployment_by_name(deployment_name)
    except ObjectNotFound:
        deployment = None
    schedules = (await client.read_deployment_schedules(deployment.id)) if deployment else []
    workers = (await client.read_workers_for_work_pool(pool_name, limit=20)) if pool else []
    runs = []
    if deployment:
        runs = await client.read_flow_runs(
            deployment_filter=DeploymentFilter(id=DeploymentFilterId(any_=[deployment.id])),
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(
                type=FlowRunFilterStateType(not_any_=[StateType.SCHEDULED]))),
            sort=FlowRunSort.START_TIME_DESC, limit=10)
    return pool, queue, deployment, schedules, workers, runs


def _snapshot(pool, queue, deployment, schedules, workers, runs):
    global_limit = (getattr(deployment, 'global_concurrency_limit', None)
                    if deployment is not None else None)
    return {
        'pool': None if pool is None else {'name': pool.name, 'type': pool.type,
            'paused': pool.is_paused, 'concurrency_limit': pool.concurrency_limit},
        'queue': None if queue is None else {'name': queue.name,
            'paused': queue.is_paused, 'concurrency_limit': queue.concurrency_limit},
        'deployment': None if deployment is None else {'id': str(deployment.id),
            'name': deployment.name, 'paused': deployment.paused,
            'concurrency_limit': getattr(global_limit, 'limit', None)},
        'schedules': [{'id': str(s.id), 'slug': s.slug, 'active': s.active,
            'parameters': s.parameters, 'max_scheduled_runs': s.max_scheduled_runs,
            'schedule': s.schedule.model_dump(mode='json')}
            for s in schedules],
        'workers': [{'name': w.name, 'status': str(w.status)} for w in workers],
        'recent_runs': [{'id': str(r.id), 'state': str(r.state_type),
                         'scheduled_start_time': r.expected_start_time,
                         'parameters': r.parameters} for r in runs],
    }


async def inspect_state(config, rehearsal):
    pool_name, queue_name, deployment_name = names(config, rehearsal)
    async with get_client() as client:
        values = await _read(client, pool_name, queue_name, deployment_name)
    return _snapshot(*values)


async def ensure_pool_queue(config, rehearsal):
    pool_name, queue_name, _ = names(config, rehearsal)
    async with get_client() as client:
        try:
            pool = await client.read_work_pool(pool_name)
        except ObjectNotFound:
            pool = None
        if pool is None:
            await client.create_work_pool(WorkPoolCreate(
                name=pool_name, type='process', is_paused=False,
                concurrency_limit=1,
                description='Isolated PNCP history mission; one process at a time'))
        else:
            if pool.type != 'process':
                raise ValueError('History pool type drift')
            if pool.concurrency_limit != 1 or pool.is_paused:
                await client.update_work_pool(pool_name, WorkPoolUpdate(
                    concurrency_limit=1, is_paused=False))
        try:
            queue = await client.read_work_queue_by_name(queue_name,
                                                         work_pool_name=pool_name)
        except ObjectNotFound:
            queue = None
        if queue is None:
            await client.create_work_queue(queue_name, work_pool_name=pool_name,
                                           concurrency_limit=1, is_paused=False)
        elif queue.concurrency_limit != 1 or queue.is_paused:
            await client.update_work_queue(queue.id, concurrency_limit=1, is_paused=False)


async def reconcile_schedules(config, rehearsal, deployment_id, active):
    desired = {s.slug: s for s in desired_schedules(config, rehearsal, active=active)}
    async with get_client() as client:
        actual = await client.read_deployment_schedules(deployment_id)
        legacy = [s for s in actual if s.slug == 'backfill-half-hour']
        if (not rehearsal and not active and len(actual) == 4 and len(legacy) == 1
                and 'backfill-hourly' in {s.slug for s in actual}
                and legacy[0].active is False
                and legacy[0].parameters == {'mode': 'backfill'}
                and legacy[0].schedule == desired['backfill-hourly'].schedule):
            await client.delete_deployment_schedule(deployment_id, legacy[0].id)
            actual = await client.read_deployment_schedules(deployment_id)
        hourly = [s for s in actual if s.slug == 'backfill-hourly']
        if (not rehearsal and not active and 'backfill-continuous' in desired
                and len(actual) == 4 and len(hourly) == 1
                and hourly[0].active is False
                and hourly[0].parameters == {'mode': 'backfill'}
                and hourly[0].schedule.interval == timedelta(hours=1)
                and {s.slug for s in actual} == set(desired) | {'backfill-hourly'}):
            await client.delete_deployment_schedule(deployment_id, hourly[0].id)
            actual = await client.read_deployment_schedules(deployment_id)
        if len(actual) != len(desired) or {s.slug for s in actual} != set(desired):
            raise ValueError('Unexpected schedule set after deployment apply')
        for schedule in actual:
            expected = desired[schedule.slug]
            if (schedule.schedule != expected.schedule
                    or schedule.parameters != expected.parameters
                    or schedule.active != expected.active
                    or schedule.max_scheduled_runs != expected.max_scheduled_runs):
                await client.update_deployment_schedule(
                    deployment_id, schedule.id, active=expected.active,
                    schedule=expected.schedule,
                    parameters=expected.parameters,
                    max_scheduled_runs=expected.max_scheduled_runs)


def apply_deployment(config, rehearsal, paused):
    pool_name, queue_name, _ = names(config, rehearsal)
    source = run_probe if rehearsal else run_history
    deployment = source.to_deployment(
        name='probe' if rehearsal else 'mission',
        work_pool_name=pool_name, work_queue_name=queue_name,
        paused=paused,
        schedules=desired_schedules(config, rehearsal, active=not paused),
        concurrency_limit=1,
        tags=['pncp-history-s05-probe'] if rehearsal else ['pncp-history', 'isolated'],
        job_variables={'working_dir': '/app'},
        entrypoint_type=EntrypointType.MODULE_PATH,
    )
    return deployment.apply(work_pool_name=pool_name)


async def set_pause(config, rehearsal, paused):
    pool_name, queue_name, deployment_name = names(config, rehearsal)
    expected = {
        schedule.slug: schedule
        for schedule in desired_schedules(config, rehearsal, active=not paused)
    }
    async with get_client() as client:
        _, _, deployment, schedules, _, _ = await _read(
            client, pool_name, queue_name, deployment_name)
        if deployment is None:
            raise ValueError('Deployment missing; apply first')
        if {schedule.slug for schedule in schedules} != set(expected):
            raise ValueError('Deployment schedule set drift')
        if paused:
            await client.set_deployment_paused_state(deployment.id, paused=True)
        for schedule in schedules:
            desired = expected[schedule.slug]
            if (schedule.schedule != desired.schedule
                    or schedule.active != desired.active
                    or schedule.parameters != desired.parameters
                    or schedule.max_scheduled_runs != desired.max_scheduled_runs):
                await client.update_deployment_schedule(
                    deployment.id, schedule.id, active=desired.active,
                    schedule=desired.schedule,
                    parameters=desired.parameters,
                    max_scheduled_runs=desired.max_scheduled_runs)
        if not paused:
            await client.set_deployment_paused_state(deployment.id, paused=False)
        updated = await client.read_deployment_schedules(deployment.id)
    if any((schedule.schedule != expected[schedule.slug].schedule
            or schedule.active != expected[schedule.slug].active
                or schedule.parameters != expected[schedule.slug].parameters)
               for schedule in updated):
            raise ValueError('Deployment schedule parameters drift after pause change')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Prefect PNCP history deployment lifecycle')
    parser.add_argument('command', choices=('plan', 'apply', 'pause', 'resume', 'status'))
    parser.add_argument('--rehearsal', action='store_true', help='isolated S05 probe server only')
    args = parser.parse_args(argv)
    config = configuration(args.rehearsal)
    before = asyncio.run(inspect_state(config, args.rehearsal))
    desired = desired_schedules(config, args.rehearsal)
    desired_slugs = [item.slug for item in desired]
    if args.command == 'plan':
        emit({'mode': 'probe' if args.rehearsal else 'mission',
              'changes': {'create_pool': before['pool'] is None,
                          'create_queue': before['queue'] is None,
                          'create_deployment': before['deployment'] is None,
                          'desired_schedule_slugs': desired_slugs,
                          'existing_schedule_slugs': [s['slug'] for s in before['schedules']]},
              'current': before, 'network_calls_to_pncp': 0,
              'procurement_database_writes': 0})
        return 0
    if args.command == 'apply':
        awaitable = ensure_pool_queue(config, args.rehearsal)
        asyncio.run(awaitable)
        blocked = not policy_plan(load_source_policy(POLICY))['ready']
        paused = True if (not args.rehearsal and blocked) else (
            before['deployment']['paused'] if before['deployment'] else True)
        deployment_id = apply_deployment(config, args.rehearsal, paused)
        asyncio.run(reconcile_schedules(config, args.rehearsal,
                                        deployment_id, active=not paused))
        after = asyncio.run(inspect_state(config, args.rehearsal))
        actual_slugs = [s['slug'] for s in after['schedules']]
        expected_parameters = {
            item.slug: item.parameters for item in desired_schedules(
                config, args.rehearsal, active=not paused)
        }
        if (str(deployment_id) != after['deployment']['id']
                or len(actual_slugs) != len(desired_slugs)
                or set(actual_slugs) != set(desired_slugs)
            or any(s['parameters'] != expected_parameters[s['slug']]
                   or s['schedule'] != next(item.schedule.model_dump(mode='json')
                       for item in desired_schedules(config, args.rehearsal,
                           active=not paused) if item.slug == s['slug'])
                       for s in after['schedules'])
                or after['pool']['concurrency_limit'] != 1
                or after['queue']['concurrency_limit'] != 1
                or after['deployment']['concurrency_limit'] != 1):
            raise ValueError('Deployment schedule identity or bounds drift')
        emit({'state': 'applied_paused' if paused else 'applied_active',
              'deployment_id': str(deployment_id), 'status': after,
              'source_policy_ready': not blocked})
        return 0
    if args.command == 'pause':
        asyncio.run(set_pause(config, args.rehearsal, True))
    if args.command == 'resume':
        if not args.rehearsal:
            readiness = policy_plan(load_source_policy(POLICY))
            if not readiness['ready']:
                emit({'state': 'blocked', 'gates': readiness['gates'],
                      'network_calls_to_pncp': 0})
                return 3
            if not live_source_ready(load_source_policy(POLICY)):
                emit({'state': 'blocked_source_budget_or_mission',
                      'network_calls_to_pncp': 0})
                return 3
        if not any('ONLINE' in worker['status'] for worker in before['workers']):
            emit({'state': 'blocked_worker_unavailable', 'network_calls_to_pncp': 0})
            return 3
        asyncio.run(set_pause(config, args.rehearsal, False))
    after = asyncio.run(inspect_state(config, args.rehearsal))
    emit({'state': args.command, 'status': after, 'network_calls_to_pncp': 0})
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        emit({'state': 'error', 'error_type': type(error).__name__})
        raise SystemExit(2)
