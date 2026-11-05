"""Stage and activate a finite history budget without resetting source attempts."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from sqlalchemy import text

from app.procurement.pncp_transport import durable_write
from app.procurement.source_control import SourceControl
from app.procurement.source_isolation import HOLD_PATH, require_source_hold, transfer_source_hold
from app.procurement.source_policy import load_source_policy
from scripts.radar_pncp_history import policy_plan, target_store


BACKUP_REPORT = Path('/evidence/c2/c2-20261013/history-20261029-pre-restore-report.json')
EVIDENCE = Path('/evidence/pncp-history-activation/20261029')
BUDGET_KEY = 'pncp:public-read:shared-docker-egress'
RESTRICTION = 'unauthenticated-public-read-shared-docker-egress'
OLD_HOLD_OWNER = 'pncp-s06-discovery-2026-10-29'
HISTORY_KEY = 'pncp-history-2026-10-27'
SOURCE_CAP = 2100
MISSION_CAP = 2000
BASE_CONSUMED = 99


def verified_backup():
    report = json.loads(BACKUP_REPORT.read_text(encoding='utf-8'))
    if report.get('restored_equal') is not True or report.get('full_restore_exit_code') != 0:
        raise ValueError('Verified current C2 restore required')
    name = report.get('backup_name')
    if not isinstance(name, str) or Path(name).name != name:
        raise ValueError('Invalid backup name')
    dump = BACKUP_REPORT.parent / name
    digest = hashlib.sha256()
    with dump.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    if dump.stat().st_size != report.get('backup_bytes') or digest.hexdigest() != report.get('backup_sha256'):
        raise ValueError('Protected backup drift')


def check_policy():
    policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
    budget = policy['mission_budget']
    if (policy['mission_id'] != HISTORY_KEY
            or budget.get('stable_budget_scope_key') != BUDGET_KEY
            or budget.get('effective_restriction_scope') != RESTRICTION
            or budget.get('external_attempts_authorized_now') != MISSION_CAP
            or budget.get('shared_source_budget_total_cap') != SOURCE_CAP
            or policy['history'].get('coverage_start') != '2019-01-01'
            or policy['history'].get('max_window_days_effective') != 365
            or not policy_plan(policy)['ready']):
        raise ValueError('Reviewed history policy is not ready')
    return policy


def inspect_ledger(store, *, expected_cap, expected_enabled):
    with store.transaction() as session:
        budget = session.execute(text('''SELECT source,restriction_scope,attempt_limit,
            attempts_consumed,enabled,min_interval_ms,max_concurrency
            FROM control.source_budgets WHERE scope_key=:key'''),
            {'key': BUDGET_KEY}).mappings().one()
        missions = {row['mission_key']: row for row in session.execute(text('''
            SELECT mission_key,attempt_limit,attempts_consumed,state
            FROM control.source_missions WHERE budget_scope_key=:key'''),
            {'key': BUDGET_KEY}).mappings()}
        unmatched = session.execute(text('''SELECT count(*) FROM control.source_attempts a
            LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
            JOIN control.source_missions m ON m.id=a.mission_id
            WHERE m.budget_scope_key=:key AND o.attempt_id IS NULL'''),
            {'key': BUDGET_KEY}).scalar_one()
    old = missions['pncp-s06-probe-2026-10-28']
    calibration = missions['pncp-s06-calibration-2026-10-29']
    discovery = missions[OLD_HOLD_OWNER]
    if (budget['source'] != 'pncp' or budget['restriction_scope'] != RESTRICTION
            or budget['attempt_limit'] != expected_cap
            or budget['attempts_consumed'] != BASE_CONSUMED
            or budget['enabled'] is not expected_enabled
            or budget['min_interval_ms'] != 1000 or budget['max_concurrency'] != 1
            or old['attempts_consumed'] != 11 or old['state'] != 'paused'
            or calibration['attempts_consumed'] != 24 or calibration['state'] != 'paused'
            or discovery['attempts_consumed'] != 64 or discovery['state'] != 'paused'
            or len(missions) != 3 or unmatched):
        raise ValueError('C2 source ledger drift or unresolved attempt')
    return budget


def prepare():
    verified_backup()
    check_policy()
    require_source_hold(HOLD_PATH, OLD_HOLD_OWNER)
    store, engine = target_store()
    try:
        inspect_ledger(store, expected_cap=100, expected_enabled=False)
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        intent = EVIDENCE / 'budget-amendment-intent.json'
        outcome = EVIDENCE / 'budget-amendment-outcome.json'
        if intent.exists() or outcome.exists():
            raise FileExistsError('History budget amendment already attempted')
        durable_write(intent, json.dumps({'source_old_cap': 100,
            'source_consumed': BASE_CONSUMED, 'source_new_cap': SOURCE_CAP,
            'history_mission_cap': MISSION_CAP, 'backup_report': BACKUP_REPORT.name},
            sort_keys=True).encode())
        amended = SourceControl(store).amend_paused_budget(
            scope_key=BUDGET_KEY, source='pncp', restriction_scope=RESTRICTION,
            expected_limit=100, expected_consumed=BASE_CONSUMED, new_limit=SOURCE_CAP)
        transfer_source_hold(HOLD_PATH, OLD_HOLD_OWNER, HISTORY_KEY)
        durable_write(outcome, json.dumps({'source_cap': amended['attempt_limit'],
            'source_consumed': amended['attempts_consumed'],
            'source_enabled': amended['enabled'], 'hold_owner': HISTORY_KEY},
            sort_keys=True).encode())
        print(json.dumps({'state': 'history_budget_prepared_paused',
            'source_cap': SOURCE_CAP, 'source_consumed': BASE_CONSUMED}))
    finally:
        engine.dispose()


def activate():
    check_policy()
    require_source_hold(HOLD_PATH, HISTORY_KEY)
    if not (EVIDENCE / 'budget-amendment-outcome.json').exists():
        raise ValueError('History budget receipt required')
    store, engine = target_store()
    try:
        inspect_ledger(store, expected_cap=SOURCE_CAP, expected_enabled=False)
        with store.transaction() as session:
            session.execute(text('''UPDATE control.source_budgets SET enabled=true
                WHERE scope_key=:key AND attempt_limit=:cap AND attempts_consumed=:consumed
                AND enabled=false'''),
                {'key': BUDGET_KEY, 'cap': SOURCE_CAP, 'consumed': BASE_CONSUMED})
        print(json.dumps({'state': 'history_source_enabled',
                          'source_cap': SOURCE_CAP, 'source_consumed': BASE_CONSUMED}))
    finally:
        engine.dispose()


def set_running(running):
    require_source_hold(HOLD_PATH, HISTORY_KEY)
    policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
    budget_policy = policy['mission_budget']
    source_cap = budget_policy['shared_source_budget_total_cap']
    mission_cap = budget_policy['external_attempts_authorized_now']
    if (policy['mission_id'] != HISTORY_KEY or source_cap != BASE_CONSUMED + mission_cap
            or (running and not policy_plan(policy)['ready'])):
        raise ValueError('Reviewed current history policy is not ready')
    if (running and budget_policy.get('adaptive_rate_test') is True
            and datetime.now(timezone.utc) >= datetime.fromisoformat(
                budget_policy['test_window_end_utc'].replace('Z', '+00:00'))):
        raise ValueError('Finite history test window has ended')
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT attempt_limit,attempts_consumed,enabled
                FROM control.source_budgets WHERE scope_key=:key FOR UPDATE'''),
                {'key': BUDGET_KEY}).mappings().one()
            mission = session.execute(text('''SELECT id,attempt_limit,attempts_consumed,state,
                budget_scope_key,contract_hash,scope FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': HISTORY_KEY}).mappings().one()
            unmatched = session.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL'''),
                {'key': BUDGET_KEY}).scalar_one()
            desired_state = 'enabled' if running else 'paused'
            previous_state = 'paused' if running else 'enabled'
            already_target = budget['enabled'] is running and mission['state'] == desired_state
            transition_ready = budget['enabled'] is not running and mission['state'] == previous_state
            if (budget['attempt_limit'] != source_cap
                    or budget['attempts_consumed'] != BASE_CONSUMED + mission['attempts_consumed']
                    or mission['attempt_limit'] != mission_cap
                    or mission['budget_scope_key'] != BUDGET_KEY
                    or mission['contract_hash'].strip() != policy['contract_sha256']
                    or mission['scope'].get('daily_attempt_limit')
                    != budget_policy['daily_attempt_limit']
                    or mission['scope'].get('daily_timezone') != policy['timezone']
                    or mission['scope'].get('test_window_start_utc')
                    != budget_policy.get('test_window_start_utc')
                    or mission['scope'].get('test_window_end_utc')
                    != budget_policy.get('test_window_end_utc')
                    or unmatched or not (already_target or transition_ready)):
                raise ValueError('History source state or ledger drift')
            if transition_ready:
                session.execute(text('''UPDATE control.source_budgets SET enabled=:enabled
                    WHERE scope_key=:key'''), {'enabled': running, 'key': BUDGET_KEY})
                session.execute(text('''UPDATE control.source_missions SET state=:state
                    WHERE id=:id'''), {'state': desired_state, 'id': mission['id']})
        print(json.dumps({'state': 'resumed' if running else 'paused',
                          'source_consumed': budget['attempts_consumed'],
                          'mission_consumed': mission['attempts_consumed']}))
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('prepare', 'activate', 'pause', 'resume'))
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare()
    elif args.command == 'activate':
        activate()
    else:
        set_running(args.command == 'resume')


if __name__ == '__main__':
    main()
