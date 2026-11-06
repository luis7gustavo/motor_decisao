"""Bounded discovery campaign on the existing C2 source ledger.

Extends only a paused budget after a verified, current C2 backup. Every
network attempt still runs through the S06 probe's durable reservation path.
"""

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
import time

from sqlalchemy import text

from app.procurement.source_control import SourceControl
from app.procurement.source_isolation import HOLD_PATH
from app.procurement.pncp_transport import durable_write
from scripts import radar_pncp_s06_probe as probe
from scripts.radar_pncp_history import target_store


MISSION_KEY = 'pncp-s06-discovery-2026-10-29'
EVIDENCE = Path('/evidence/pncp-probe/20261029-discovery')
BACKUP_REPORT = Path('/evidence/c2/c2-20261013/discovery-20261029-pre-restore-report.json')
SOURCE_LIMIT = 100
MISSION_LIMIT = 64
EXPECTED_SOURCE_CONSUMED = 35


def configure():
    probe.MISSION_KEY = MISSION_KEY
    probe.EVIDENCE = EVIDENCE
    probe.ATTEMPT_LIMIT = MISSION_LIMIT
    probe.SOURCE_BUDGET_LIMIT = SOURCE_LIMIT
    probe.MISSION_KIND = 'bounded_s06_accessible_range_discovery'
    probe.REQUEST_TIMEOUT_SECONDS = 60


def check_hold():
    owner = json.loads(HOLD_PATH.read_text(encoding='utf-8'))
    if owner.get('mission_key') != MISSION_KEY:
        raise ValueError('Discovery source hold is absent or owned by another mission')


def check_backup():
    report = json.loads(BACKUP_REPORT.read_text(encoding='utf-8'))
    if report.get('restored_equal') is not True or report.get('full_restore_exit_code') != 0:
        raise ValueError('Verified isolated C2 restore required')
    name = report.get('backup_name')
    if not isinstance(name, str) or Path(name).name != name:
        raise ValueError('Invalid protected backup name')
    dump = BACKUP_REPORT.parent / name
    digest = hashlib.sha256()
    with dump.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    if dump.stat().st_size != report.get('backup_bytes') or digest.hexdigest() != report.get('backup_sha256'):
        raise ValueError('Protected backup differs from restore report')


def prepare():
    check_hold()
    check_backup()
    probe.checked_contracts()
    store, engine = target_store()
    try:
        control = SourceControl(store)
        with store.transaction() as session:
            budget = session.execute(text('''SELECT source,restriction_scope,attempt_limit,
                attempts_consumed,enabled,min_interval_ms,max_concurrency
                FROM control.source_budgets WHERE scope_key=:key'''),
                {'key': probe.BUDGET_KEY}).mappings().one()
            missions = {row['mission_key']: row for row in session.execute(text('''
                SELECT mission_key,attempt_limit,attempts_consumed,state
                FROM control.source_missions WHERE budget_scope_key=:key'''),
                {'key': probe.BUDGET_KEY}).mappings()}
        old = missions['pncp-s06-probe-2026-10-28']
        calibration = missions['pncp-s06-calibration-2026-10-29']
        if (budget['source'] != 'pncp' or budget['restriction_scope'] != probe.RESTRICTION
                or budget['attempt_limit'] != 36 or budget['attempts_consumed'] != EXPECTED_SOURCE_CONSUMED
                or budget['enabled'] or budget['min_interval_ms'] != 1000
                or budget['max_concurrency'] != 1 or old['attempts_consumed'] != 11
                or old['state'] != 'paused' or calibration['attempt_limit'] != 24
                or calibration['attempts_consumed'] != 24 or calibration['state'] != 'paused'
                or len(missions) != 2):
            raise ValueError('Paused source ledger baseline drift')
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        intent = EVIDENCE / 'budget-amendment-intent.json'
        outcome = EVIDENCE / 'budget-amendment-outcome.json'
        if intent.exists() or outcome.exists():
            raise FileExistsError('Discovery amendment evidence already exists')
        durable_write(intent, json.dumps({'source_old_cap': 36, 'source_consumed': 35,
            'source_new_cap': SOURCE_LIMIT, 'mission_cap': MISSION_LIMIT,
            'backup_report': BACKUP_REPORT.name}, sort_keys=True).encode())
        amended = control.amend_paused_budget(scope_key=probe.BUDGET_KEY,
            source='pncp', restriction_scope=probe.RESTRICTION,
            expected_limit=36, expected_consumed=35, new_limit=SOURCE_LIMIT)
        mission = control.register_mission(mission_key=MISSION_KEY, source='pncp',
            budget_scope_key=probe.BUDGET_KEY, contract_hash=probe.CONSULTATION_HASH,
            fixed_end=date(2026, 10, 26),
            scope={'kind': probe.MISSION_KIND,
                   'integration_contract_hash': probe.INTEGRATION_HASH,
                   'max_attempts': MISSION_LIMIT},
            attempt_limit=MISSION_LIMIT, state='paused')
        durable_write(outcome, json.dumps({'source_cap': amended['attempt_limit'],
            'source_consumed': amended['attempts_consumed'],
            'mission_id': str(mission['id']), 'mission_state': mission['state']},
            sort_keys=True).encode())
        print(json.dumps({'state': 'prepared_paused', 'source_cap': SOURCE_LIMIT,
                          'source_consumed': amended['attempts_consumed'],
                          'mission_cap': MISSION_LIMIT}, sort_keys=True))
    finally:
        engine.dispose()


def resume():
    check_hold()
    if not (EVIDENCE / 'budget-amendment-outcome.json').exists():
        raise ValueError('Discovery amendment receipt required')
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': probe.BUDGET_KEY}).mappings().one()
            mission = session.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': MISSION_KEY}).mappings().one()
            others = session.execute(text('''SELECT count(*) FROM control.source_missions
                WHERE budget_scope_key=:key AND mission_key<>:mission AND state='enabled' '''),
                {'key': probe.BUDGET_KEY, 'mission': MISSION_KEY}).scalar_one()
            if (budget['attempt_limit'] != SOURCE_LIMIT or budget['enabled']
                    or budget['restriction_scope'] != probe.RESTRICTION
                    or budget['min_interval_ms'] != 1000 or budget['max_concurrency'] != 1
                    or mission['attempt_limit'] != MISSION_LIMIT
                    or mission['state'] != 'paused' or mission['attempts_consumed'] >= MISSION_LIMIT
                    or budget['attempts_consumed'] != EXPECTED_SOURCE_CONSUMED + mission['attempts_consumed']
                    or others):
                raise ValueError('Discovery resume baseline drift')
            session.execute(text('''UPDATE control.source_budgets SET enabled=true
                WHERE scope_key=:key'''), {'key': probe.BUDGET_KEY})
            session.execute(text('''UPDATE control.source_missions SET state='enabled'
                WHERE mission_key=:key'''), {'key': MISSION_KEY})
        print(json.dumps({'state': 'discovery_enabled'}))
    finally:
        engine.dispose()


def verify_live():
    check_hold()
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT attempt_limit,attempts_consumed,enabled
                FROM control.source_budgets WHERE scope_key=:key'''),
                {'key': probe.BUDGET_KEY}).mappings().one()
            mission = session.execute(text('''SELECT attempt_limit,attempts_consumed,state
                FROM control.source_missions WHERE mission_key=:key'''),
                {'key': MISSION_KEY}).mappings().one()
            if (budget['attempt_limit'] != SOURCE_LIMIT or not budget['enabled']
                    or mission['attempt_limit'] != MISSION_LIMIT or mission['state'] != 'enabled'
                    or mission['attempts_consumed'] >= MISSION_LIMIT
                    or budget['attempts_consumed'] != EXPECTED_SOURCE_CONSUMED + mission['attempts_consumed']):
                raise ValueError('Discovery ledger drift')
    finally:
        engine.dispose()


def main():
    configure()
    if len(sys.argv) > 1 and sys.argv[1] == 'scan-window':
        parser = argparse.ArgumentParser(description='Scan one bounded window over current modalities')
        parser.add_argument('command')
        parser.add_argument('--start', required=True)
        parser.add_argument('--end', required=True)
        parser.add_argument('--modalities', default=','.join(str(n) for n in range(1, 20)))
        args = parser.parse_args()
        modalities = [int(value) for value in args.modalities.split(',')]
        if (not modalities or len(set(modalities)) != len(modalities)
                or any(value < 1 or value > 19 for value in modalities)):
            raise ValueError('Current modality IDs 1-19 required')
        original_argv = sys.argv
        try:
            for modality in modalities:
                for operation in ('publication', 'update'):
                    verify_live()
                    sys.argv = [original_argv[0], operation, '--start', args.start,
                                '--end', args.end, '--modality', str(modality)]
                    outcome = probe.main()
                    if outcome:
                        return outcome
                    time.sleep(1.2)
            return 0
        finally:
            sys.argv = original_argv
    if len(sys.argv) > 1 and sys.argv[1] == 'prepare':
        prepare()
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == 'resume':
        resume()
        return 0
    if len(sys.argv) > 1 and sys.argv[1] not in ('status', 'pause'):
        verify_live()
    return probe.main()


if __name__ == '__main__':
    raise SystemExit(main())
