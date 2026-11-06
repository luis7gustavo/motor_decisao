"""Second bounded S06 probe campaign, sharing the original durable source budget.

Prepare extends the paused budget from 12 to 36 only after a verified backup.
The original mission stays paused; no command retries or follows redirects.
"""

from datetime import date
import hashlib
import json
from pathlib import Path
import sys

from sqlalchemy import text

from app.procurement.pncp_transport import durable_write
from app.procurement.source_control import SourceControl
from scripts import radar_pncp_s06_probe as probe
from scripts.radar_pncp_history import target_store


MISSION_KEY = 'pncp-s06-calibration-2026-10-29'
OLD_MISSION_KEY = 'pncp-s06-probe-2026-10-28'
EVIDENCE = Path('/evidence/pncp-probe/20261029')
BACKUP_REPORT = Path('/evidence/c2/c2-20261013/calibration-20261029-restore-report.json')
SOURCE_LIMIT = 36  # Existing 12 plus 24 new calibration attempts.
MISSION_LIMIT = 24


def _configure_probe():
    probe.MISSION_KEY = MISSION_KEY
    probe.ATTEMPT_LIMIT = MISSION_LIMIT
    probe.SOURCE_BUDGET_LIMIT = SOURCE_LIMIT
    probe.MISSION_KIND = 'bounded_s06_calibration'
    probe.REQUEST_TIMEOUT_SECONDS = 60
    probe.EVIDENCE = EVIDENCE


def prepare():
    report = json.loads(BACKUP_REPORT.read_text())
    if report.get('restored_equal') is not True:
        raise ValueError('Verified isolated C2 restore required')
    backup_name = report.get('backup_name')
    if not isinstance(backup_name, str) or Path(backup_name).name != backup_name:
        raise ValueError('Invalid protected backup name')
    dump = BACKUP_REPORT.parent / backup_name
    digest = hashlib.sha256()
    with dump.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    if dump.stat().st_size != report.get('backup_bytes') or digest.hexdigest() != report.get('backup_sha256'):
        raise ValueError('Protected backup differs from verified restore report')
    probe.checked_contracts()
    store, engine = target_store()
    try:
        control = SourceControl(store)
        with store.transaction() as session:
            budget = session.execute(text('''SELECT scope_key,source,restriction_scope,
                attempt_limit,attempts_consumed,enabled,min_interval_ms,max_concurrency
                FROM control.source_budgets WHERE scope_key=:key'''),
                {'key': probe.BUDGET_KEY}).mappings().one()
            old = session.execute(text('''SELECT mission_key,attempt_limit,
                attempts_consumed,state FROM control.source_missions
                WHERE mission_key=:key'''), {'key': OLD_MISSION_KEY}).mappings().one()
        if (budget['source'] != 'pncp' or budget['restriction_scope'] != probe.RESTRICTION
                or budget['attempt_limit'] != 12 or budget['attempts_consumed'] != 11
                or budget['enabled'] or budget['min_interval_ms'] != 1000
                or budget['max_concurrency'] != 1 or old['attempt_limit'] != 12
                or old['attempts_consumed'] != 11 or old['state'] != 'paused'):
            raise ValueError('Paused S06 source budget baseline drift')
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        intent = EVIDENCE / 'budget-amendment-intent.json'
        outcome = EVIDENCE / 'budget-amendment-outcome.json'
        if intent.exists() or outcome.exists():
            raise FileExistsError('Budget amendment evidence already exists; inspect before retry')
        durable_write(intent, json.dumps({'scope_key': probe.BUDGET_KEY,
            'old_limit': 12, 'expected_consumed': 11, 'new_limit': SOURCE_LIMIT,
            'new_mission': MISSION_KEY, 'new_mission_limit': MISSION_LIMIT,
            'backup_report': BACKUP_REPORT.name}, sort_keys=True).encode())
        amended = control.amend_paused_budget(scope_key=probe.BUDGET_KEY,
            source='pncp', restriction_scope=probe.RESTRICTION,
            expected_limit=12, expected_consumed=11, new_limit=SOURCE_LIMIT)
        mission = control.register_mission(mission_key=MISSION_KEY, source='pncp',
            budget_scope_key=probe.BUDGET_KEY, contract_hash=probe.CONSULTATION_HASH,
            fixed_end=date(2026, 10, 26),
            scope={'kind': 'bounded_s06_calibration', 'max_attempts': MISSION_LIMIT,
                   'integration_contract_hash': probe.INTEGRATION_HASH},
            attempt_limit=MISSION_LIMIT, state='paused')
        durable_write(outcome, json.dumps({'scope_key': probe.BUDGET_KEY,
            'attempt_limit': amended['attempt_limit'],
            'attempts_consumed': amended['attempts_consumed'],
            'enabled': amended['enabled'], 'mission_id': str(mission['id']),
            'mission_state': mission['state']}, sort_keys=True).encode())
        print(json.dumps({'state': 'prepared_paused', 'source_limit': SOURCE_LIMIT,
                          'source_consumed': amended['attempts_consumed'],
                          'mission_limit': MISSION_LIMIT}, sort_keys=True))
    finally:
        engine.dispose()


def resume():
    if not (EVIDENCE / 'budget-amendment-outcome.json').exists():
        raise ValueError('Budget amendment receipt required before resume')
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': probe.BUDGET_KEY}).mappings().one()
            old = session.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': OLD_MISSION_KEY}).mappings().one()
            mission = session.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': MISSION_KEY}).mappings().one()
            if (budget['attempt_limit'] != SOURCE_LIMIT
                    or budget['attempts_consumed'] != old['attempts_consumed'] + mission['attempts_consumed']
                    or budget['enabled'] or budget['restriction_scope'] != probe.RESTRICTION
                    or budget['min_interval_ms'] != 1000 or budget['max_concurrency'] != 1
                    or old['state'] != 'paused' or mission['state'] != 'paused'
                    or mission['attempt_limit'] != MISSION_LIMIT
                    or mission['attempts_consumed'] >= MISSION_LIMIT):
                raise ValueError('Calibration resume baseline drift')
            session.execute(text('''UPDATE control.source_budgets SET enabled=true
                WHERE scope_key=:key'''), {'key': probe.BUDGET_KEY})
            session.execute(text('''UPDATE control.source_missions SET state='enabled'
                WHERE mission_key=:key'''), {'key': MISSION_KEY})
        print(json.dumps({'state': 'calibration_enabled',
                          'source_consumed': budget['attempts_consumed']}))
    finally:
        engine.dispose()


def verify_live_campaign():
    store, engine = target_store()
    try:
        with store.transaction() as session:
            budget = session.execute(text('''SELECT attempt_limit,attempts_consumed,
                enabled,min_interval_ms,max_concurrency,restriction_scope
                FROM control.source_budgets WHERE scope_key=:key'''),
                {'key': probe.BUDGET_KEY}).mappings().one()
            old = session.execute(text('''SELECT attempts_consumed,state
                FROM control.source_missions WHERE mission_key=:key'''),
                {'key': OLD_MISSION_KEY}).mappings().one()
            mission = session.execute(text('''SELECT attempt_limit,attempts_consumed,state
                FROM control.source_missions WHERE mission_key=:key'''),
                {'key': MISSION_KEY}).mappings().one()
        if (budget['attempt_limit'] != SOURCE_LIMIT or not budget['enabled']
                or budget['min_interval_ms'] != 1000 or budget['max_concurrency'] != 1
                or budget['restriction_scope'] != probe.RESTRICTION
                or old['state'] != 'paused' or old['attempts_consumed'] != 11
                or mission['state'] != 'enabled' or mission['attempt_limit'] != MISSION_LIMIT
                or mission['attempts_consumed'] >= MISSION_LIMIT
                or budget['attempts_consumed'] != old['attempts_consumed'] + mission['attempts_consumed']):
            raise ValueError('Calibration source ledger or mission drift')
    finally:
        engine.dispose()


def main():
    _configure_probe()
    if len(sys.argv) > 1 and sys.argv[1] == 'prepare':
        prepare()
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == 'resume':
        resume()
        return 0
    if len(sys.argv) > 1 and sys.argv[1] not in ('status', 'pause'):
        verify_live_campaign()
    return probe.main()


if __name__ == '__main__':
    raise SystemExit(main())
