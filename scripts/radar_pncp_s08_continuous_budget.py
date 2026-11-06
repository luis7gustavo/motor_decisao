"""One-time audited extension of the paused PNCP history budget.

This command never starts the scheduler or sends a PNCP request. The immutable
attempt ledger remains cumulative; the daily cap is enforced on reservation.
"""

import hashlib
import json
from pathlib import Path

from app.procurement.pncp_transport import durable_write
from app.procurement.source_control import SourceControl
from app.procurement.source_isolation import HOLD_PATH, require_source_hold
from app.procurement.source_policy import load_source_policy
from scripts.radar_pncp_history import policy_plan, target_store


ROOT = Path('/evidence/c2/c2-20261013')
BACKUP_REPORT = ROOT / 'history-20261103-prebudget-restore-report.json'
RECEIPT = Path('/evidence/pncp-history-budget/20261103-continuous-budget.json')
HISTORY_KEY = 'pncp-history-2026-10-27'
BUDGET_KEY = 'pncp:public-read:shared-docker-egress'
RESTRICTION = 'unauthenticated-public-read-shared-docker-egress'


def verified_backup():
    report = json.loads(BACKUP_REPORT.read_text(encoding='utf-8'))
    if (report.get('restored_equal') is not True
            or report.get('full_restore_exit_code') != 0
            or report.get('rehearsal_cleaned') is not True
            or report.get('invalid_bronze_hashes') != 0
            or report.get('target') != 'test_20261020_ba894ce71405'
            or report.get('revision') != '20261027_0012'
            or report['counts']['control.source_attempts']
            != report['counts']['control.source_attempt_outcomes']):
        raise ValueError('Verified paused C2 backup and restore required')
    dump = ROOT / report['backup_name']
    if dump.name != report['backup_name'] or not dump.is_file():
        raise ValueError('Protected backup path invalid')
    with dump.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    if dump.stat().st_size != report['backup_bytes'] or digest != report['backup_sha256']:
        raise ValueError('Protected backup drift')
    return report


def main():
    require_source_hold(HOLD_PATH, HISTORY_KEY)
    backup = verified_backup()
    policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
    limits = policy['mission_budget']
    if (policy['mission_id'] != HISTORY_KEY or not policy_plan(policy)['ready']
            or limits['stable_budget_scope_key'] != BUDGET_KEY
            or limits['effective_restriction_scope'] != RESTRICTION
            or limits['external_attempts_authorized_now'] != 20000
            or limits['shared_source_budget_total_cap'] != 20099
            or limits['daily_attempt_limit'] != 200
            or policy['timezone'] != 'America/Sao_Paulo'):
        raise ValueError('Reviewed daily and cumulative limits differ')
    if RECEIPT.exists():
        raise FileExistsError('Budget amendment receipt already exists')
    store, engine = target_store()
    try:
        result = SourceControl(store).amend_paused_history_limits(
            scope_key=BUDGET_KEY, mission_key=HISTORY_KEY, source='pncp',
            restriction_scope=RESTRICTION,
            expected_source_limit=2100, expected_source_consumed=2099,
            new_source_limit=20099,
            expected_mission_limit=2000, expected_mission_consumed=2000,
            new_mission_limit=20000, daily_attempt_limit=200,
            daily_timezone='America/Sao_Paulo')
        receipt = {'state': 'amended_paused', 'backup_report': BACKUP_REPORT.name,
                   'backup_sha256': backup['backup_sha256'],
                   'previous_source_limit': 2100, 'previous_mission_limit': 2000,
                   **result, 'pncp_network_calls': 0}
        RECEIPT.parent.mkdir(parents=True, exist_ok=True)
        durable_write(RECEIPT, (json.dumps(receipt, sort_keys=True) + '\n').encode())
        print(json.dumps(receipt, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
