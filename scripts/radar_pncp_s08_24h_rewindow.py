"""Rewindow a disrupted, paused 24-hour test after verified restore."""
import hashlib
import json
from pathlib import Path

from app.procurement.pncp_transport import durable_write
from app.procurement.source_control import SourceControl
from app.procurement.source_isolation import HOLD_PATH, require_source_hold
from app.procurement.source_policy import load_source_policy
from scripts.radar_pncp_history import policy_plan, target_store


ROOT = Path('/evidence/c2/c2-20261013')
BACKUP_REPORT = ROOT / 'history-20261104-pre-rewindow-restore-report.json'
RECEIPT = Path('/evidence/pncp-history-budget/20261104-24h-rewindow.json')
HISTORY_KEY = 'pncp-history-2026-10-27'
BUDGET_KEY = 'pncp:public-read:shared-docker-egress'
OLD_START = '2026-11-04T04:05:00+00:00'
OLD_END = '2026-11-05T04:05:00+00:00'


def main():
    require_source_hold(HOLD_PATH, HISTORY_KEY)
    report = json.loads(BACKUP_REPORT.read_text(encoding='utf-8'))
    if (report.get('restored_equal') is not True
            or report.get('full_restore_exit_code') != 0
            or report.get('rehearsal_cleaned') is not True
            or report.get('invalid_bronze_hashes') != 0
            or report.get('target') != 'test_20261020_ba894ce71405'
            or report.get('revision') != '20261027_0012'
            or report['counts']['control.source_attempts'] != 2212
            or report['counts']['control.source_attempt_outcomes'] != 2212):
        raise ValueError('Verified paused post-interruption backup required')
    dump = ROOT / report['backup_name']
    if not dump.is_file() or dump.name != report['backup_name']:
        raise ValueError('Protected backup path invalid')
    with dump.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    if dump.stat().st_size != report['backup_bytes'] or digest != report['backup_sha256']:
        raise ValueError('Protected backup drift')
    policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
    budget = policy['mission_budget']
    if (policy['mission_id'] != HISTORY_KEY or not policy_plan(policy)['ready']
            or budget.get('adaptive_rate_test') is not True
            or budget['external_attempts_authorized_now'] != 100000
            or budget['shared_source_budget_total_cap'] != 100099
            or budget['daily_attempt_limit'] != 90000):
        raise ValueError('Reviewed finite test policy differs')
    if RECEIPT.exists():
        raise FileExistsError('Rewindow receipt already exists')
    store, engine = target_store()
    try:
        result = SourceControl(store).rewindow_paused_history_test(
            scope_key=BUDGET_KEY, mission_key=HISTORY_KEY,
            expected_source_limit=100099, expected_mission_limit=100000,
            expected_source_consumed=2212, expected_mission_consumed=2113,
            old_start_utc=OLD_START, old_end_utc=OLD_END,
            new_start_utc=budget['test_window_start_utc'],
            new_end_utc=budget['test_window_end_utc'])
        receipt = {'state': 'rewindowed_paused', 'backup_report': BACKUP_REPORT.name,
                   'backup_sha256': report['backup_sha256'],
                   'interrupted_test_attempts': 100,
                   'previous_test_window_start_utc': OLD_START,
                   'previous_test_window_end_utc': OLD_END,
                   **result, 'pncp_network_calls': 0}
        RECEIPT.parent.mkdir(parents=True, exist_ok=True)
        durable_write(RECEIPT, (json.dumps(receipt, sort_keys=True) + '\n').encode())
        print(json.dumps(receipt, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
