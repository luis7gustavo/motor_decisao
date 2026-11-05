"""Single Prefect owner for the history mission; all external work uses the CLI gate."""

from contextlib import redirect_stdout
import asyncio
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context

from scripts.radar_pncp_history import main as history_main, policy_plan
from app.procurement.source_policy import load_source_policy


CONFIG = Path('config/contracts/pncp-history-schedule.json')
POLICY = Path('config/contracts/pncp-history-mission-policy.json')


def _pause_finite_test(config):
    """Pause this deployment first, then its isolated source mission."""
    from scripts.radar_pncp_schedule import set_pause
    from scripts.radar_pncp_s06_activate_history import set_running
    asyncio.run(set_pause(config, False, True))
    set_running(False)


def _invoke(command: str, steps: int, resource: str | None = None) -> dict:
    output = io.StringIO()
    with redirect_stdout(output):
        argv = [command, '--steps', str(steps)]
        if command == 'audit':
            argv.append('--summary-only')
        if resource is not None:
            argv.extend(['--history-resource', resource])
        exit_code = history_main(argv)
    lines = output.getvalue().splitlines()
    payload = json.loads(lines[-1]) if lines else {'state': 'missing_cli_output'}
    return {'command': command, 'resource': resource,
            'exit_code': exit_code, 'result': payload}


def _record(result: dict) -> dict:
    state = result['business_outcome']
    audit = result.get('audit') or {}
    coverage = audit.get('result', {}).get('coverage_status', 'not_audited')
    historical = audit.get('result', {}).get('historical_coverage_status', coverage)
    create_markdown_artifact(
        key='pncp-history-business-outcome',
        description='Último resultado operacional da missão PNCP histórica',
        markdown=(f"# Missão PNCP histórica\n\nEstado de negócio: **{state}**.\n\n"
                  f"Modo: `{result['mode']}`. Comandos: "
                  f"{', '.join(item['command'] for item in result.get('commands', []))}.\n\n"
                  f"Marco histórico: `{historical}`. Estado agregado das partições: "
                  f"`{coverage}`.\n\n"
                  f"Chamadas externas no bloqueio: {result.get('network_calls_when_blocked', 'n/a')}."),
    )
    return result


@flow(name='radar-pncp-history', retries=0, log_prints=False, persist_result=False)
def run_history(mode: str = 'sync') -> dict:
    """Run one finite batch. Prefect completion never asserts business coverage."""
    logger = get_run_logger()
    if (os.environ.get('RADAR_HISTORY_RUNTIME_DISABLED') == '1'
            or os.environ.get('PREFECT_API_URL') == 'http://prefect-server:4200/api'):
        logger.warning('History worker is disabled on this control plane')
        return _record({'mode': mode, 'business_outcome': 'blocked_runtime_control_plane',
                        'commands': [], 'network_calls_when_blocked': 0})
    if mode not in ('backfill', 'sync', 'reconcile'):
        raise ValueError('Unknown history schedule mode')
    config = json.loads(CONFIG.read_text(encoding='utf-8'))
    policy = load_source_policy(POLICY)
    readiness = policy_plan(policy)
    if not readiness['ready']:
        logger.warning('History mission blocked by %d policy gates', len(readiness['gates']))
        return _record({'mode': mode, 'business_outcome': 'blocked_contract',
                        'gates': readiness['gates'], 'commands': [],
                        'network_calls_when_blocked': 0})
    if policy.get('mission_budget', {}).get('adaptive_rate_test') is True:
        observed = _invoke('status', 1)
        if observed['exit_code'] != 0:
            raise ValueError('Finite test status unavailable')
        end = datetime.fromisoformat(observed['result']['test_window_end_utc'].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) >= end:
            _pause_finite_test(config)
            logger.info('Finite 24-hour history test ended; deployment and source paused')
            return _record({'mode': mode, 'business_outcome': 'test_window_complete',
                            'commands': [], 'network_calls_when_blocked': 0})
    context = get_run_context()
    scheduled = getattr(context.flow_run, 'expected_start_time', None)
    if scheduled is not None:
        late = (datetime.now(timezone.utc) - scheduled.astimezone(timezone.utc)).total_seconds()
        if late > config['max_lateness_seconds'][mode]:
            logger.warning('Skipping stale scheduled %s run, age_seconds=%d', mode, late)
            return _record({'mode': mode, 'business_outcome': 'skipped_stale_schedule',
                            'age_seconds': int(late), 'commands': []})
    publication_steps = max(1, (config['backfill_steps'] + 1) // 2)
    update_steps = config['backfill_steps'] - publication_steps
    backfill = [('backfill', publication_steps, 'publication')]
    if update_steps:
        backfill.append(('backfill', update_steps, 'update'))
    steps = {
        'backfill': backfill,
        'sync': [('backfill', 1, 'publication'), ('backfill', 1, 'update'),
                 ('sync', config['sync_steps'], None),
                 ('enrich', config['enrich_steps'], None)],
        'reconcile': [('backfill', 1, 'publication'), ('backfill', 1, 'update'),
                      ('reconcile', config['reconcile_steps'], None)],
    }[mode]
    commands = []
    stop_reason = None
    for command, count, resource in steps:
        item = _invoke(command, count, resource)
        commands.append(item)
        batch_results = item['result'].get('results', [])
        logger.info('History command=%s resource=%s exit=%s states=%s',
                    command, resource, item['exit_code'],
                    [(result.get('state'), result.get('reason'))
                     for result in batch_results])
        if any(result.get('state') == 'blocked_waf' for result in batch_results):
            stop_reason = 'blocked_waf'
            break
        if any(result.get('state') == 'test_window_complete' for result in batch_results):
            stop_reason = 'test_window_complete'
            break
        if policy.get('mission_budget', {}).get('adaptive_rate_test') is True and any(
                result.get('state') == 'quota_exhausted' for result in batch_results):
            stop_reason = 'quota_exhausted'
            break
        if item['exit_code'] != 0:
            if command == 'backfill' and resource == 'publication':
                continue
            break
    # Audit is local and read-only. Run it even after a partial batch so gaps
    # remain visible to the next scheduled run and in the Prefect artifact.
    audit = _invoke('audit', 1)
    codes = [item['exit_code'] for item in commands] + [audit['exit_code']]
    if stop_reason is not None:
        _pause_finite_test(config)
        outcome = stop_reason
    elif any(code == 2 for code in codes):
        outcome = 'failed'
    elif any(code == 3 for code in codes):
        outcome = 'partial_or_deferred'
    else:
        outcome = 'batch_completed'
    logger.info('History mission mode=%s business_outcome=%s commands=%d',
                mode, outcome, len(commands))
    return _record({'mode': mode, 'business_outcome': outcome,
                    'commands': commands, 'audit': audit})
