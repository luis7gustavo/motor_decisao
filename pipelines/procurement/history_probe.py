"""Harmless S05 scheduler/worker probe; no procurement DB or source transport."""

from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.context import get_run_context


STATE = Path(os.environ.get('RADAR_HISTORY_PROBE_STATE',
                            '/evidence/s05-prefect-probe/state.json'))


@flow(name='radar-pncp-s05-probe', retries=0, log_prints=False, persist_result=False)
def run_probe(mode: str = 'probe') -> dict:
    if mode != 'probe':
        raise ValueError('Probe accepts only probe mode')
    run_id = str(get_run_context().flow_run.id)
    STATE.parent.mkdir(parents=True, exist_ok=True)
    with STATE.open('a+', encoding='utf-8') as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        stream.seek(0)
        content = stream.read()
        state = json.loads(content) if content else {'run_ids': [], 'last_run_utc': None}
        if run_id not in state['run_ids']:
            state['run_ids'].append(run_id)
            state['last_run_utc'] = datetime.now(timezone.utc).isoformat()
            stream.seek(0)
            stream.truncate()
            json.dump(state, stream, separators=(',', ':'))
            stream.flush()
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    result = {'business_outcome': 'synthetic_probe_complete', 'run_id': run_id,
              'durable_run_count': len(state['run_ids']), 'network_calls': 0,
              'procurement_database_writes': 0}
    create_markdown_artifact(key='pncp-history-s05-probe',
        description='Ensaio isolado do scheduler e worker',
        markdown=(f"# S05 Prefect probe\n\nRun `{run_id}`; "
                  f"contador persistente: {len(state['run_ids'])}; "
                  "chamadas PNCP: 0; escrita no banco de licitações: 0."))
    get_run_logger().info('S05 probe completed run_id=%s durable_count=%d',
                          run_id, len(state['run_ids']))
    return result
