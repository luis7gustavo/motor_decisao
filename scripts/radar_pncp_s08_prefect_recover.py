"""Cancel only stale, unstarted Prefect runs left by the runtime interruption."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (DeploymentFilter, DeploymentFilterId,
    FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType)
from prefect.client.schemas.sorting import FlowRunSort
from prefect.states import Cancelled, StateType

from app.procurement.pncp_transport import durable_write


DEPLOYMENT = UUID('9183361f-fe0e-4a10-8469-d348f765fdb7')
ORPHAN = UUID('01a1a4b5-441c-7fe0-ab04-4008f37ca222')
RECEIPT = Path('/evidence/pncp-history-budget/20261104-prefect-stale-runs.json')


async def main():
    if RECEIPT.exists():
        raise FileExistsError('Prefect recovery receipt already exists')
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)
    cancelled = []
    async with get_client() as client:
        orphan = await client.read_flow_run(ORPHAN)
        if (orphan.deployment_id != DEPLOYMENT
                or orphan.state_type != StateType.PENDING
                or orphan.state_name != 'Submitting'
                or orphan.start_time is not None
                or orphan.expected_start_time >= cutoff):
            raise ValueError('Orphan no longer matches reviewed state')
        await client.set_flow_run_state(
            ORPHAN, Cancelled(message='Orphaned submitting run after runtime restart'),
            force=True)
        cancelled.append(str(ORPHAN))
        scheduled = await client.read_flow_runs(
            deployment_filter=DeploymentFilter(id=DeploymentFilterId(any_=[DEPLOYMENT])),
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(
                type=FlowRunFilterStateType(any_=[StateType.SCHEDULED]))),
            sort=FlowRunSort.EXPECTED_START_TIME_ASC, limit=200)
        stale = [run for run in scheduled if run.expected_start_time < cutoff]
        if len(stale) == 200:
            raise ValueError('Stale run batch exceeds reviewed bound')
        for run in stale:
            current = await client.read_flow_run(run.id)
            if (current.deployment_id != DEPLOYMENT
                    or current.state_type != StateType.SCHEDULED
                    or current.expected_start_time >= cutoff):
                continue
            await client.set_flow_run_state(
                run.id, Cancelled(message='Stale scheduled run after runtime restart'),
                force=True)
            cancelled.append(str(run.id))
        after = [await client.read_flow_run(UUID(run_id)) for run_id in cancelled]
        if any(run.state_type != StateType.CANCELLED for run in after):
            raise ValueError('Prefect stale run cancellation did not persist')
    receipt = {'state': 'stale_runs_cancelled', 'deployment_id': str(DEPLOYMENT),
               'cutoff_utc': cutoff.isoformat(), 'cancelled_count': len(cancelled),
               'cancelled_run_ids': cancelled, 'pncp_network_calls': 0,
               'procurement_database_writes': 0}
    RECEIPT.parent.mkdir(parents=True, exist_ok=True)
    durable_write(RECEIPT, (json.dumps(receipt, sort_keys=True) + '\n').encode())
    print(json.dumps({key: value for key, value in receipt.items()
                      if key != 'cancelled_run_ids'}, sort_keys=True))


if __name__ == '__main__':
    asyncio.run(main())
