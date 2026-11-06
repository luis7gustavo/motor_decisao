"""Run the existing partial PNCP job through its manual Prefect flow only."""
import json
import os
from pathlib import Path
import re
from uuid import UUID

ROOT=Path('/evidence/c2/c2-20261013')


def main():
    target=json.loads((ROOT/'target-credentials.json').read_text())
    if (not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}',target.get('database',''))
            or not re.fullmatch(r'radar_test_[a-f0-9]{12}',target.get('role',''))):
        raise ValueError('Persistent isolated target identity is invalid')
    job_id=UUID(json.loads((ROOT/'collection-job.json').read_text())['job_id'])
    os.environ.update(DATABASE_URL=target['url'],RADAR_TEST_DB=target['database'],
        RADAR_TEST_OID=str(target['database_oid']),RADAR_TEST_ROLE=target['role'],
        PREFECT_API_URL='http://prefect-server:4200/api',PREFECT_SERVER_ANALYTICS_ENABLED='false')
    from pipelines.procurement.pncp_flow import collect_pncp
    if collect_pncp.retries!=0:
        raise ValueError('Manual flow must keep Prefect retries disabled')
    state=collect_pncp(str(job_id),return_state=True)
    flow_run_id=getattr(state.state_details,'flow_run_id',None)
    summary={'flow':'radar-pncp-publicacao','flow_run_id':str(flow_run_id) if flow_run_id else None,
        'state':state.name,'retries':collect_pncp.retries,'job_id':str(job_id)}
    if state.is_completed():
        result=state.result()
        summary.update(result={key:result.get(key) for key in
            ('state','next_page','records','requests','error_code')})
        print(json.dumps(summary,indent=2))
        return 0
    print(json.dumps(summary,indent=2))
    return 1


if __name__=='__main__':
    raise SystemExit(main())
