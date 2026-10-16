"""Manual-only Prefect entrypoint. Uses an existing isolated ingestion job."""
import os
from pathlib import Path
import re
from uuid import UUID

from prefect import flow
from sqlalchemy import create_engine

from app.procurement.pncp import Collector, Scope, Store, live_transport


@flow(name='radar-pncp-publicacao',retries=0,log_prints=False,persist_result=False)
def collect_pncp(job_id: str) -> dict:
    # Credentials stay in the isolated process environment, never flow params.
    engine=create_engine(os.environ['DATABASE_URL'])
    try:
        store=Store(engine,expected_database=os.environ['RADAR_TEST_DB'])
        identity=UUID(job_id)
        row=store.get(identity)
        session_id=row['checkpoint']['session']
        if not re.fullmatch(r'[A-Za-z0-9_-]+',session_id):
            raise ValueError('Invalid durable session identifier')
        send=live_transport(Path('/evidence/c2')/session_id,Scope.from_data(row['checkpoint']['scope']))
        result=Collector(store,send).run(identity)
        return {'job_id':str(identity),'state':result['state'],'next_page':result['next_page'],
            'records':result['records_seen'],'requests':result['request_count'],'error_code':result['error_code']}
    finally:
        engine.dispose()
