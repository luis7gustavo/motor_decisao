"""Operator-bounded PNCP calibration and one-page isolated real collection."""
import argparse
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from uuid import UUID, uuid4

from sqlalchemy import create_engine, text

from app.procurement.pncp import Collector, Scope, Store
from app.procurement.pncp_transport import Budget

SESSION='c2-20261013'
ROOT=Path('/evidence/c2')/SESSION
CONTRACT=Path('/app/config/contracts/pncp-consulta-20261013.json')
MODALITY=6  # Narrow operator-authorized probe; the response is evidence, not a catalogue.


def target():
    record=json.loads((ROOT/'target-credentials.json').read_text())
    if (not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}',record.get('database',''))
            or not re.fullmatch(r'radar_test_[a-f0-9]{12}',record.get('role',''))):
        raise ValueError('Persistent isolated target identity is invalid')
    engine=create_engine(record['url'],connect_args={'connect_timeout':10},pool_pre_ping=True)
    import os
    os.environ['RADAR_TEST_DB']=record['database']
    os.environ['RADAR_TEST_OID']=str(record['database_oid'])
    os.environ['RADAR_TEST_ROLE']=record['role']
    return engine,Store(engine,expected_database=record['database'])


def contract_hash():
    body=CONTRACT.read_bytes()
    digest=hashlib.sha256(body).hexdigest()
    expected='a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d'
    if digest!=expected:
        raise ValueError('Pinned public contract snapshot hash mismatch')
    return digest


def archive_probe(store,budget,scope,label):
    job_id=store.create(scope,session_id=SESSION)
    owner=str(uuid4())
    token=store.claim(job_id,owner)
    url=scope.url(1)
    try:
        response=budget.request(url,on_reserved=lambda:store.note_attempt(job_id,owner,token))
        raw=store.bronze(job_id,owner,token,response,url)
        store.release(job_id,owner,token,'partial','calibration_only')
        if response.status==204:
            dates=[]
            count=0
        elif response.status==200:
            envelope=json.loads(response.body)
            if not isinstance(envelope,dict) or not isinstance(envelope.get('data'),list):
                raise ValueError('Calibration response envelope invalid')
            dates=[]
            for record in envelope['data']:
                value=record.get('dataPublicacaoPncp') if isinstance(record,dict) else None
                if isinstance(value,str):
                    try:
                        dates.append(datetime.fromisoformat(value.replace('Z','+00:00')).date().isoformat())
                    except ValueError:
                        pass
            count=len(envelope['data'])
        else:
            return {'label':label,'job_id':str(job_id),'status':response.status,
                    'bytes':len(response.body),'sha256':hashlib.sha256(response.body).hexdigest(),
                    'records':None,'publication_dates':[],'bronze_id':str(raw['id'])}
        return {'label':label,'job_id':str(job_id),'status':response.status,'bytes':len(response.body),
            'sha256':hashlib.sha256(response.body).hexdigest(),'records':count,
            'publication_dates':sorted(set(dates)),'missing_publication_date':count-len(dates),
            'bronze_id':str(raw['id'])}
    except Exception as exc:
        try:
            store.release(job_id,owner,token,'partial',type(exc).__name__)
        except Exception:
            pass
        return {'label':label,'job_id':str(job_id),'failure':type(exc).__name__}


def calibrate(store):
    digest=contract_hash()
    budget=Budget(ROOT,category='data',purpose='calibration')
    single=Scope(date(2026,10,13),date(2026,10,13),MODALITY,digest)
    extended=Scope(date(2026,10,13),date(2026,10,14),MODALITY,digest)
    first=archive_probe(store,budget,single,'13-13')
    second=archive_probe(store,budget,extended,'13-14')
    comparable=all(item.get('status') in (200,204) and 'publication_dates' in item for item in (first,second))
    result={'session':SESSION,'operator_authorized':True,'modality_code':MODALITY,
        'modality_label':'exploratory; response values must be reviewed',
        'scopes':[single.data(),extended.data()],'probes':[first,second],
        'comparable':comparable,
        'shared_dates':sorted(set(first.get('publication_dates',[]))&set(second.get('publication_dates',[]))),
        'only_single':sorted(set(first.get('publication_dates',[]))-set(second.get('publication_dates',[]))),
        'only_extended':sorted(set(second.get('publication_dates',[]))-set(first.get('publication_dates',[]))),
        'quota':budget.state()['counters']}
    path=ROOT/'calibration.json'
    temp=path.with_suffix('.json.tmp')
    temp.write_text(json.dumps(result,indent=2)+'\n')
    path.parent.mkdir(parents=True,exist_ok=True)
    temp.replace(path)
    path.chmod(0o600)
    print(json.dumps(result,indent=2))
    return int(not comparable)


def collect(store):
    evidence=json.loads((ROOT/'calibration.json').read_text())
    if not evidence.get('comparable'):
        raise ValueError('Valid two-window calibration must precede real collection')
    digest=contract_hash()
    scope=Scope(date(2026,10,6),date(2026,10,14),MODALITY,digest,page_size=10)
    job_file=ROOT/'collection-job.json'
    if job_file.exists():
        job_id=UUID(json.loads(job_file.read_text())['job_id'])
    else:
        job_id=store.create(scope,session_id=SESSION)
        temp=job_file.with_suffix('.json.tmp')
        temp.write_text(json.dumps({'job_id':str(job_id),'scope':scope.data()},indent=2)+'\n')
        temp.replace(job_file)
        job_file.chmod(0o600)
    budget=Budget(ROOT,category='data',purpose='collection')
    result=Collector(store,budget.request).run(job_id,max_pages=1)
    summary={'job_id':str(job_id),'state':result['state'],'error_code':result.get('error_code'),
        'request_count':result['request_count'],'records_seen':result['records_seen'],
        'next_page':result['next_page'],'window':scope.data(),'quota':budget.state()['counters']}
    print(json.dumps(summary,indent=2))
    return 0 if result['records_seen'] else 1


def verify(store):
    record=json.loads((ROOT/'target-credentials.json').read_text())
    budget=Budget(ROOT,category='data',purpose='collection')
    with store.transaction() as conn:
        counts={table:conn.execute(text('SELECT count(*) FROM '+table)).scalar_one() for table in (
            'bronze.pncp_responses','control.ingestion_jobs','commercial.system_settings',
            'procurement.procurements','procurement.procurement_versions',
            'procurement.version_observations','procurement.procurement_items','procurement.judgment_units')}
        jobs=[{'state':row['state'],'error_code':row['error_code'],'page':row['next_page'],
               'requests':row['request_count'],'records':row['records_seen'],
               'start':row['window_start'].isoformat(),'end':row['window_end'].isoformat()}
              for row in conn.execute(text('''SELECT state,error_code,next_page,request_count,records_seen,
                  window_start,window_end FROM control.ingestion_jobs WHERE checkpoint->>'session'=:session
                  ORDER BY created_at,id'''),{'session':SESSION}).mappings()]
        responses=[{'status':row['http_status'],'bytes':row['byte_length'],
                    'sha256':row['body_sha256']} for row in conn.execute(text('''SELECT http_status,
                    octet_length(body) AS byte_length,body_sha256 FROM bronze.pncp_responses
                    ORDER BY received_at,id''')).mappings()]
        modality_distribution=[dict(row) for row in conn.execute(text('''SELECT snapshot->>'modalidadeId' AS code,
            snapshot->>'modalidadeNome' AS label,count(*) AS records FROM procurement.procurement_versions
            GROUP BY 1,2 ORDER BY 1,2''')).mappings()]
        publication_dates=[dict(row) for row in conn.execute(text('''SELECT left(snapshot->>'dataPublicacaoPncp',10) AS date,
            count(*) AS records FROM procurement.procurement_versions GROUP BY 1 ORDER BY 1''')).mappings()]
    result={'database':record['database'],'database_oid':record['database_oid'],'role':record['role'],
        'counts':counts,'jobs':jobs,'bronze_responses':responses,
        'modality_distribution':modality_distribution,'publication_dates':publication_dates,
        'quota':budget.state()['counters']}
    print(json.dumps(result,indent=2))
    return int(counts['bronze.pncp_responses']!=3 or counts['procurement.procurements']!=10
               or budget.state()['counters']['data']['consumed']!=3)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['calibrate','collect','verify'])
    args=parser.parse_args()
    engine,store=target()
    try:
        if args.mode=='calibrate':
            return calibrate(store)
        return collect(store) if args.mode=='collect' else verify(store)
    finally:
        engine.dispose()


if __name__=='__main__':
    raise SystemExit(main())
