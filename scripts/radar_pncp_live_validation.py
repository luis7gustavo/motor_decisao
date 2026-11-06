"""Operator-bounded PNCP calibration and one-page isolated real collection."""
import argparse
from collections import Counter
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
    return 0 if (result['state']=='success' or result['error_code']=='sample_page_limit') else 2


def assess_session_evidence(ledger,job_file,jobs,responses,observation_counts):
    """Reconcile growing C2 receipts and promoted pages without fixed sample counts."""
    issues=[]
    warnings=[]
    attempts=ledger['attempts']
    data_attempts=[entry for entry in attempts if entry.get('category')=='data']
    job_ids={str(job['id']) for job in jobs}
    collection=next((job for job in jobs if str(job['id'])==job_file['job_id']),None)
    if ledger['session']!=SESSION or collection is None:
        issues.append('session_or_collection_job_mismatch')
    elif collection['checkpoint'].get('scope')!=job_file['scope']:
        issues.append('collection_scope_mismatch')
    if sum(job['request_count'] for job in jobs)!=len(data_attempts):
        issues.append('request_count_ledger_mismatch')
    receipt_signatures=Counter((entry['body_sha256'],entry['status'],entry['bytes'])
        for entry in data_attempts if 'body_sha256' in entry)
    bronze_signatures=Counter()
    public_responses=[]
    for response in responses:
        body=bytes(response['body'])
        if str(response['job_id']) not in job_ids:
            issues.append('bronze_outside_session')
        if len(body)!=response['byte_length'] or hashlib.sha256(body).hexdigest()!=response['body_sha256']:
            issues.append('bronze_body_hash_or_size_mismatch')
        bronze_signatures[(response['body_sha256'],response['http_status'],response['byte_length'])]+=1
        metadata={}
        if response['http_status']==200:
            try:
                envelope=json.loads(body)
                if isinstance(envelope,dict):
                    metadata={key:envelope.get(key) for key in
                              ('totalRegistros','totalPaginas','numeroPagina','paginasRestantes')}
            except (ValueError,UnicodeDecodeError):
                warnings.append('unparseable_200_receipt_retained')
        public_responses.append({'job_id':str(response['job_id']),'page':response['page'],
                                 'status':response['http_status'],'bytes':response['byte_length'],
                                 'sha256':response['body_sha256'],'page_metadata':metadata})
    if bronze_signatures!=receipt_signatures:
        issues.append('bronze_receipt_mismatch')
    for job in jobs:
        identity=str(job['id'])
        cp=job['checkpoint']
        if cp.get('session')!=SESSION or job['records_seen']!=cp.get('processed'):
            issues.append('job_checkpoint_mismatch')
        seen=cp.get('seen',[])
        if len(seen)!=len(set(seen)) or len(seen)!=job['records_seen']:
            issues.append('job_identity_count_mismatch')
        if observation_counts.get(identity,0)!=job['records_seen']:
            issues.append('job_observation_count_mismatch')
        accepted={response['page'] for response in responses
                  if str(response['job_id'])==identity and response['http_status'] in (200,204)}
        if not set(range(1,job['next_page'])).issubset(accepted):
            issues.append('advanced_cursor_without_bronze_page')
        if job['error_code']=='SourceChanged':
            change=cp.get('source_change')
            if (not isinstance(change,dict) or change.get('page')!=job['next_page']
                    or change.get('reconciliation_policy')!='child_partition_replay_required'
                    or change.get('replays_started',3)>change.get('max_replays',2)
                    or not any(str(response['id'])==change.get('raw_response_id')
                               and str(response['job_id'])==identity for response in responses)):
                issues.append('source_change_evidence_mismatch')
        elif job['error_code']=='ValueError':
            # Existing C2 job predates typed SourceChanged; preserve its state.
            last_page=next((response for response in reversed(public_responses)
                            if response['job_id']==identity and response['page']==job['next_page']),None)
            if (last_page and 'total' in cp and
                    (last_page['page_metadata'].get('totalRegistros')!=cp['total']
                     or last_page['page_metadata'].get('totalPaginas')!=cp['pages'])):
                warnings.append('legacy_untyped_pagination_change')
    return sorted(set(issues)),sorted(set(warnings)),public_responses


def verify(store):
    record=json.loads((ROOT/'target-credentials.json').read_text())
    budget=Budget(ROOT,category='data',purpose='collection')
    ledger=budget.state()
    job_file=json.loads((ROOT/'collection-job.json').read_text())
    file_issues=[]
    for entry in ledger['attempts']:
        if 'body_file' not in entry:
            continue
        name=entry['body_file']
        if Path(name).name!=name:
            file_issues.append('invalid_receipt_filename')
            continue
        try:
            body=(ROOT/name).read_bytes()
        except OSError:
            file_issues.append('receipt_file_missing')
            continue
        if len(body)!=entry['bytes'] or hashlib.sha256(body).hexdigest()!=entry['body_sha256']:
            file_issues.append('receipt_file_hash_or_size_mismatch')
    with store.transaction() as conn:
        counts={table:conn.execute(text('SELECT count(*) FROM '+table)).scalar_one() for table in (
            'bronze.pncp_responses','control.ingestion_jobs','commercial.system_settings',
            'procurement.procurements','procurement.procurement_versions',
            'procurement.version_observations','procurement.procurement_items','procurement.judgment_units')}
        jobs=[dict(row) for row in conn.execute(text('''SELECT id,state,error_code,next_page,
                  request_count,records_seen,window_start,window_end,checkpoint
                  FROM control.ingestion_jobs WHERE checkpoint->>'session'=:session
                  ORDER BY created_at,id'''),{'session':SESSION}).mappings()]
        responses=[dict(row) for row in conn.execute(text('''SELECT b.id,b.job_id,b.page,
                    b.http_status,b.body,octet_length(b.body) AS byte_length,b.body_sha256
                    FROM bronze.pncp_responses b JOIN control.ingestion_jobs j ON j.id=b.job_id
                    WHERE j.checkpoint->>'session'=:session ORDER BY b.received_at,b.id'''),
                    {'session':SESSION}).mappings()]
        observation_counts={str(row['correlation_id']):row['records'] for row in conn.execute(text('''
            SELECT o.correlation_id,count(*) AS records FROM procurement.version_observations o
            JOIN control.ingestion_jobs j ON j.id=o.correlation_id
            WHERE j.checkpoint->>'session'=:session GROUP BY o.correlation_id'''),
            {'session':SESSION}).mappings()}
        modality_distribution=[dict(row) for row in conn.execute(text('''SELECT snapshot->>'modalidadeId' AS code,
            snapshot->>'modalidadeNome' AS label,count(*) AS records FROM procurement.procurement_versions
            GROUP BY 1,2 ORDER BY 1,2''')).mappings()]
        publication_dates=[dict(row) for row in conn.execute(text('''SELECT left(snapshot->>'dataPublicacaoPncp',10) AS date,
            count(*) AS records FROM procurement.procurement_versions GROUP BY 1 ORDER BY 1''')).mappings()]
    issues,warnings,public_responses=assess_session_evidence(
        ledger,job_file,jobs,responses,observation_counts)
    issues=sorted(set(issues+file_issues))
    public_jobs=[{'job_id':str(row['id']),'state':row['state'],'error_code':row['error_code'],
                  'page':row['next_page'],'requests':row['request_count'],'records':row['records_seen'],
                  'start':row['window_start'].isoformat(),'end':row['window_end'].isoformat(),
                  'source_change':row['checkpoint'].get('source_change')}
                 for row in jobs]
    collection=next((row for row in public_jobs if row['job_id']==job_file['job_id']),None)
    result={'database':record['database'],'database_oid':record['database_oid'],'role':record['role'],
        'evidence_integrity_valid':not issues,'collection_business_state':collection['state'] if collection else 'missing',
        'coverage_verified':False,'issues':issues,'warnings':warnings,
        'counts':counts,'jobs':public_jobs,'bronze_responses':public_responses,
        'modality_distribution':modality_distribution,'publication_dates':publication_dates,
        'quota':ledger['counters']}
    print(json.dumps(result,indent=2))
    return int(bool(issues))


def classify(store):
    """Classify only the preserved C2 job from its saved Bronze page."""
    job_file=json.loads((ROOT/'collection-job.json').read_text())
    job_id=UUID(job_file['job_id'])
    before=store.get(job_id)
    if before['checkpoint'].get('scope')!=job_file['scope']:
        raise ValueError('Saved C2 scope differs from the database job')
    after=store.classify_saved_source_change(job_id,session_id=SESSION)
    print(json.dumps({'job_id':str(job_id),'state':after['state'],
                      'error_code':after['error_code'],'next_page':after['next_page'],
                      'requests_unchanged':after['request_count']==before['request_count'],
                      'records_unchanged':after['records_seen']==before['records_seen'],
                      'source_change':after['checkpoint'].get('source_change')},indent=2))
    return int(not (after['error_code']=='SourceChanged'
                    and after['next_page']==before['next_page']
                    and after['request_count']==before['request_count']
                    and after['records_seen']==before['records_seen']))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['calibrate','collect','verify','classify'])
    args=parser.parse_args()
    engine,store=target()
    try:
        if args.mode=='calibrate':
            return calibrate(store)
        if args.mode=='collect':
            return collect(store)
        return classify(store) if args.mode=='classify' else verify(store)
    finally:
        engine.dispose()


if __name__=='__main__':
    raise SystemExit(main())
