"""PNCP publication adapter, isolated-only until the source policy is approved.

Bronze commits before normalization. A page and its checkpoint commit together.
No HTTP runs on import and no connection is derived from a production default.
"""
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import random
import time
from urllib.parse import urlencode
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from .repository import ProcurementRepository, Snapshot
from .pncp_transport import Budget, BudgetExhausted, Response, retry_delay

RESOURCE = '/v1/contratacoes/publicacao'
BASE = 'https://pncp.gov.br/api/consulta'


def json_text(value):
    def convert(item):
        if isinstance(item, Decimal) and item.is_finite():
            return format(item,'f')
        raise ValueError('Unsupported JSON value')
    return json.dumps(value,default=convert,sort_keys=True,separators=(',',':'),allow_nan=False)


def reject_constant(_):
    raise ValueError('Nonfinite JSON number')


def offset_instant(value):
    if not isinstance(value,str):
        return None
    try:
        instant = datetime.fromisoformat(value.replace('Z','+00:00'))
        return instant if instant.tzinfo is not None and instant.utcoffset() is not None else None
    except ValueError:
        return None


@dataclass(frozen=True)
class Scope:
    start: date
    end: date
    modality: int
    contract_hash: str
    page_size: int = 10
    uf: str | None = None

    def __post_init__(self):
        if (type(self.start) is not date or type(self.end) is not date or self.start>self.end
                or type(self.modality) is not int or self.modality<=0
                or type(self.page_size) is not int or not 10<=self.page_size<=50
                or not re.fullmatch('[0-9a-f]{64}',self.contract_hash)
                or self.uf is not None and not re.fullmatch('[A-Z]{2}',self.uf)):
            raise ValueError('Invalid fixed publication scope')

    def data(self):
        value=asdict(self)
        value.update(start=self.start.isoformat(),end=self.end.isoformat())
        return value

    @classmethod
    def from_data(cls,value):
        return cls(**{**value,'start':date.fromisoformat(value['start']),'end':date.fromisoformat(value['end'])})

    def url(self,page):
        if type(page) is not int or page<1:
            raise ValueError('Invalid page')
        params={'dataInicial':self.start.strftime('%Y%m%d'),'dataFinal':self.end.strftime('%Y%m%d'),
            'codigoModalidadeContratacao':self.modality,'pagina':page,'tamanhoPagina':self.page_size}
        if self.uf:
            params['uf']=self.uf
        return BASE+RESOURCE+'?'+urlencode(params)


def live_transport(budget_path,scope=None):
    policy=json.loads(Path('config/contracts/pncp-publicacao-policy.json').read_text())
    if not policy['enabled']:
        raise ValueError('Live publication blocked: temporal boundaries/terms/modality evidence pending')
    if scope is None or scope.contract_hash!=policy['contract_sha256']:
        raise ValueError('Scope and verified contract differ')
    contract=Path('config/contracts/pncp-consulta-20261013.json').read_bytes()
    if hashlib.sha256(contract).hexdigest()!=policy['contract_sha256']:
        raise ValueError('Contract snapshot integrity failed')
    if (not policy.get('bounds_evidence') or not policy.get('filtered_field')
            or not policy.get('limits_reviewed') or scope.modality not in policy.get('verified_modalities',[])):
        raise ValueError('Incomplete live source policy')
    budget=Budget(budget_path)
    budget.state()
    return budget.request


class Store:
    def __init__(self,engine,*,expected_database):
        if (expected_database!=os.environ.get('RADAR_TEST_DB')
                or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}',expected_database)
                or engine.url.database!=expected_database):
            raise ValueError('PNCP writes require the runner-owned isolated target')
        self.engine=engine
        self.expected_database=expected_database
        with self.transaction():
            pass

    @contextmanager
    def transaction(self):
        with Session(self.engine) as session, session.begin():
            identity=session.execute(text('''SELECT current_database(),current_user,
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)''')).one()
            expected=(self.expected_database,os.environ['RADAR_TEST_ROLE'],int(os.environ['RADAR_TEST_OID']),False)
            if tuple(identity)!=expected:
                raise ValueError('Isolated database identity/role mismatch')
            yield session

    def create(self,scope,*,session_id):
        if not session_id:
            raise ValueError('Session identifier required')
        job_id,run_id,policy_id=uuid4(),uuid4(),uuid4()
        params=scope.data()
        policy={'schema_version':'pncp-publication-1','scope':params,'session':session_id}
        checkpoint={'schema_version':'pncp-checkpoint-1','scope':params,'session':session_id,
            'processed':0,'seen':[],'page_attempts':0,'not_before':0}
        with self.transaction() as s:
            s.execute(text("INSERT INTO control.pipeline_runs(id,pipeline_name,status) VALUES (:id,'radar-pncp','created')"),{'id':run_id})
            s.execute(text('''INSERT INTO commercial.system_settings
                (id,namespace,version_no,values,schema_version,author,effective_at,content_hash)
                VALUES (:id,:namespace,1,CAST(:value AS jsonb),'pncp-publication-1','isolated-operator',now(),:hash)'''),
                {'id':policy_id,'namespace':'pncp-'+str(job_id),'value':json_text(policy),
                 'hash':hashlib.sha256(json_text(policy).encode()).hexdigest()})
            s.execute(text('''INSERT INTO control.ingestion_jobs
                (id,pipeline_run_id,scope_key,window_start,window_end,source,resource,policy_id,checkpoint)
                VALUES (:id,:run,:scope,:start,:end,'pncp',:resource,:policy,CAST(:checkpoint AS jsonb))'''),
                {'id':job_id,'run':run_id,'scope':hashlib.sha256(json_text(params).encode()).hexdigest(),
                 'start':scope.start,'end':scope.end,'resource':RESOURCE,'policy':policy_id,'checkpoint':json_text(checkpoint)})
        return job_id

    def get(self,job_id):
        with self.transaction() as s:
            row=s.execute(text('SELECT * FROM control.ingestion_jobs WHERE id=:id'),{'id':job_id}).mappings().one()
            return dict(row)

    def claim(self,job_id,owner):
        with self.transaction() as s:
            token=s.execute(text('''UPDATE control.ingestion_jobs SET state='running',lease_owner=:owner,
                lease_until=now()+interval '5 minutes',fencing_token=fencing_token+1
                WHERE id=:id AND state<>'success' AND (lease_owner IS NULL OR lease_until<now())
                RETURNING fencing_token'''),{'id':job_id,'owner':owner}).scalar_one_or_none()
            if token is None:
                raise RuntimeError('Job lease unavailable')
            return token

    def locked(self,s,job_id,owner,token):
        row=s.execute(text('''SELECT * FROM control.ingestion_jobs WHERE id=:id
            AND lease_owner=:owner AND fencing_token=:token AND lease_until>now() FOR UPDATE'''),
            {'id':job_id,'owner':owner,'token':token}).mappings().one_or_none()
        if row is None:
            raise RuntimeError('Expired or superseded ingestion worker')
        return dict(row)

    def note_attempt(self,job_id,owner,token):
        with self.transaction() as s:
            row=self.locked(s,job_id,owner,token)
            cp=row['checkpoint']
            if cp['page_attempts']>=3:
                raise RuntimeError('Page retry limit exhausted')
            cp['page_attempts']+=1
            s.execute(text('''UPDATE control.ingestion_jobs SET request_count=request_count+1,
                attempts=attempts+1,checkpoint=CAST(:cp AS jsonb),lease_until=now()+interval '5 minutes'
                WHERE id=:id'''),{'id':job_id,'cp':json_text(cp)})

    def bronze(self,job_id,owner,token,response,url):
        with self.transaction() as s:
            row=self.locked(s,job_id,owner,token)
            scope=Scope.from_data(row['checkpoint']['scope'])
            # A receipt belongs to a reserved attempt, even if two statuses
            # carry identical bytes. Reprocessing that same attempt is stable.
            fingerprint=hashlib.sha256(json_text({'url':url,'attempt':row['request_count']}).encode()).hexdigest()
            body_hash=hashlib.sha256(response.body).hexdigest()
            instant=offset_instant(response.received_at) or datetime.now(timezone.utc)
            raw=s.execute(text('''INSERT INTO bronze.pncp_responses
                (job_id,request_fingerprint,page,http_status,received_at,body,body_sha256,
                 headers_safe,contract_version,source,resource,contract_hash,request_params_safe,schema_version)
                VALUES (:job,:fingerprint,:page,:status,:received,:body,:hash,
                 CAST(:headers AS jsonb),'OpenAPI-1.0','pncp',:resource,:contract,CAST(:params AS jsonb),'pncp-bronze-1')
                ON CONFLICT(job_id,request_fingerprint,body_sha256) DO NOTHING RETURNING id,received_at'''),
                {'job':job_id,'fingerprint':fingerprint,'page':row['next_page'],'status':response.status,
                 'received':instant,'body':response.body,'hash':body_hash,'headers':json_text({
                     k:v for k,v in response.headers.items() if k in ('content-type','date','retry-after','etag','last-modified')}),
                 'resource':RESOURCE,'contract':scope.contract_hash,'params':json_text(scope.data())}).mappings().first()
            if raw is None:
                raw=s.execute(text('''SELECT id,received_at FROM bronze.pncp_responses WHERE
                    job_id=:job AND request_fingerprint=:fingerprint AND body_sha256=:hash'''),
                    {'job':job_id,'fingerprint':fingerprint,'hash':body_hash}).mappings().one()
            cp=row['checkpoint']
            cp['last_raw_id']=str(raw['id'])
            s.execute(text('UPDATE control.ingestion_jobs SET bytes_seen=bytes_seen+:bytes,checkpoint=CAST(:cp AS jsonb) WHERE id=:id'),
                {'id':job_id,'bytes':len(response.body),'cp':json_text(cp)})
            return dict(raw)

    def apply_page(self,job_id,owner,token,response,raw):
        with self.transaction() as s:
            row=self.locked(s,job_id,owner,token)
            cp=row['checkpoint']
            if response.status==204:
                if cp['processed']:
                    raise ValueError('Unexpected empty page during pagination')
                done=True
            else:
                envelope=json.loads(response.body,parse_float=Decimal,parse_constant=reject_constant)
                if not isinstance(envelope,dict) or not isinstance(envelope.get('data'),list):
                    raise ValueError('Invalid publication envelope')
                for key in ('numeroPagina','totalRegistros','totalPaginas','paginasRestantes'):
                    if type(envelope.get(key)) is not int or envelope[key]<0:
                        raise ValueError('Missing or invalid page metadata')
                data=envelope['data']
                page,total,pages,remaining=(envelope[key] for key in ('numeroPagina','totalRegistros','totalPaginas','paginasRestantes'))
                empty_ok=(not data and total==0 and page==1 and pages in (0,1)
                          and remaining==0 and cp['processed']==0)
                if (page!=row['next_page'] or (pages<page and not empty_ok)
                        or (remaining!=pages-page and not empty_ok)
                        or type(envelope.get('empty')) is not bool or envelope['empty']!= (len(data)==0)
                        or (not data and not empty_ok) or len(data)>cp['scope']['page_size']):
                    raise ValueError('Inconsistent or empty page metadata')
                if 'total' in cp and (cp['total']!=total or cp['pages']!=pages):
                    raise ValueError('Source changed pagination totals')
                if total<cp['processed']+len(data) or (remaining==0 and total!=cp['processed']+len(data)):
                    raise ValueError('Page record counts do not reconcile')
                ids=[]
                for record in data:
                    if not isinstance(record,dict) or not isinstance(record.get('numeroControlePNCP'),str) or not record['numeroControlePNCP'].strip():
                        raise ValueError('Missing official source identity')
                    key=hashlib.sha256(record['numeroControlePNCP'].encode()).hexdigest()
                    if key in cp['seen'] or key in ids:
                        raise ValueError('Repeated identity within pagination')
                    ids.append(key)
                repo=ProcurementRepository(s)
                for record in data:
                    org=record.get('orgaoEntidade') or {}
                    original=org.get('cnpj') if isinstance(org,dict) else None
                    if original is not None and not isinstance(original,str):
                        raise ValueError('Fiscal source must remain textual')
                    updated=record.get('dataAtualizacaoGlobal') or record.get('dataAtualizacao')
                    repo.record(Snapshot(source='pncp',source_id=record['numeroControlePNCP'],raw_response_id=raw['id'],
                        snapshot=record,source_manifest={'resource':RESOURCE,'contract_hash':cp['scope']['contract_hash'],
                            'coverage':{'header':'observed','items':'pending','documents':'pending'}},
                        schema_version='pncp-publication-1',observed_at=raw['received_at'],
                        source_updated_at=offset_instant(updated),source_time_text=updated if isinstance(updated,str) else None,
                        timezone_status='offset' if offset_instant(updated) else 'unknown',
                        deadline_at=offset_instant(record.get('dataEncerramentoProposta')),
                        official_status=record.get('situacaoCompraNome') if isinstance(record.get('situacaoCompraNome'),str) else None,
                        cnpj_original=original),actor='pncp-adapter',correlation_id=job_id)
                cp.update(processed=cp['processed']+len(data),seen=cp['seen']+ids,total=total,pages=pages)
                done=remaining==0
            cp.update(page_attempts=0,not_before=0)
            s.execute(text('''UPDATE control.ingestion_jobs SET checkpoint=CAST(:cp AS jsonb),next_page=next_page+1,
                records_seen=:count,last_success_at=now(),state=:state,error_code=NULL,
                lease_until=now()+interval '5 minutes' WHERE id=:id'''),
                {'id':job_id,'cp':json_text(cp),'count':cp['processed'],'state':'success' if done else 'running'})
            return done

    def release(self,job_id,owner,token,state,error=None,*,not_before=0):
        with self.transaction() as s:
            row=self.locked(s,job_id,owner,token)
            cp=row['checkpoint']
            cp['not_before']=max(cp.get('not_before',0),not_before)
            s.execute(text('''UPDATE control.ingestion_jobs SET state=:state,error_code=:error,
                checkpoint=CAST(:cp AS jsonb),lease_owner=NULL,lease_until=NULL WHERE id=:id'''),
                {'id':job_id,'state':state,'error':error,'cp':json_text(cp)})
            s.execute(text('UPDATE control.pipeline_runs SET status=:state,finished_at=now() WHERE id=:id'),
                {'id':row['pipeline_run_id'],'state':state})


class Collector:
    def __init__(self,store,send,*,sleep=time.sleep):
        self.store,self.send,self.sleep=store,send,sleep

    def run(self,job_id):
        row=self.store.get(job_id)
        if row['state']=='success' or row['checkpoint'].get('not_before',0)>time.time():
            return row
        owner=str(uuid4())
        token=self.store.claim(job_id,owner)
        try:
            while True:
                row=self.store.get(job_id)
                if row['checkpoint']['page_attempts']>=3:
                    self.store.release(job_id,owner,token,'partial','retry_exhausted')
                    return self.store.get(job_id)
                scope=Scope.from_data(row['checkpoint']['scope'])
                url=scope.url(row['next_page'])
                try:
                    if isinstance(getattr(self.send,'__self__',None),Budget):
                        response=self.send(url,on_reserved=lambda:self.store.note_attempt(job_id,owner,token))
                    else:
                        self.store.note_attempt(job_id,owner,token)
                        response=self.send(url)
                except BudgetExhausted:
                    self.store.release(job_id,owner,token,'partial','quota_exhausted')
                    return self.store.get(job_id)
                except (OSError,TimeoutError):
                    self.sleep(2**(row['checkpoint']['page_attempts']+1)+random.uniform(0,0.25))
                    continue
                raw=self.store.bronze(job_id,owner,token,response,url)
                if response.status in (200,204):
                    if self.store.apply_page(job_id,owner,token,response,raw):
                        self.store.release(job_id,owner,token,'success')
                        return self.store.get(job_id)
                    continue
                if response.status==429 or 500<=response.status<600:
                    delay=retry_delay(response.headers.get('retry-after'),2**(row['checkpoint']['page_attempts']+1)+random.uniform(0,0.25))
                    if delay>30:
                        self.store.release(job_id,owner,token,'partial','retry_deferred',not_before=time.time()+delay)
                        return self.store.get(job_id)
                    self.sleep(delay)
                    continue
                self.store.release(job_id,owner,token,'partial','http_'+str(response.status))
                return self.store.get(job_id)
        except Exception as exc:
            # Persist only error type: source contents and credentials stay out
            # of operational logs. A stale owner cannot release another lease.
            self.store.release(job_id,owner,token,'partial',type(exc).__name__)
            return self.store.get(job_id)
