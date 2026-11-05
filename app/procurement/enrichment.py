"""Contract-gated raw evidence queue for PNCP detail/items/results/documents.

The current policy has no validated read contracts for these resources, so
live enqueue/send remain blocked. Synthetic tests exercise retention only;
raw retrieval never implies parsed or complete procurement documentation.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4, uuid5

from sqlalchemy import text

from .history import NAMESPACE
from .pncp import offset_instant
from .pncp_transport import retry_delay, validate_url
from .source_control import SourceControl, _hash, _json
from .source_policy import load_source_policy


KINDS = frozenset({'procurement_detail', 'items', 'results', 'documents'})


class EnrichmentQueue:
    def __init__(self, isolated_store, send, *, allow_synthetic=False):
        self.store = isolated_store
        self.send = send
        self.control = SourceControl(isolated_store)
        self.allow_synthetic = allow_synthetic

    def _gate(self, kind, url, contract_hash):
        validate_url(url)
        if kind not in KINDS or len(contract_hash) != 64:
            raise ValueError('Invalid enrichment resource or contract')
        if self.allow_synthetic:
            return
        policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
        validated = policy.get('validated_resource_contracts', {}).get(kind)
        if (policy.get('enabled') is not True
                or policy.get('resource_gates', {}).get(kind) != 'enabled'
                or not isinstance(validated, dict)
                or validated.get('contract_sha256') != contract_hash
                or not validated.get('path_prefix')
                or not urlsplit(url).path.startswith(validated['path_prefix'])
                or kind == 'documents' and not validated.get('storage_policy_validated')):
            raise ValueError('Enrichment read contract is blocked')

    def enqueue(self, mission_id, *, kind, identity_key, url, cycle_key, contract_hash):
        if not identity_key or not cycle_key:
            raise ValueError('Enrichment identity and cycle required')
        self._gate(kind, url, contract_hash)
        key = _hash({'kind': kind, 'identity': identity_key, 'cycle': cycle_key,
                     'url': url, 'contract': contract_hash})
        return self.control.enqueue(mission_id, work_kind='enrich:' + kind,
            idempotency_key=key, identity_key=identity_key,
            payload={'url': url, 'contract_hash': contract_hash,
                     'kind': kind, 'cycle_key': cycle_key}, priority=40)

    def _job(self, queue, mission):
        job_id = uuid5(NAMESPACE, 'enrich-job:' + str(queue['id']))
        run_id = uuid5(NAMESPACE, 'enrich-run:' + str(queue['id']))
        policy_id = uuid5(NAMESPACE, 'enrich-policy:' + str(queue['id']))
        policy = {'schema_version': 'pncp-enrich-raw-1', 'queue_id': str(queue['id']),
                  'kind': queue['payload']['kind'], 'mission_key': mission['mission_key']}
        with self.store.transaction() as s:
            s.execute(text("""INSERT INTO control.pipeline_runs(id,pipeline_name,status)
                VALUES (:id,'radar-pncp-enrich','created') ON CONFLICT(id) DO NOTHING"""),
                {'id': run_id})
            s.execute(text("""INSERT INTO commercial.system_settings
                (id,namespace,version_no,values,schema_version,author,effective_at,content_hash)
                VALUES (:id,:namespace,1,CAST(:value AS jsonb),'pncp-enrich-raw-1',
                'isolated-history',now(),:hash) ON CONFLICT(id) DO NOTHING"""),
                {'id': policy_id, 'namespace': 'pncp-enrich-' + str(queue['id']),
                 'value': _json(policy), 'hash': _hash(policy)})
            s.execute(text("""INSERT INTO control.ingestion_jobs
                (id,pipeline_run_id,scope_key,window_start,window_end,source,resource,
                 policy_id,checkpoint)
                VALUES (:id,:run,:scope,:day,:day,'pncp',:resource,:policy,
                 CAST(:checkpoint AS jsonb)) ON CONFLICT(id) DO NOTHING"""),
                {'id': job_id, 'run': run_id, 'scope': queue['idempotency_key'],
                 'day': mission['fixed_end'], 'resource': queue['payload']['kind'],
                 'policy': policy_id,
                 'checkpoint': _json({'schema_version': 'pncp-enrich-raw-1',
                                      'domain_status': 'raw_only'})})
        return job_id

    def _capture(self, queue, job_id, attempt_id, response):
        if type(response.status) is not int or not 100 <= response.status <= 599 or not isinstance(response.body, bytes):
            raise ValueError('Invalid enrichment response')
        url = queue['payload']['url']
        safe_headers = {k.lower(): v for k, v in response.headers.items()
                        if k.lower() in {'content-type', 'date', 'retry-after', 'etag', 'last-modified'}}
        received = offset_instant(response.received_at) or datetime.now(timezone.utc)
        digest = hashlib.sha256(response.body).hexdigest()
        with self.store.transaction() as s:
            raw = s.execute(text("""INSERT INTO bronze.pncp_responses
                (job_id,request_fingerprint,page,http_status,received_at,body,body_sha256,
                 headers_safe,contract_version,source,resource,contract_hash,request_params_safe,schema_version)
                VALUES (:job,:fingerprint,NULL,:status,:received,:body,:digest,
                 CAST(:headers AS jsonb),'OpenAPI-1.0','pncp',:resource,:contract,
                 CAST(:params AS jsonb),'pncp-enrich-raw-1') RETURNING id"""),
                {'job': job_id, 'fingerprint': _hash({'attempt': str(attempt_id)}),
                 'status': response.status, 'received': received,
                 'body': response.body, 'digest': digest, 'headers': _json(safe_headers),
                 'resource': urlsplit(url).path,
                 'contract': queue['payload']['contract_hash'],
                 'params': _json({'kind': queue['payload']['kind'],
                                  'identity_key': queue['identity_key']})}).scalar_one()
            s.execute(text("""INSERT INTO control.source_attempt_outcomes
                (attempt_id,outcome,http_status,bronze_response_id)
                VALUES (:attempt,:outcome,:status,:raw)"""),
                {'attempt': attempt_id, 'outcome': 'http_' + str(response.status),
                 'status': response.status, 'raw': raw})
            s.execute(text("""UPDATE control.ingestion_jobs SET request_count=request_count+1,
                bytes_seen=bytes_seen+:size,state=:state,last_success_at=clock_timestamp()
                WHERE id=:job"""),
                {'size': len(response.body), 'state': 'success' if response.status in (200, 204) else 'partial',
                 'job': job_id})
        return raw, digest

    def step(self, mission_id, *, owner=None):
        owner = owner or str(uuid4())
        with self.store.transaction() as s:
            mission = dict(s.execute(text('SELECT * FROM control.source_missions WHERE id=:id'),
                                     {'id': mission_id}).mappings().one())
        if mission['state'] != 'enabled':
            return {'state': 'blocked'}
        queue = self.control.claim_queue(mission_id, owner, work_prefix='enrich:')
        if queue is None:
            return {'state': 'idle'}
        kind = queue['payload']['kind']
        try:
            self._gate(kind, queue['payload']['url'], queue['payload']['contract_hash'])
        except ValueError:
            self.control.release_queue(queue['id'], owner, queue['fencing_token'], state='blocked')
            return {'state': 'blocked', 'reason': 'resource_gate'}
        if queue['attempts'] > 3:
            self.control.release_queue(queue['id'], owner, queue['fencing_token'], state='failed')
            return {'state': 'failed', 'reason': 'retry_exhausted'}
        job_id = self._job(queue, mission)
        url = queue['payload']['url']
        reserved = self.control.reserve_attempt(mission_id, work_kind='enrich',
            request_hash=hashlib.sha256(url.encode()).hexdigest(), job_id=job_id)
        if reserved['state'] != 'reserved':
            state = 'blocked' if reserved['state'] in ('blocked', 'quota_exhausted',
                        'quota_reserved_for_backfill') else 'pending'
            self.control.release_queue(queue['id'], owner, queue['fencing_token'],
                state=state, not_before=reserved.get('not_before'))
            return {'state': reserved['state']}
        try:
            response = self.send(url)
        except (OSError, TimeoutError):
            self.control.record_outcome(reserved['attempt_id'], outcome='transport_error')
            until = self.control.defer_source(mission['budget_scope_key'],
                datetime.now(timezone.utc) + timedelta(seconds=2 ** queue['attempts']))
            self.control.release_queue(queue['id'], owner, queue['fencing_token'],
                state='pending', not_before=until)
            return {'state': 'deferred', 'reason': 'transport_error'}
        raw, digest = self._capture(queue, job_id, reserved['attempt_id'], response)
        if response.status in (200, 204):
            self.control.finish_queue(queue['id'], owner, queue['fencing_token'])
            return {'state': 'raw_collected', 'kind': kind, 'sha256': digest,
                    'raw_id': str(raw), 'domain_complete': False}
        if response.status == 429 or 500 <= response.status < 600:
            delay = retry_delay(response.headers.get('retry-after'), 2 ** queue['attempts'])
            until = self.control.defer_source(mission['budget_scope_key'],
                datetime.now(timezone.utc) + timedelta(seconds=delay))
            self.control.release_queue(queue['id'], owner, queue['fencing_token'],
                state='pending', not_before=until)
            return {'state': 'deferred', 'reason': 'http_' + str(response.status)}
        self.control.release_queue(queue['id'], owner, queue['fencing_token'], state='failed')
        return {'state': 'failed', 'reason': 'http_' + str(response.status)}
