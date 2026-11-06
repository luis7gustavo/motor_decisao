"""Isolated PNCP history backfill. The transport is supplied by the caller.

Every attempt is reserved before transport. Bronze and the attempt receipt commit
before a page is interpreted; projection, identity index and cursor commit together.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from sqlalchemy import text

from .pncp import BASE, RESOURCE, Scope, SourceChanged, offset_instant, reject_constant
from .pncp_transport import retry_delay, validate_url
from .repository import ProcurementRepository, Snapshot
from .source_control import SourceControl, _hash, _json
from .source_policy import load_source_policy
from .source_isolation import HOLD_PATH, require_source_hold


NAMESPACE = UUID('58a7f178-88ac-447d-a3ed-76d4c4301b5e')
MAX_PAGE_ATTEMPTS = 3
UPDATE_RESOURCE = '/v1/contratacoes/atualizacao'
LIST_RESOURCES = frozenset({RESOURCE, UPDATE_RESOURCE})


def scope_url(scope, resource, page):
    if resource not in LIST_RESOURCES:
        raise ValueError('Unvalidated list resource')
    return scope.url(page).replace(BASE + RESOURCE, BASE + resource, 1)


def invalid_json_200(response):
    """Classify throttling-like 200 bodies before any page promotion."""
    if response.status != 200:
        return None
    content_type = response.headers.get('content-type', '').lower()
    if 'text/html' in content_type or response.body.lstrip()[:32].lower().startswith(
            (b'<!doctype html', b'<html')):
        return 'html_200'
    try:
        json.loads(response.body, parse_float=Decimal, parse_constant=reject_constant)
    except (ValueError, UnicodeError, TypeError):
        return 'invalid_json_200'
    return None


def windows(start: date, end: date, days: int):
    if not isinstance(start, date) or not isinstance(end, date) or start > end or type(days) is not int or days < 1:
        raise ValueError('A proven start, fixed end and positive window are required')
    cursor = start
    while cursor <= end:
        last = min(end, cursor + timedelta(days=days - 1))
        yield cursor, last
        cursor = last + timedelta(days=1)


def job_uuid(partition_id):
    return uuid5(NAMESPACE, 'history-job:' + str(partition_id))


def snapshot_from_record(record, raw, scope, resource):
    if not isinstance(record, dict) or not isinstance(record.get('numeroControlePNCP'), str):
        raise ValueError('Missing official identity')
    org = record.get('orgaoEntidade') or {}
    original = org.get('cnpj') if isinstance(org, dict) else None
    if original is not None and not isinstance(original, str):
        raise ValueError('Fiscal source must remain textual')
    updated = record.get('dataAtualizacaoGlobal') or record.get('dataAtualizacao')
    return Snapshot(source='pncp', source_id=record['numeroControlePNCP'],
        raw_response_id=raw['id'], snapshot=record,
        source_manifest={'resource': resource, 'contract_hash': scope.contract_hash,
            'coverage': {'header': 'observed', 'items': 'pending', 'documents': 'pending'}},
        schema_version='pncp-publication-1', observed_at=raw['received_at'],
        source_updated_at=offset_instant(updated),
        source_time_text=updated if isinstance(updated, str) else None,
        timezone_status='offset' if offset_instant(updated) else 'unknown',
        deadline_at=offset_instant(record.get('dataEncerramentoProposta')),
        official_status=record.get('situacaoCompraNome') if isinstance(record.get('situacaoCompraNome'), str) else None,
        cnpj_original=original, canonicalizer_version='radar-internal-2')


class HistoryBackfill:
    def __init__(self, isolated_store, send, *, allow_synthetic=False):
        self.store = isolated_store
        self.control = SourceControl(isolated_store)
        self.send = send
        self.allow_synthetic = allow_synthetic

    def _live_gate(self, mission=None, resource=None):
        if self.allow_synthetic:
            return
        policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
        require_source_hold(HOLD_PATH, policy['mission_id'])
        history = policy.get('history') or {}
        budget = policy.get('mission_budget') or {}
        if (policy.get('enabled') is not True or policy.get('contract_currently_revalidated') is not True
                or history.get('coverage_start_status') not in ('verified', 'verified_accessible')
                or history.get('modality_catalog_status') not in ('verified', 'verified_accessible')
                or history.get('fixed_end_filter_semantics_validated') is not True
                or not budget.get('external_attempts_authorized_now')):
            raise ValueError('Historical source network gate is blocked')
        if resource is not None:
            gate = 'publication' if resource == RESOURCE else 'global_update' if resource == UPDATE_RESOURCE else None
            if gate is None or policy.get('resource_gates', {}).get(gate) != 'enabled':
                raise ValueError('Resource network gate is blocked')
        contract = Path(policy['contract_snapshot']).read_bytes()
        if hashlib.sha256(contract).hexdigest() != policy['contract_sha256']:
            raise ValueError('Archived contract hash drift')
        if mission is not None and (mission['mission_key'] != policy['mission_id']
                or mission['contract_hash'].strip() != policy['contract_sha256']
                or mission['coverage_start'].isoformat() != history['coverage_start']
                or mission['fixed_end'].isoformat() != history['fixed_end_local_date']
                or mission['scope']['modalities'] != history['modality_catalog']):
            raise ValueError('Mission differs from reviewed source policy')

    def plan(self, *, mission_key, budget_scope_key, restriction_scope, contract_hash,
             coverage_start, fixed_end, modalities, window_days, attempt_limit=0,
              enabled=False, page_size=50, source_attempt_limit=None,
              historical_resources=(RESOURCE,), daily_attempt_limit=None,
              daily_timezone=None):
        if (not modalities or any(type(m) is not int or m <= 0 for m in modalities)
                or len(set(modalities)) != len(modalities)):
            raise ValueError('Validated modality catalogue required')
        if (not historical_resources or len(set(historical_resources)) != len(historical_resources)
                or any(resource not in LIST_RESOURCES for resource in historical_resources)):
            raise ValueError('Validated historical list resources required')
        Scope(coverage_start, fixed_end, modalities[0], contract_hash, page_size)
        spans = list(windows(coverage_start, fixed_end, window_days))
        if source_attempt_limit is None:
            source_attempt_limit = attempt_limit
        if (type(source_attempt_limit) is not int or source_attempt_limit < attempt_limit):
            raise ValueError('Shared source cap must include mission cap')
        if enabled:
            self._live_gate(resource=RESOURCE)
            if UPDATE_RESOURCE in historical_resources:
                self._live_gate(resource=UPDATE_RESOURCE)
            if not self.allow_synthetic:
                policy = load_source_policy('config/contracts/pncp-history-mission-policy.json')
                history = policy['history']
                budget_policy = policy['mission_budget']
                if (mission_key != policy['mission_id'] or contract_hash != policy['contract_sha256']
                        or coverage_start.isoformat() != history['coverage_start']
                        or fixed_end.isoformat() != history['fixed_end_local_date']
                        or list(modalities) != history['modality_catalog']
                        or attempt_limit != budget_policy['external_attempts_authorized_now']
                        or source_attempt_limit != budget_policy['shared_source_budget_total_cap']
            or daily_attempt_limit != budget_policy.get('daily_attempt_limit')
            or daily_timezone != policy.get('timezone')
                        or restriction_scope != budget_policy['effective_restriction_scope']
                        or budget_scope_key != budget_policy['stable_budget_scope_key']):
                    raise ValueError('Plan differs from reviewed mission policy')
                interval = budget_policy['effective_min_interval_ms']
                concurrency = budget_policy['effective_max_concurrency']
                if (type(interval) is not int or interval < 1000
                        or type(concurrency) is not int or concurrency != 1):
                    raise ValueError('Source limit policy unresolved')
            else:
                interval, concurrency = 0, 1
        else:
            interval, concurrency = 0 if self.allow_synthetic else 1000, 1
        budget = self.control.register_budget(
            scope_key=budget_scope_key, source='pncp', restriction_scope=restriction_scope,
            attempt_limit=source_attempt_limit, min_interval_ms=interval,
            max_concurrency=concurrency, enabled=enabled,
            adaptive_interval=(not self.allow_synthetic and enabled and
                               budget_policy.get('adaptive_rate_test') is True))
        mission_scope = {'resource': RESOURCE, 'modalities': modalities,
                         'window_days': window_days, 'page_size': page_size,
                         'backfill_reserve_attempts': max(1, attempt_limit // 5) if attempt_limit else 0,
                         'coverage_basis': 'synthetic' if self.allow_synthetic else 'pending'}
        if daily_attempt_limit is not None:
            if type(daily_attempt_limit) is not int or daily_attempt_limit < 1 or not daily_timezone:
                raise ValueError('Invalid daily mission limit')
            mission_scope.update(daily_attempt_limit=daily_attempt_limit,
                                 daily_timezone=daily_timezone)
        if not self.allow_synthetic and enabled and budget_policy.get('adaptive_rate_test') is True:
            mission_scope.update(test_window_start_utc=budget_policy['test_window_start_utc'],
                                 test_window_end_utc=budget_policy['test_window_end_utc'])
        mission = self.control.register_mission(
            mission_key=mission_key, source='pncp', budget_scope_key=budget_scope_key,
            contract_hash=contract_hash, coverage_start=coverage_start, fixed_end=fixed_end,
            scope=mission_scope,
            attempt_limit=attempt_limit, state='enabled' if enabled else 'blocked')
        parts = [self.control.register_partition(
            mission['id'], resource=resource, modality_code=modality,
            window_start=first, window_end=last)
            for resource in historical_resources for modality in modalities
            for first, last in spans]
        return {'budget': budget, 'mission': mission, 'partitions': parts}

    def _job(self, part, mission):
        """A stable job ID links attempts and Bronze without a new schema column."""
        job_id = job_uuid(part['id'])
        run_id = uuid5(NAMESPACE, 'run:' + str(part['id']))
        policy_id = uuid5(NAMESPACE, 'policy:' + str(part['id']))
        scope = Scope(part['window_start'], part['window_end'], part['modality_code'],
                      mission['contract_hash'].strip(), mission['scope']['page_size'])
        policy = {'schema_version': 'pncp-history-partition-1', 'scope': scope.data(),
                  'resource': part['resource'],
                  'mission_key': mission['mission_key'], 'partition_id': str(part['id'])}
        with self.store.transaction() as s:
            s.execute(text("""INSERT INTO control.pipeline_runs(id,pipeline_name,status)
                VALUES (:id,'radar-pncp-history','created') ON CONFLICT(id) DO NOTHING"""), {'id': run_id})
            s.execute(text("""INSERT INTO commercial.system_settings
                (id,namespace,version_no,values,schema_version,author,effective_at,content_hash)
                VALUES (:id,:namespace,1,CAST(:value AS jsonb),'pncp-history-partition-1',
                'isolated-history',now(),:hash) ON CONFLICT(id) DO NOTHING"""),
                {'id': policy_id, 'namespace': 'pncp-history-' + str(part['id']),
                 'value': _json(policy), 'hash': _hash(policy)})
            s.execute(text("""INSERT INTO control.ingestion_jobs
                (id,pipeline_run_id,scope_key,window_start,window_end,source,resource,policy_id,checkpoint)
                VALUES (:id,:run,:scope,:start,:end,'pncp',:resource,:policy,CAST(:checkpoint AS jsonb))
                ON CONFLICT(id) DO NOTHING"""),
                {'id': job_id, 'run': run_id, 'scope': part['scope_key'],
                 'start': part['window_start'], 'end': part['window_end'],
                 'resource': part['resource'], 'policy': policy_id,
                 'checkpoint': _json({'schema_version': 'pncp-history-checkpoint-1',
                                      'processed': 0, 'total': None, 'pages': None})})
            row = s.execute(text('SELECT scope_key,policy_id FROM control.ingestion_jobs WHERE id=:id'),
                            {'id': job_id}).one()
            if row.scope_key != part['scope_key'] or row.policy_id != policy_id:
                raise ValueError('History job identity conflict')
        return job_id, scope

    def _capture(self, part, job_id, attempt_id, scope, response, *, outcome=None):
        if type(response.status) is not int or not 100 <= response.status <= 599 or not isinstance(response.body, bytes):
            raise ValueError('Invalid transport response')
        safe_headers = {k.lower(): v for k, v in response.headers.items()
                        if k.lower() in {'content-type', 'date', 'retry-after', 'etag', 'last-modified'}}
        received = offset_instant(response.received_at) or datetime.now(timezone.utc)
        body_hash = hashlib.sha256(response.body).hexdigest()
        with self.store.transaction() as s:
            raw = s.execute(text("""INSERT INTO bronze.pncp_responses
                (job_id,request_fingerprint,page,http_status,received_at,body,body_sha256,
                 headers_safe,contract_version,source,resource,contract_hash,request_params_safe,schema_version)
                VALUES (:job,:fingerprint,:page,:status,:received,:body,:hash,
                 CAST(:headers AS jsonb),'OpenAPI-1.0','pncp',:resource,:contract,
                 CAST(:params AS jsonb),'pncp-bronze-1') RETURNING id,received_at"""),
                {'job': job_id, 'fingerprint': _hash({'attempt': str(attempt_id)}),
                 'page': part['next_page'], 'status': response.status, 'received': received,
                 'body': response.body, 'hash': body_hash, 'headers': _json(safe_headers),
                 'resource': part['resource'], 'contract': scope.contract_hash,
                 'params': _json({'start': scope.start.isoformat(), 'end': scope.end.isoformat(),
                                  'modality': scope.modality, 'page': part['next_page'],
                                  'page_size': scope.page_size,
                                  'client_elapsed_ms': response.elapsed_ms})}).mappings().one()
            s.execute(text("""INSERT INTO control.source_attempt_outcomes
                (attempt_id,outcome,http_status,bronze_response_id)
                VALUES (:attempt,:outcome,:status,:raw)"""),
                {'attempt': attempt_id, 'outcome': outcome or 'http_' + str(response.status),
                 'status': response.status, 'raw': raw['id']})
            s.execute(text("""UPDATE control.ingestion_jobs SET request_count=request_count+1,
                bytes_seen=bytes_seen+:bytes WHERE id=:id"""),
                {'id': job_id, 'bytes': len(response.body)})
        return dict(raw)

    def _promote(self, part, owner, job_id, scope, response, raw):
        """No identity or cursor survives alone. A rejected page keeps its Bronze."""
        with self.store.transaction() as s:
            locked = s.execute(text("""SELECT * FROM control.source_partitions WHERE id=:id
                AND lease_owner=:owner AND fencing_token=:token AND lease_until>clock_timestamp()
                AND state='running' FOR UPDATE"""),
                {'id': part['id'], 'owner': owner, 'token': part['fencing_token']}).mappings().one_or_none()
            if locked is None or locked['next_page'] != part['next_page']:
                raise ValueError('Stale partition claim')
            job = s.execute(text('SELECT * FROM control.ingestion_jobs WHERE id=:id FOR UPDATE'),
                            {'id': job_id}).mappings().one()
            if job['next_page'] != locked['next_page']:
                raise ValueError('Job and partition cursors differ')
            cp = dict(job['checkpoint'])
            if response.status == 204:
                if cp['processed']:
                    raise SourceChanged('page_disappeared_during_pagination', page=locked['next_page'],
                                        raw_response_id=raw['id'])
                records, total, pages, remaining = [], 0, 0, 0
            else:
                envelope = json.loads(response.body, parse_float=Decimal, parse_constant=reject_constant)
                if not isinstance(envelope, dict) or not isinstance(envelope.get('data'), list):
                    raise ValueError('Invalid publication envelope')
                for key in ('numeroPagina', 'totalRegistros', 'totalPaginas', 'paginasRestantes'):
                    if type(envelope.get(key)) is not int or envelope[key] < 0:
                        raise ValueError('Invalid page metadata')
                records = envelope['data']
                page, total, pages, remaining = (envelope[k] for k in
                    ('numeroPagina', 'totalRegistros', 'totalPaginas', 'paginasRestantes'))
                empty = not records and total == 0 and page == 1 and pages in (0, 1) and remaining == 0
                if page != locked['next_page']:
                    raise SourceChanged('page_sequence_changed', page=locked['next_page'],
                                        raw_response_id=raw['id'], expected={'page': locked['next_page']},
                                        observed={'page': page})
                if (len(records) > scope.page_size or remaining != max(0, pages - page)
                        or (pages < page and not empty)
                        or type(envelope.get('empty')) is not bool
                        or envelope['empty'] != (len(records) == 0)):
                    raise ValueError('Inconsistent page metadata')
                if not records and not empty:
                    raise SourceChanged('page_disappeared_during_pagination', page=page,
                                        raw_response_id=raw['id'])
                if cp['total'] is not None and (cp['total'], cp['pages']) != (total, pages):
                    raise SourceChanged('pagination_totals_changed', page=page, raw_response_id=raw['id'],
                                        expected={'total_records': cp['total'], 'total_pages': cp['pages']},
                                        observed={'total_records': total, 'total_pages': pages})
                if total < cp['processed'] + len(records) or (remaining == 0 and total != cp['processed'] + len(records)):
                    raise SourceChanged('page_record_counts_changed', page=page, raw_response_id=raw['id'])
            ids = []
            for record in records:
                if not isinstance(record, dict) or not isinstance(record.get('numeroControlePNCP'), str) or not record['numeroControlePNCP'].strip():
                    raise ValueError('Missing official identity')
                source_id = record['numeroControlePNCP']
                if source_id in ids:
                    raise SourceChanged('repeated_identity_within_page', page=locked['next_page'],
                                        raw_response_id=raw['id'])
                ids.append(source_id)
                if s.execute(text("""SELECT 1 FROM control.source_partition_identities
                    WHERE partition_id=:part AND pass_no=:pass AND source_id=:source"""),
                    {'part': part['id'], 'pass': locked['pass_no'], 'source': source_id}).first():
                    raise SourceChanged('repeated_identity_across_pages', page=locked['next_page'],
                                        raw_response_id=raw['id'])
            repo = ProcurementRepository(s)
            for record in records:
                repo.record(snapshot_from_record(record, raw, scope, part['resource']),
                    actor='pncp-history', correlation_id=job_id)
                s.execute(text("""INSERT INTO control.source_partition_identities
                    (partition_id,pass_no,source_id,raw_response_id,observed_at)
                    VALUES (:part,:pass,:source,:raw,:observed)"""),
                    {'part': part['id'], 'pass': locked['pass_no'],
                     'source': record['numeroControlePNCP'], 'raw': raw['id'],
                     'observed': raw['received_at']})
            cp.update(processed=cp['processed'] + len(records), total=total, pages=pages)
            complete = remaining == 0
            if complete:
                cp['coverage_status'] = 'pagination_consistent_unverified'
            new_state = 'complete' if complete else 'pending'
            s.execute(text("""UPDATE control.source_partitions SET next_page=next_page+1,
                state=:state,lease_owner=NULL,lease_until=NULL
                WHERE id=:id AND fencing_token=:token"""),
                {'id': part['id'], 'token': part['fencing_token'], 'state': new_state})
            s.execute(text("""UPDATE control.ingestion_jobs SET next_page=next_page+1,
                records_seen=:count,checkpoint=CAST(:cp AS jsonb),state=:state,
                last_success_at=clock_timestamp(),error_code=NULL WHERE id=:id"""),
                {'id': job_id, 'count': cp['processed'], 'cp': _json(cp),
                 'state': 'success' if complete else 'partial'})
        return complete

    def _release(self, part, owner, state, error=None):
        with self.store.transaction() as s:
            row = s.execute(text("""UPDATE control.source_partitions
                SET state=:state,lease_owner=NULL,lease_until=NULL
                WHERE id=:id AND lease_owner=:owner AND fencing_token=:token
                AND lease_until>clock_timestamp() RETURNING id"""),
                {'id': part['id'], 'owner': owner, 'token': part['fencing_token'],
                 'state': state}).first()
            if row is None:
                raise ValueError('Stale partition claim')
            s.execute(text('UPDATE control.ingestion_jobs SET state=:state,error_code=:error WHERE id=:id'),
                      {'id': job_uuid(part['id']), 'state': 'partial', 'error': error})

    def _defer_transient(self, budget_scope_key, delay):
        # A source-wide run of failures opens a persistent 15-minute circuit.
        with self.store.transaction() as s:
            recent = s.execute(text("""SELECT o.outcome FROM control.source_attempts a
                JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key ORDER BY a.sequence_no DESC LIMIT 3"""),
                {'key': budget_scope_key}).scalars().all()
            transient = lambda value: value in ('transport_error', 'http_429',
                'html_200', 'invalid_json_200') or (
            value.startswith('http_') and value[5:].isdigit() and int(value[5:]) >= 500)
        if len(recent) == 3 and all(transient(outcome) for outcome in recent):
            delay = max(delay, 900)
        return self.control.defer_source(budget_scope_key,
            datetime.now(timezone.utc) + timedelta(seconds=delay))

    def _source_change(self, part, owner, error):
        with self.store.transaction() as s:
            job_id = job_uuid(part['id'])
            job = s.execute(text('SELECT checkpoint FROM control.ingestion_jobs WHERE id=:id FOR UPDATE'),
                            {'id': job_id}).mappings().one()
            cp = dict(job['checkpoint'])
            cp['source_change'] = error.metadata
            row = s.execute(text("""UPDATE control.source_partitions
                SET state='reconcile',lease_owner=NULL,lease_until=NULL
                WHERE id=:id AND lease_owner=:owner AND fencing_token=:token
                AND lease_until>clock_timestamp() RETURNING replays_started,max_replays"""),
                {'id': part['id'], 'owner': owner, 'token': part['fencing_token']}).one_or_none()
            if row is None:
                raise ValueError('Stale partition claim')
            s.execute(text("""UPDATE control.ingestion_jobs SET checkpoint=CAST(:cp AS jsonb),
                state='partial',error_code='SourceChanged' WHERE id=:id"""),
                      {'id': job_id, 'cp': _json(cp)})
            # A changed multi-day partition is split without dropping parent or Bronze.
            if part['window_start'] < part['window_end'] and row.replays_started < row.max_replays:
                middle = part['window_start'] + timedelta(days=(part['window_end'] - part['window_start']).days // 2)
                s.execute(text("""UPDATE control.source_partitions
                    SET replays_started=replays_started+1 WHERE id=:id AND state='reconcile'
                    AND replays_started<max_replays"""), {'id': part['id']})
                for first, last in ((part['window_start'], middle), (middle + timedelta(days=1), part['window_end'])):
                    scope = {'resource': part['resource'], 'modality': part['modality_code'],
                             'start': first.isoformat(), 'end': last.isoformat(),
                             'pass': part['pass_no'] + 1, 'parent': str(part['id'])}
                    s.execute(text("""INSERT INTO control.source_partitions
                        (mission_id,parent_id,scope_key,resource,modality_code,window_start,
                         window_end,pass_no,priority,max_replays)
                        VALUES (:mission,:parent,:key,:resource,:modality,:start,:end,
                                :pass,:priority,:max) ON CONFLICT(mission_id,scope_key) DO NOTHING"""),
                        {'mission': part['mission_id'], 'parent': part['id'],
                         'key': _hash(scope), 'resource': part['resource'],
                         'modality': part['modality_code'], 'start': first, 'end': last,
                         'pass': part['pass_no'] + 1, 'priority': 60,
                         'max': part['max_replays']})

    def split_exhausted_windows(self, mission_id, *, limit=40):
        """Retain 5xx evidence and replace failing wide windows with children."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Invalid split batch limit')
        if not self.allow_synthetic:
            require_source_hold(HOLD_PATH, load_source_policy(
                'config/contracts/pncp-history-mission-policy.json')['mission_id'])
        split = []
        with self.store.transaction() as s:
            mission = s.execute(text('SELECT id FROM control.source_missions WHERE id=:id'),
                                {'id': mission_id}).one()
            parts = s.execute(text("""SELECT p.* FROM control.source_partitions p
                JOIN control.ingestion_jobs j ON j.scope_key=p.scope_key
                    AND j.resource=p.resource
                WHERE p.mission_id=:id
                    AND ((p.state='partial' AND j.error_code='retry_exhausted')
                      OR (p.state='pending'
                          AND j.error_code IN ('http_500','http_502','http_503','http_504')
                          AND p.window_end-p.window_start>=30))
                    AND p.window_start<p.window_end AND p.lease_owner IS NULL
                ORDER BY p.window_start,p.modality_code,p.id LIMIT :limit
                FOR UPDATE OF p"""), {'id': mission.id, 'limit': limit}).mappings().all()
            for part in parts:
                job_id = job_uuid(part['id'])
                job = s.execute(text("""SELECT checkpoint,error_code FROM control.ingestion_jobs
                    WHERE id=:id FOR UPDATE"""), {'id': job_id}).mappings().one_or_none()
                if job is None or job['error_code'] not in (
                        'retry_exhausted', 'http_500', 'http_502',
                        'http_503', 'http_504'):
                    continue
                days = (part['window_end'] - part['window_start']).days + 1
                chunk_days = 30 if days > 30 else max(1, days // 2)
                children = list(windows(part['window_start'],
                                        part['window_end'], chunk_days))
                if s.execute(text("""UPDATE control.source_partitions
                    SET state='reconcile',replays_started=replays_started+1
                    WHERE id=:id AND state IN ('partial','pending')
                    AND replays_started<max_replays
                    RETURNING id"""), {'id': part['id']}).one_or_none() is None:
                    continue
                checkpoint = dict(job['checkpoint'])
                checkpoint['split_reason'] = (
                    'retry_exhausted_wide_window'
                    if job['error_code'] == 'retry_exhausted'
                    else 'http_5xx_wide_window')
                checkpoint['split_children'] = [
                    [first.isoformat(), last.isoformat()] for first, last in children]
                s.execute(text("""UPDATE control.ingestion_jobs
                    SET checkpoint=CAST(:cp AS jsonb) WHERE id=:id"""),
                          {'id': job_id, 'cp': _json(checkpoint)})
                for first, last in children:
                    scope = {'resource': part['resource'],
                             'modality': part['modality_code'],
                             'start': first.isoformat(), 'end': last.isoformat(),
                             'pass': part['pass_no'] + 1, 'parent': str(part['id'])}
                    s.execute(text("""INSERT INTO control.source_partitions
                        (mission_id,parent_id,scope_key,resource,modality_code,
                         window_start,window_end,pass_no,priority,max_replays)
                        VALUES (:mission,:parent,:key,:resource,:modality,
                                :start,:end,:pass,100,:max)
                        ON CONFLICT (mission_id,scope_key) DO NOTHING"""),
                              {'mission': mission_id, 'parent': part['id'],
                               'key': _hash(scope), 'resource': part['resource'],
                               'modality': part['modality_code'], 'start': first,
                               'end': last, 'pass': part['pass_no'] + 1,
                               'max': part['max_replays']})
                split.append({'parent_id': str(part['id']),
                              'resource': part['resource'],
                              'modality': part['modality_code'],
                              'children': checkpoint['split_children']})
        return split

    def step(self, mission_id, *, owner=None, priority_floor=None, priority_ceiling=None,
             resource_filter=None):
        """Process at most one page; repeated calls are restart-safe."""
        if resource_filter is not None and resource_filter not in LIST_RESOURCES:
            raise ValueError('Unknown historical list resource')
        with self.store.transaction() as s:
            mission = dict(s.execute(text('SELECT * FROM control.source_missions WHERE id=:id'),
                                     {'id': mission_id}).mappings().one())
        if mission['state'] != 'enabled':
            return {'state': 'blocked'}
        self._live_gate(mission)
        owner = owner or str(uuid4())
        part = self.control.claim_partition(mission_id, owner,
            priority_floor=priority_floor, priority_ceiling=priority_ceiling,
            resource=resource_filter)
        if part is None:
            return {'state': 'idle'}
        try:
            self._live_gate(mission, part['resource'])
        except ValueError:
            with self.store.transaction() as s:
                s.execute(text("""UPDATE control.source_partitions SET state='blocked',
                    lease_owner=NULL,lease_until=NULL WHERE id=:id AND lease_owner=:owner
                    AND fencing_token=:token"""),
                    {'id': part['id'], 'owner': owner, 'token': part['fencing_token']})
            return {'state': 'blocked', 'reason': 'resource_gate'}
        job_id, scope = self._job(part, mission)
        url = scope_url(scope, part['resource'], part['next_page'])
        validate_url(url)
        request_hash = hashlib.sha256(url.encode()).hexdigest()
        with self.store.transaction() as s:
            attempts = s.execute(text("""SELECT count(*) FROM control.source_attempts
                WHERE job_id=:job AND request_hash=:hash"""),
                {'job': job_id, 'hash': request_hash}).scalar_one()
        if attempts >= MAX_PAGE_ATTEMPTS:
            self._release(part, owner, 'partial', 'retry_exhausted')
            return {'state': 'partial', 'reason': 'retry_exhausted'}
        work_kind = ('reconcile' if part['parent_id'] is not None or part['pass_no'] >= 1_000_000
                     else 'sync' if part['pass_no'] >= 100_000
                     or part['window_start'] > mission['fixed_end'] else 'backfill')
        reservation = self.control.reserve_attempt(mission_id, work_kind=work_kind,
                                                    request_hash=request_hash, job_id=job_id)
        if reservation['state'] != 'reserved':
            self._release(part, owner, 'pending', reservation['state'])
            return {'state': reservation['state'], 'not_before': reservation.get('not_before')}
        attempt_id = reservation['attempt_id']
        try:
            response = self.send(url)
        except (OSError, TimeoutError):
            self.control.record_outcome(attempt_id, outcome='transport_error')
            self._defer_transient(mission['budget_scope_key'], 2 ** (attempts + 1))
            self._release(part, owner, 'pending', 'transport_error')
            return {'state': 'deferred', 'reason': 'transport_error'}
        except BaseException:
            self.control.record_outcome(attempt_id, outcome='transport_exception')
            self._release(part, owner, 'partial', 'transport_exception')
            raise
        malformed = invalid_json_200(response)
        raw = self._capture(part, job_id, attempt_id, scope, response, outcome=malformed)
        adaptive = mission['scope'].get('test_window_end_utc') is not None
        if response.status == 403:
            self.control.defer_source(mission['budget_scope_key'],
                datetime.now(timezone.utc) + timedelta(hours=24))
            self._release(part, owner, 'pending', 'http_403')
            return {'state': 'blocked_waf', 'reason': 'http_403'}
        if response.status == 429 or malformed:
            if adaptive:
                self.control.adjust_adaptive_rate(mission['budget_scope_key'], throttled=True)
            delay = retry_delay(response.headers.get('retry-after'), 2 ** (attempts + 1))
            self._defer_transient(mission['budget_scope_key'], delay)
            reason = malformed or 'http_429'
            self._release(part, owner, 'pending', reason)
            return {'state': 'deferred', 'reason': reason}
        if 500 <= response.status < 600:
            delay = retry_delay(response.headers.get('retry-after'), 2 ** (attempts + 1))
            self._defer_transient(mission['budget_scope_key'], delay)
            self._release(part, owner, 'pending', 'http_' + str(response.status))
            return {'state': 'deferred', 'reason': 'http_' + str(response.status)}
        if response.status not in (200, 204):
            self._release(part, owner, 'partial', 'http_' + str(response.status))
            return {'state': 'partial', 'reason': 'http_' + str(response.status)}
        try:
            complete = self._promote(part, owner, job_id, scope, response, raw)
        except SourceChanged as error:
            self._source_change(part, owner, error)
            return {'state': 'reconcile', 'reason': error.metadata['reason']}
        except Exception as error:
            self._release(part, owner, 'partial', type(error).__name__)
            return {'state': 'partial', 'reason': type(error).__name__}
        if adaptive:
            self.control.adjust_adaptive_rate(mission['budget_scope_key'], throttled=False)
        return {'state': 'complete' if complete else 'page_committed',
                'partition_id': str(part['id']), 'page': part['next_page']}

    def status(self, mission_id):
        with self.store.transaction() as s:
            mission = s.execute(text('SELECT mission_key,state,attempts_consumed,attempt_limit,scope FROM control.source_missions WHERE id=:id'),
                                {'id': mission_id}).mappings().one()
            states = {r.state: r.count for r in s.execute(text("""SELECT state,count(*) AS count
                FROM control.source_partitions WHERE mission_id=:id GROUP BY state"""), {'id': mission_id})}
            scoped_states = {'historical': {}, 'recurring': {}}
            for row in s.execute(text("""SELECT CASE WHEN pass_no < 100000 THEN 'historical'
                ELSE 'recurring' END AS collection_scope, state, count(*) AS count
                FROM control.source_partitions WHERE mission_id=:id
                GROUP BY collection_scope,state"""), {'id': mission_id}):
                scoped_states[row.collection_scope][row.state] = row.count
            identities = s.execute(text("""SELECT count(*) FROM control.source_partition_identities i
                JOIN control.source_partitions p ON p.id=i.partition_id WHERE p.mission_id=:id"""),
                {'id': mission_id}).scalar_one()
            return {'mission_key': mission['mission_key'], 'state': mission['state'],
                    'attempts_consumed': mission['attempts_consumed'],
                    'attempt_limit': mission['attempt_limit'],
                    'test_window_start_utc': mission['scope'].get('test_window_start_utc'),
                    'test_window_end_utc': mission['scope'].get('test_window_end_utc'),
                    'partitions': states,
                    'historical_partitions': scoped_states['historical'],
                    'recurring_partitions': scoped_states['recurring'],
                    'identities': identities, 'coverage': 'partial_or_unverified'}

    def audit(self, mission_id):
        with self.store.transaction() as s:
            rows = s.execute(text("""SELECT p.id,p.parent_id,p.resource,p.modality_code,
                p.window_start,p.window_end,p.pass_no,p.state,p.next_page,p.replays_started,
                count(i.source_id) AS identities
                FROM control.source_partitions p LEFT JOIN control.source_partition_identities i
                ON i.partition_id=p.id WHERE p.mission_id=:id
                GROUP BY p.id ORDER BY p.window_start,p.modality_code,p.pass_no"""),
                {'id': mission_id}).mappings().all()
            return [{'partition_id': str(r['id']), 'parent_id': str(r['parent_id']) if r['parent_id'] else None,
                     'resource': r['resource'], 'modality': r['modality_code'],
                     'start': r['window_start'].isoformat(), 'end': r['window_end'].isoformat(),
                     'pass_no': r['pass_no'], 'state': r['state'], 'next_page': r['next_page'],
                     'replays_started': r['replays_started'], 'identities': r['identities'],
                     'coverage': 'pagination_consistent_unverified' if r['state'] == 'complete' else 'partial'}
                    for r in rows]
