"""Bounded PNCP publication/update sync and conservative reconciliation.

The coordinator creates durable daily partitions. The S03 worker owns requests,
Bronze, versioning, fencing and the shared attempt budget. No schedule is made.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from zoneinfo import ZoneInfo

from sqlalchemy import text

from .history import HistoryBackfill, RESOURCE, UPDATE_RESOURCE, LIST_RESOURCES, job_uuid, snapshot_from_record
from .pncp import Scope, reject_constant
from .repository import ProcurementRepository
from .source_control import SourceControl


LOCAL_ZONE = ZoneInfo('America/Sao_Paulo')
EPOCH = datetime(2020, 1, 1, tzinfo=timezone.utc)


def cycle_number(cycle_at, *, broad=False):
    if not isinstance(cycle_at, datetime) or cycle_at.tzinfo is None or cycle_at.utcoffset() is None:
        raise ValueError('Cycle time must include an offset')
    slot = int((cycle_at.astimezone(timezone.utc) - EPOCH).total_seconds() // (6 * 3600))
    if slot < 0 or slot >= 900_000:
        raise ValueError('Cycle time outside supported range')
    return (1_000_000 if broad else 100_000) + slot


def day_before_cycle(cycle_at):
    if not isinstance(cycle_at, datetime) or cycle_at.tzinfo is None or cycle_at.utcoffset() is None:
        raise ValueError('Cycle time must include an offset')
    return cycle_at.astimezone(LOCAL_ZONE).date() - timedelta(days=1)


class SyncCoordinator:
    def __init__(self, worker: HistoryBackfill):
        self.worker = worker
        self.store = worker.store
        self.control = SourceControl(self.store)

    def _mission(self, mission_id):
        with self.store.transaction() as s:
            return dict(s.execute(text('SELECT * FROM control.source_missions WHERE id=:id'),
                                  {'id': mission_id}).mappings().one())

    def _watermark(self, mission_id, resource, modality):
        with self.store.transaction() as s:
            return s.execute(text("""SELECT contiguous_through FROM control.source_watermarks
                WHERE mission_id=:mission AND resource=:resource AND partition_key=:key"""),
                {'mission': mission_id, 'resource': resource,
                 'key': 'modality:' + str(modality)}).scalar_one_or_none()

    def plan_sync(self, mission_id, *, cycle_at, overlap_days=2, catchup_days=7,
                  resources=(RESOURCE, UPDATE_RESOURCE)):
        """Plan a bounded six-hour cycle; never skip earlier unconfirmed days."""
        if (type(overlap_days) is not int or not 1 <= overlap_days <= 30
                or type(catchup_days) is not int or not 1 <= catchup_days <= 31
                or not resources or any(resource not in LIST_RESOURCES for resource in resources)):
            raise ValueError('Invalid bounded sync plan')
        mission = self._mission(mission_id)
        if mission['state'] != 'enabled':
            return {'state': 'blocked', 'partitions': [], 'gaps': []}
        last_closed = day_before_cycle(cycle_at)
        pass_no = cycle_number(cycle_at)
        planned, gaps = [], []
        for resource in resources:
            try:
                self.worker._live_gate(mission, resource)
            except ValueError:
                gaps.append({'resource': resource, 'reason': 'resource_gate_blocked'})
                continue
            for modality in mission['scope']['modalities']:
                watermark = self._watermark(mission_id, resource, modality) or mission['fixed_end']
                start = max(mission['fixed_end'] + timedelta(days=1),
                            watermark - timedelta(days=overlap_days - 1))
                end = min(last_closed, start + timedelta(days=catchup_days - 1))
                current = start
                while current <= end:
                    planned.append(self.control.register_partition(mission_id,
                        resource=resource, modality_code=modality,
                        window_start=current, window_end=current, pass_no=pass_no,
                        priority=20))
                    current += timedelta(days=1)
        return {'state': 'planned', 'partitions': planned, 'gaps': gaps,
                'cycle_pass': pass_no, 'last_closed': last_closed}

    def plan_wide_reconcile(self, mission_id, *, cycle_at, lookback_days=30,
                            resources=(RESOURCE, UPDATE_RESOURCE)):
        """A separate bounded pass; no source snapshot is inferred."""
        if type(lookback_days) is not int or not 1 <= lookback_days <= 90:
            raise ValueError('Invalid reconciliation lookback')
        mission = self._mission(mission_id)
        if mission['state'] != 'enabled':
            return {'state': 'blocked', 'partitions': [], 'gaps': []}
        end = day_before_cycle(cycle_at)
        start = end - timedelta(days=lookback_days - 1)
        pass_no = cycle_number(cycle_at, broad=True)
        planned, gaps = [], []
        for resource in resources:
            try:
                self.worker._live_gate(mission, resource)
            except ValueError:
                gaps.append({'resource': resource, 'reason': 'resource_gate_blocked'})
                continue
            for modality in mission['scope']['modalities']:
                current = start
                while current <= end:
                    planned.append(self.control.register_partition(mission_id,
                        resource=resource, modality_code=modality,
                        window_start=current, window_end=current,
                        pass_no=pass_no, priority=60))
                    current += timedelta(days=1)
        return {'state': 'planned', 'partitions': planned, 'gaps': gaps,
                'cycle_pass': pass_no, 'start': start, 'end': end}

    def advance_watermark(self, mission_id, *, resource, modality):
        """Advance only across consecutive complete dates, stopping at any gap."""
        if resource not in LIST_RESOURCES or type(modality) is not int:
            raise ValueError('Unknown watermark key')
        mission = self._mission(mission_id)
        key = 'modality:' + str(modality)
        with self.store.transaction() as s:
            s.execute(text("""INSERT INTO control.source_watermarks
                (mission_id,resource,partition_key,contiguous_through)
                VALUES (:mission,:resource,:key,:floor)
                ON CONFLICT(mission_id,resource,partition_key) DO NOTHING"""),
                {'mission': mission_id, 'resource': resource, 'key': key,
                 'floor': mission['fixed_end']})
            row = s.execute(text("""SELECT contiguous_through FROM control.source_watermarks
                WHERE mission_id=:mission AND resource=:resource AND partition_key=:key
                FOR UPDATE"""),
                {'mission': mission_id, 'resource': resource, 'key': key}).one()
            cursor = row.contiguous_through or mission['fixed_end']
            while True:
                following = cursor + timedelta(days=1)
                states = s.execute(text("""SELECT state FROM control.source_partitions
                    WHERE mission_id=:mission AND resource=:resource AND modality_code=:modality
                    AND window_start=:day AND window_end=:day
                    AND pass_no>=100000 AND pass_no<1000000"""),
                    {'mission': mission_id, 'resource': resource, 'modality': modality,
                     'day': following}).scalars().all()
                if not states or any(state != 'complete' for state in states):
                    break
                cursor = following
            s.execute(text("""UPDATE control.source_watermarks
                SET contiguous_through=:through,updated_at=clock_timestamp()
                WHERE mission_id=:mission AND resource=:resource AND partition_key=:key"""),
                {'through': cursor, 'mission': mission_id, 'resource': resource, 'key': key})
            return cursor

    def run_sync(self, mission_id, *, max_pages=4):
        if type(max_pages) is not int or not 1 <= max_pages <= 100:
            raise ValueError('Sync batch must be bounded')
        results = []
        for _ in range(max_pages):
            result = self.worker.step(mission_id, priority_ceiling=39)
            results.append(result)
            if result['state'] == 'complete':
                with self.store.transaction() as s:
                    part = s.execute(text('SELECT resource,modality_code FROM control.source_partitions WHERE id=:id'),
                                     {'id': result['partition_id']}).one()
                self.advance_watermark(mission_id, resource=part.resource,
                                       modality=part.modality_code)
            if result['state'] not in ('page_committed', 'complete'):
                break
        return results

    def reconcile_parent(self, parent_id):
        """Require two matching complete child passes and retained parent IDs.

        Without an official snapshot this produces only reconciled_sem_snapshot.
        """
        with self.store.transaction() as s:
            parent = dict(s.execute(text("""SELECT * FROM control.source_partitions
                WHERE id=:id AND state='reconcile' FOR UPDATE"""),
                {'id': parent_id}).mappings().one())
            children = [dict(r) for r in s.execute(text("""SELECT * FROM control.source_partitions
                WHERE parent_id=:id ORDER BY pass_no,window_start"""),
                {'id': parent_id}).mappings()]
            first_pass = [r for r in children if r['pass_no'] == parent['pass_no'] + 1]
            second_pass = [r for r in children if r['pass_no'] == parent['pass_no'] + 2]
            if len(first_pass) < 2 or any(r['state'] != 'complete' for r in first_pass):
                return {'state': 'waiting_first_pass'}
            spans = [(r['window_start'], r['window_end']) for r in first_pass]
            if (spans[0][0] != parent['window_start']
                    or spans[-1][1] != parent['window_end']
                    or any(left[1] + timedelta(days=1) != right[0]
                           for left, right in zip(spans, spans[1:]))):
                raise ValueError('Child windows do not exactly cover parent')
            if not second_pass:
                if parent['replays_started'] >= parent['max_replays']:
                    return {'state': 'replay_limit'}
                s.execute(text("""UPDATE control.source_partitions
                    SET replays_started=replays_started+1 WHERE id=:id"""),
                    {'id': parent_id})
                for child in first_pass:
                    scope = {'resource': child['resource'], 'modality': child['modality_code'],
                             'start': child['window_start'].isoformat(),
                             'end': child['window_end'].isoformat(),
                             'pass': child['pass_no'] + 1, 'parent': str(parent_id)}
                    from .source_control import _hash
                    s.execute(text("""INSERT INTO control.source_partitions
                        (mission_id,parent_id,scope_key,resource,modality_code,
                         window_start,window_end,pass_no,priority,max_replays)
                        VALUES (:mission,:parent,:key,:resource,:modality,:start,:end,
                         :pass,:priority,:max) ON CONFLICT(mission_id,scope_key) DO NOTHING"""),
                        {'mission': parent['mission_id'], 'parent': parent_id,
                         'key': _hash(scope), 'resource': child['resource'],
                         'modality': child['modality_code'],
                         'start': child['window_start'], 'end': child['window_end'],
                         'pass': child['pass_no'] + 1, 'priority': child['priority'],
                         'max': child['max_replays']})
                return {'state': 'second_pass_planned'}
            if (len(second_pass) != len(first_pass)
                    or any(r['state'] != 'complete' for r in second_pass)):
                return {'state': 'waiting_second_pass'}
            for first, second in zip(first_pass, second_pass):
                if (first['window_start'], first['window_end']) != (second['window_start'], second['window_end']):
                    raise ValueError('Reconciliation pass windows differ')
                mismatch = s.execute(text("""SELECT EXISTS (
                    (SELECT source_id FROM control.source_partition_identities WHERE partition_id=:first
                     EXCEPT SELECT source_id FROM control.source_partition_identities WHERE partition_id=:second)
                    UNION ALL
                    (SELECT source_id FROM control.source_partition_identities WHERE partition_id=:second
                     EXCEPT SELECT source_id FROM control.source_partition_identities WHERE partition_id=:first))"""),
                    {'first': first['id'], 'second': second['id']}).scalar_one()
                if mismatch:
                    return {'state': 'identity_mismatch'}
            missing = s.execute(text("""SELECT count(*) FROM control.source_partition_identities p
                WHERE p.partition_id=:parent AND NOT EXISTS (
                    SELECT 1 FROM control.source_partition_identities c
                    WHERE c.partition_id=ANY(:children) AND c.source_id=p.source_id)"""),
                {'parent': parent_id, 'children': [r['id'] for r in second_pass]}).scalar_one()
            if missing:
                return {'state': 'parent_identity_missing', 'count': missing}
            s.execute(text("""UPDATE control.source_partitions SET state='complete'
                WHERE id=:id AND state='reconcile'"""), {'id': parent_id})
            job_id = job_uuid(parent_id)
            job = s.execute(text('SELECT checkpoint FROM control.ingestion_jobs WHERE id=:id FOR UPDATE'),
                            {'id': job_id}).mappings().one()
            from .source_control import _json
            checkpoint = dict(job['checkpoint'])
            checkpoint['coverage_status'] = 'reconciled_sem_snapshot'
            checkpoint['reconciliation'] = {'method': 'two_matching_child_passes',
                'first_pass': [str(r['id']) for r in first_pass],
                'second_pass': [str(r['id']) for r in second_pass],
                'absolute_completeness': False}
            s.execute(text("""UPDATE control.ingestion_jobs SET state='success',
                error_code=NULL,checkpoint=CAST(:checkpoint AS jsonb) WHERE id=:id"""),
                {'id': job_id, 'checkpoint': _json(checkpoint)})
            return {'state': 'reconciled_sem_snapshot'}

    @staticmethod
    def resource_gaps(policy):
        gates = policy.get('resource_gates', {})
        return [{'resource': resource, 'status': gates.get(resource, 'not_identified')}
                for resource in ('procurement_detail', 'items', 'results', 'documents')
                if gates.get(resource) != 'enabled']

    def replay_raw(self, mission_id, raw_id):
        """Reproject a committed Bronze page without network, quota or cursor move."""
        with self.store.transaction() as s:
            row = s.execute(text("""SELECT r.id,r.job_id,r.page,r.resource,r.http_status,
                r.body,r.body_sha256,r.received_at,j.scope_key,j.next_page,j.window_start,
                j.window_end,m.contract_hash,m.scope,p.id AS partition_id,p.modality_code
                FROM bronze.pncp_responses r
                JOIN control.ingestion_jobs j ON j.id=r.job_id
                JOIN control.source_partitions p ON p.scope_key=j.scope_key
                JOIN control.source_missions m ON m.id=p.mission_id
                WHERE r.id=:raw AND m.id=:mission"""),
                {'raw': raw_id, 'mission': mission_id}).mappings().one()
            if (row['resource'] not in LIST_RESOURCES or row['http_status'] != 200
                    or row['page'] >= row['next_page']
                    or hashlib.sha256(bytes(row['body'])).hexdigest() != row['body_sha256']):
                raise ValueError('Replay requires intact committed Bronze')
            envelope = json.loads(bytes(row['body']), parse_float=Decimal,
                                  parse_constant=reject_constant)
            records = envelope.get('data') if isinstance(envelope, dict) else None
            if not isinstance(records, list) or envelope.get('numeroPagina') != row['page']:
                raise ValueError('Replay page envelope invalid')
            if any(not isinstance(record, dict) for record in records):
                raise ValueError('Replay record invalid')
            source_ids = [record.get('numeroControlePNCP') for record in records]
            if (any(not isinstance(value, str) or not value.strip() for value in source_ids)
                    or len(set(source_ids)) != len(source_ids)):
                raise ValueError('Replay identity invalid')
            linked = s.execute(text("""SELECT source_id FROM control.source_partition_identities
                WHERE partition_id=:partition AND raw_response_id=:raw"""),
                {'partition': row['partition_id'], 'raw': raw_id}).scalars().all()
            if sorted(linked) != sorted(source_ids):
                raise ValueError('Unpromoted or altered Bronze cannot be replayed as committed')
            scope = Scope(row['window_start'], row['window_end'], row['modality_code'],
                          row['contract_hash'].strip(), row['scope']['page_size'])
            raw = {'id': row['id'], 'received_at': row['received_at']}
            repo = ProcurementRepository(s)
            for record in records:
                repo.record(snapshot_from_record(record, raw, scope, row['resource']),
                            actor='pncp-history-replay', correlation_id=row['job_id'])
            return {'state': 'replayed', 'records': len(records), 'network_calls': 0,
                    'quota_attempts': 0, 'cursor_advanced': False}
