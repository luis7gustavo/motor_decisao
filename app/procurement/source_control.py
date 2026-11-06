"""Durable source control on the existing isolated procurement database.

Reservations commit before the caller touches the network. This module does
not send requests, renew budgets, or enable a mission on its own.
"""

from datetime import datetime, time, timedelta, timezone
import hashlib
import json
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import text


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _daily_window(now, timezone_name):
    if not isinstance(timezone_name, str) or not timezone_name:
        raise ValueError('Daily budget timezone required')
    zone = ZoneInfo(timezone_name)
    local_day = now.astimezone(zone).date()
    start = datetime.combine(local_day, time.min, zone).astimezone(timezone.utc)
    end = datetime.combine(local_day + timedelta(days=1), time.min, zone).astimezone(timezone.utc)
    return start, end


class SourceControl:
    def __init__(self, isolated_store):
        self.store=isolated_store

    def register_budget(self,*,scope_key,source,restriction_scope,attempt_limit=0,
                        min_interval_ms=1000,max_concurrency=1,enabled=False,
                        adaptive_interval=False):
        if (not scope_key or not source or not restriction_scope or type(attempt_limit) is not int
                or attempt_limit<0 or type(min_interval_ms) is not int or min_interval_ms<0
                or type(max_concurrency) is not int or max_concurrency<1 or type(enabled) is not bool):
            raise ValueError('Invalid source budget')
        with self.store.transaction() as s:
            s.execute(text('''INSERT INTO control.source_budgets
                (scope_key,source,restriction_scope,attempt_limit,min_interval_ms,max_concurrency,enabled)
                VALUES (:key,:source,:restriction,:limit,:interval,:concurrency,:enabled)
                ON CONFLICT (scope_key) DO NOTHING'''),
                {'key':scope_key,'source':source,'restriction':restriction_scope,
                 'limit':attempt_limit,'interval':min_interval_ms,
                 'concurrency':max_concurrency,'enabled':enabled})
            row=s.execute(text('SELECT * FROM control.source_budgets WHERE scope_key=:key'),
                          {'key':scope_key}).mappings().one()
            expected=(source,restriction_scope,attempt_limit,max_concurrency,enabled)
            actual=tuple(row[key] for key in ('source','restriction_scope','attempt_limit',
                'max_concurrency','enabled'))
            interval_valid = (1000 <= row['min_interval_ms'] <= 60000
                              if adaptive_interval and min_interval_ms == 1000
                              else row['min_interval_ms'] == min_interval_ms)
            if actual!=expected or not interval_valid:
                raise ValueError('Existing source budget cannot be reset or widened')
            return dict(row)

    def amend_paused_budget(self, *, scope_key, source, restriction_scope,
                            expected_limit, expected_consumed, new_limit):
        """Increase a paused source cap without changing consumed attempts or timing."""
        if (not scope_key or not source or not restriction_scope
                or type(expected_limit) is not int or expected_limit < 0
                or type(expected_consumed) is not int or expected_consumed < 0
                or type(new_limit) is not int or new_limit <= expected_limit):
            raise ValueError('Invalid source budget amendment')
        with self.store.transaction() as s:
            row = s.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': scope_key}).mappings().one()
            if (row['source'] != source or row['restriction_scope'] != restriction_scope
                    or row['enabled'] or row['attempt_limit'] != expected_limit
                    or row['attempts_consumed'] != expected_consumed):
                raise ValueError('Paused source budget baseline differs from amendment')
            active_missions = s.execute(text('''SELECT count(*) FROM control.source_missions
                WHERE budget_scope_key=:key AND state='enabled' '''),
                {'key': scope_key}).scalar_one()
            if active_missions:
                raise ValueError('Source mission must be paused before budget amendment')
            updated = s.execute(text('''UPDATE control.source_budgets
                SET attempt_limit=:limit WHERE scope_key=:key RETURNING *'''),
                {'key': scope_key, 'limit': new_limit}).mappings().one()
            return dict(updated)

    def amend_paused_history_limits(self, *, scope_key, mission_key, source,
                                    restriction_scope, expected_source_limit,
                                    expected_source_consumed, new_source_limit,
                                    expected_mission_limit, expected_mission_consumed,
                                    new_mission_limit, daily_attempt_limit,
                                    daily_timezone):
        """Atomically extend finite caps and add a local-day cap while paused."""
        if (not scope_key or not mission_key or not source or not restriction_scope
                or any(type(value) is not int for value in (
                    expected_source_limit, expected_source_consumed, new_source_limit,
                    expected_mission_limit, expected_mission_consumed,
                    new_mission_limit, daily_attempt_limit))
                or expected_source_consumed < 0 or expected_mission_consumed < 0
                or new_source_limit <= expected_source_limit
                or new_mission_limit <= expected_mission_limit
                or daily_attempt_limit < 1):
            raise ValueError('Invalid history budget amendment')
        _daily_window(datetime.now(timezone.utc), daily_timezone)
        with self.store.transaction() as s:
            budget = s.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': scope_key}).mappings().one()
            mission = s.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': mission_key}).mappings().one()
            consumed = s.execute(text('''SELECT coalesce(sum(attempts_consumed),0)
                FROM control.source_missions WHERE budget_scope_key=:key'''),
                {'key': scope_key}).scalar_one()
            unmatched = s.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL'''),
                {'key': scope_key}).scalar_one()
            active = s.execute(text('''SELECT count(*) FROM control.source_missions
                WHERE budget_scope_key=:key AND state='enabled' '''),
                {'key': scope_key}).scalar_one()
            if (budget['source'] != source or budget['restriction_scope'] != restriction_scope
                    or budget['enabled'] or budget['attempt_limit'] != expected_source_limit
                    or budget['attempts_consumed'] != expected_source_consumed
                    or mission['source'] != source or mission['budget_scope_key'] != scope_key
                    or mission['state'] != 'paused'
                    or mission['attempt_limit'] != expected_mission_limit
                    or mission['attempts_consumed'] != expected_mission_consumed
                    or consumed != expected_source_consumed or unmatched or active
                    or new_source_limit < new_mission_limit +
                    (expected_source_consumed - expected_mission_consumed)):
                raise ValueError('Paused history ledger differs from reviewed baseline')
            scope = dict(mission['scope'])
            if 'daily_attempt_limit' in scope or 'daily_timezone' in scope:
                raise ValueError('Daily history limit already configured')
            scope.update(daily_attempt_limit=daily_attempt_limit,
                         daily_timezone=daily_timezone,
                         backfill_reserve_attempts=max(1, new_mission_limit // 5))
            s.execute(text('''UPDATE control.source_budgets SET attempt_limit=:limit
                WHERE scope_key=:key'''), {'limit': new_source_limit, 'key': scope_key})
            s.execute(text('''UPDATE control.source_missions
                SET attempt_limit=:limit, scope=CAST(:scope AS jsonb)
                WHERE id=:id'''), {'limit': new_mission_limit, 'scope': _json(scope),
                                  'id': mission['id']})
            return {'source_limit': new_source_limit, 'source_consumed': expected_source_consumed,
                    'mission_limit': new_mission_limit,
                    'mission_consumed': expected_mission_consumed,
                    'daily_attempt_limit': daily_attempt_limit,
                    'daily_timezone': daily_timezone}

    def amend_paused_history_test(self, *, scope_key, mission_key, source,
                                  restriction_scope, expected_source_limit,
                                  expected_source_consumed, new_source_limit,
                                  expected_mission_limit, expected_mission_consumed,
                                  new_mission_limit, expected_daily_limit,
                                  new_daily_limit, daily_timezone,
                                  test_start_utc, test_end_utc):
        """Change a paused, fully reconciled mission to one finite 24-hour test."""
        values = (expected_source_limit, expected_source_consumed, new_source_limit,
                  expected_mission_limit, expected_mission_consumed, new_mission_limit,
                  expected_daily_limit, new_daily_limit)
        if (not scope_key or not mission_key or not source or not restriction_scope
                or any(type(value) is not int for value in values)
                or any(value < 0 for value in values)
                or new_source_limit <= expected_source_limit
                or new_mission_limit <= expected_mission_limit
                or new_daily_limit <= expected_daily_limit
                or new_source_limit != new_mission_limit +
                    (expected_source_consumed - expected_mission_consumed)
                or new_daily_limit != 90000):
            raise ValueError('Invalid finite history test limits')
        start = datetime.fromisoformat(test_start_utc.replace('Z', '+00:00'))
        end = datetime.fromisoformat(test_end_utc.replace('Z', '+00:00'))
        if (start.tzinfo is None or end.tzinfo is None
                or start.utcoffset() != timedelta(0)
                or end.utcoffset() != timedelta(0)
                or end - start != timedelta(hours=24)):
            raise ValueError('History test must be exactly 24 UTC hours')
        _daily_window(start, daily_timezone)
        with self.store.transaction() as s:
            budget = s.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': scope_key}).mappings().one()
            mission = s.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': mission_key}).mappings().one()
            consumed = s.execute(text('''SELECT coalesce(sum(attempts_consumed),0)
                FROM control.source_missions WHERE budget_scope_key=:key'''),
                {'key': scope_key}).scalar_one()
            unresolved = s.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL'''),
                {'key': scope_key}).scalar_one()
            active = s.execute(text('''SELECT count(*) FROM control.source_missions
                WHERE budget_scope_key=:key AND state='enabled' '''),
                {'key': scope_key}).scalar_one()
            if (budget['source'] != source or budget['restriction_scope'] != restriction_scope
                    or budget['enabled'] or budget['attempt_limit'] != expected_source_limit
                    or budget['attempts_consumed'] != expected_source_consumed
                    or budget['max_concurrency'] != 1 or budget['min_interval_ms'] != 1000
                    or mission['source'] != source or mission['budget_scope_key'] != scope_key
                    or mission['state'] != 'paused'
                    or mission['attempt_limit'] != expected_mission_limit
                    or mission['attempts_consumed'] != expected_mission_consumed
                    or mission['scope'].get('daily_attempt_limit') != expected_daily_limit
                    or mission['scope'].get('daily_timezone') != daily_timezone
                    or consumed != expected_source_consumed or unresolved or active):
                raise ValueError('Paused history test ledger differs reviewed baseline')
            scope = dict(mission['scope'])
            if 'test_window_start_utc' in scope or 'test_window_end_utc' in scope:
                raise ValueError('History test window already configured')
            scope.update(daily_attempt_limit=new_daily_limit,
                         test_window_start_utc=test_start_utc,
                         test_window_end_utc=test_end_utc,
                         backfill_reserve_attempts=max(1, new_mission_limit // 5))
            s.execute(text('''UPDATE control.source_budgets SET attempt_limit=:limit
                WHERE scope_key=:key'''), {'limit': new_source_limit, 'key': scope_key})
            s.execute(text('''UPDATE control.source_missions
                SET attempt_limit=:limit,scope=CAST(:scope AS jsonb) WHERE id=:id'''),
                {'limit': new_mission_limit, 'scope': _json(scope), 'id': mission['id']})
            return {'source_limit': new_source_limit,
                    'source_consumed': expected_source_consumed,
                    'mission_limit': new_mission_limit,
                    'mission_consumed': expected_mission_consumed,
                    'daily_attempt_limit': new_daily_limit,
                    'test_window_start_utc': test_start_utc,
                    'test_window_end_utc': test_end_utc}

    def rewindow_paused_history_test(self, *, scope_key, mission_key,
                                     expected_source_limit, expected_mission_limit,
                                     expected_source_consumed, expected_mission_consumed,
                                     old_start_utc, old_end_utc,
                                     new_start_utc, new_end_utc):
        """Restart a disrupted finite test without refunding any attempt."""
        old_start = datetime.fromisoformat(old_start_utc.replace('Z', '+00:00'))
        old_end = datetime.fromisoformat(old_end_utc.replace('Z', '+00:00'))
        new_start = datetime.fromisoformat(new_start_utc.replace('Z', '+00:00'))
        new_end = datetime.fromisoformat(new_end_utc.replace('Z', '+00:00'))
        if (any(value.tzinfo is None or value.utcoffset() != timedelta(0)
                for value in (old_start, old_end, new_start, new_end))
                or old_end - old_start != timedelta(hours=24)
                or new_end - new_start != timedelta(hours=24)
                or new_start <= old_start
                or new_start < datetime.now(timezone.utc)
                or any(type(value) is not int or value < 0 for value in
                       (expected_source_limit, expected_mission_limit,
                        expected_source_consumed, expected_mission_consumed))
                or expected_source_limit < expected_mission_limit):
            raise ValueError('Invalid finite test rewindow')
        with self.store.transaction() as s:
            budget = s.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': scope_key}).mappings().one()
            mission = s.execute(text('''SELECT * FROM control.source_missions
                WHERE mission_key=:key FOR UPDATE'''), {'key': mission_key}).mappings().one()
            unresolved = s.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL'''),
                {'key': scope_key}).scalar_one()
            active = s.execute(text('''SELECT count(*) FROM control.source_missions
                WHERE budget_scope_key=:key AND state='enabled' '''),
                {'key': scope_key}).scalar_one()
            if (budget['enabled'] or budget['attempt_limit'] != expected_source_limit
                    or budget['attempts_consumed'] != expected_source_consumed
                    or budget['max_concurrency'] != 1
                    or not 1000 <= budget['min_interval_ms'] <= 60000
                    or mission['budget_scope_key'] != scope_key
                    or mission['state'] != 'paused'
                    or mission['attempt_limit'] != expected_mission_limit
                    or mission['attempts_consumed'] != expected_mission_consumed
                    or expected_source_consumed - expected_mission_consumed
                    != expected_source_limit - expected_mission_limit
                    or mission['scope'].get('daily_attempt_limit') != 90000
                    or mission['scope'].get('test_window_start_utc') != old_start_utc
                    or mission['scope'].get('test_window_end_utc') != old_end_utc
                    or unresolved or active):
                raise ValueError('Paused finite test differs from reviewed baseline')
            scope = dict(mission['scope'])
            scope.update(test_window_start_utc=new_start_utc,
                         test_window_end_utc=new_end_utc)
            s.execute(text('''UPDATE control.source_missions
                SET scope=CAST(:scope AS jsonb) WHERE id=:id'''),
                {'scope': _json(scope), 'id': mission['id']})
            return {'mission_consumed': expected_mission_consumed,
                    'source_consumed': expected_source_consumed,
                    'test_window_start_utc': new_start_utc,
                    'test_window_end_utc': new_end_utc}

    def register_mission(self,*,mission_key,source,budget_scope_key,contract_hash,
                         fixed_end,scope,coverage_start=None,attempt_limit=0,state='blocked'):
        if (not mission_key or not source or not budget_scope_key or len(contract_hash)!=64
                or type(scope) is not dict or type(attempt_limit) is not int or attempt_limit<0
                or state not in ('blocked','enabled','paused')):
            raise ValueError('Invalid mission manifest')
        values={'key':mission_key,'source':source,'budget':budget_scope_key,
                'contract':contract_hash,'start':coverage_start,'end':fixed_end,
                'scope':_json(scope),'limit':attempt_limit,'state':state}
        with self.store.transaction() as s:
            budget=s.execute(text('SELECT source FROM control.source_budgets WHERE scope_key=:key'),
                             {'key':budget_scope_key}).scalar_one()
            if budget!=source:
                raise ValueError('Mission source differs from shared budget')
            s.execute(text('''INSERT INTO control.source_missions
                (mission_key,source,budget_scope_key,contract_hash,coverage_start,fixed_end,
                 scope,attempt_limit,state)
                VALUES (:key,:source,:budget,:contract,:start,:end,CAST(:scope AS jsonb),:limit,:state)
                ON CONFLICT (mission_key) DO NOTHING'''),values)
            row=s.execute(text('SELECT * FROM control.source_missions WHERE mission_key=:key'),
                          {'key':mission_key}).mappings().one()
            if (row['source']!=source or row['budget_scope_key']!=budget_scope_key
                    or row['contract_hash']!=contract_hash or row['coverage_start']!=coverage_start
                    or row['fixed_end']!=fixed_end or row['scope']!=scope
                    or row['attempt_limit']!=attempt_limit or row['state']!=state):
                raise ValueError('Existing mission manifest cannot be silently changed')
            return dict(row)

    def register_partition(self,mission_id,*,resource,modality_code,window_start,window_end,
                           pass_no=1,parent_id=None,priority=100,max_replays=2):
        scope={'resource':resource,'modality':modality_code,'start':window_start.isoformat(),
               'end':window_end.isoformat(),'pass':pass_no,'parent':str(parent_id) if parent_id else None}
        key=_hash(scope)
        with self.store.transaction() as s:
            if parent_id is not None:
                parent=s.execute(text('SELECT mission_id FROM control.source_partitions WHERE id=:id'),
                                 {'id':parent_id}).scalar_one()
                if parent!=mission_id:
                    raise ValueError('Child partition must share mission')
            s.execute(text('''INSERT INTO control.source_partitions
                (mission_id,parent_id,scope_key,resource,modality_code,window_start,window_end,
                 pass_no,priority,max_replays)
                VALUES (:mission,:parent,:key,:resource,:modality,:start,:end,:pass,:priority,:max)
                ON CONFLICT (mission_id,scope_key) DO NOTHING'''),
                {'mission':mission_id,'parent':parent_id,'key':key,'resource':resource,
                 'modality':modality_code,'start':window_start,'end':window_end,
                 'pass':pass_no,'priority':priority,'max':max_replays})
            row=s.execute(text('''SELECT * FROM control.source_partitions
                WHERE mission_id=:mission AND scope_key=:key'''),
                {'mission':mission_id,'key':key}).mappings().one()
            if (row['parent_id']!=parent_id or row['priority']!=priority
                    or row['max_replays']!=max_replays):
                raise ValueError('Existing partition cannot be silently changed')
            return dict(row)

    def reserve_attempt(self,mission_id,*,work_kind,request_hash,lease_seconds=120,job_id=None):
        if (not work_kind or len(request_hash)!=64 or type(lease_seconds) is not int
                or lease_seconds<1 or lease_seconds>3600):
            raise ValueError('Invalid attempt request')
        with self.store.transaction() as s:
            mission_ref=s.execute(text('SELECT budget_scope_key FROM control.source_missions WHERE id=:id'),
                                  {'id':mission_id}).scalar_one()
            budget=s.execute(text('''SELECT * FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''),{'key':mission_ref}).mappings().one()
            mission=s.execute(text('''SELECT * FROM control.source_missions
                WHERE id=:id FOR UPDATE'''),{'id':mission_id}).mappings().one()
            now=s.execute(text('SELECT clock_timestamp()')).scalar_one()
            if not budget['enabled'] or mission['state']!='enabled':
                return {'state':'blocked'}
            if budget['attempts_consumed']>=budget['attempt_limit'] or mission['attempts_consumed']>=mission['attempt_limit']:
                return {'state':'quota_exhausted'}
            test_start = mission['scope'].get('test_window_start_utc')
            test_end = mission['scope'].get('test_window_end_utc')
            if test_start or test_end:
                if not test_start or not test_end:
                    raise ValueError('Incomplete finite test window')
                start = datetime.fromisoformat(test_start.replace('Z', '+00:00'))
                end = datetime.fromisoformat(test_end.replace('Z', '+00:00'))
                if (start.tzinfo is None or end.tzinfo is None
                        or start.utcoffset() != timedelta(0)
                        or end.utcoffset() != timedelta(0)
                        or end - start != timedelta(hours=24)):
                    raise ValueError('Invalid finite test window')
                if now < start:
                    return {'state':'deferred','not_before':start,'reason':'test_not_started'}
                if now >= end:
                    return {'state':'test_window_complete'}
            daily_limit = mission['scope'].get('daily_attempt_limit')
            if daily_limit is not None:
                if type(daily_limit) is not int or daily_limit < 1:
                    raise ValueError('Invalid daily attempt limit')
                day_start, day_end = _daily_window(now, mission['scope'].get('daily_timezone'))
                used_today = s.execute(text('''SELECT count(*) FROM control.source_attempts
                    WHERE budget_scope_key=:key AND reserved_at>=:start AND reserved_at<:end'''),
                    {'key': mission_ref, 'start': day_start, 'end': day_end}).scalar_one()
                if used_today >= daily_limit:
                    return {'state':'deferred', 'not_before':day_end,
                            'reason':'daily_attempt_limit'}
            reserved_for_backfill=mission['scope'].get('backfill_reserve_attempts',0)
            if (work_kind not in ('backfill','reconcile') and
                    mission['attempt_limit']-mission['attempts_consumed']<=reserved_for_backfill):
                return {'state':'quota_reserved_for_backfill'}
            wait_until=max(budget['not_before'],budget['next_allowed_at'])
            if now<wait_until:
                return {'state':'deferred','not_before':wait_until}
            active=s.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND a.lease_until>:now AND o.attempt_id IS NULL'''),
                {'key':mission_ref,'now':now}).scalar_one()
            if active>=budget['max_concurrency']:
                until=s.execute(text('''SELECT min(a.lease_until) FROM control.source_attempts a
                    LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                    WHERE a.budget_scope_key=:key AND a.lease_until>:now AND o.attempt_id IS NULL'''),
                    {'key':mission_ref,'now':now}).scalar_one()
                return {'state':'deferred','not_before':until}
            attempt_id=uuid4()
            sequence=budget['attempts_consumed']+1
            lease_until=now+timedelta(seconds=lease_seconds)
            next_allowed=now+timedelta(milliseconds=budget['min_interval_ms'])
            s.execute(text('''UPDATE control.source_budgets SET attempts_consumed=attempts_consumed+1,
                next_allowed_at=:next WHERE scope_key=:key'''),{'next':next_allowed,'key':mission_ref})
            s.execute(text('''UPDATE control.source_missions SET attempts_consumed=attempts_consumed+1
                WHERE id=:id'''),{'id':mission_id})
            s.execute(text('''INSERT INTO control.source_attempts
                (id,budget_scope_key,mission_id,job_id,sequence_no,work_kind,request_hash,reserved_at,lease_until)
                VALUES (:id,:budget,:mission,:job,:sequence,:kind,:hash,:now,:lease)'''),
                {'id':attempt_id,'budget':mission_ref,'mission':mission_id,'job':job_id,
                 'sequence':sequence,'kind':work_kind,'hash':request_hash,
                 'now':now,'lease':lease_until})
            return {'state':'reserved','attempt_id':attempt_id,'sequence_no':sequence,
                    'reserved_at':now,'lease_until':lease_until}

    def record_outcome(self,attempt_id,*,outcome,http_status=None,bronze_response_id=None):
        if not outcome:
            raise ValueError('Attempt outcome required')
        with self.store.transaction() as s:
            attempt=s.execute(text('SELECT job_id FROM control.source_attempts WHERE id=:id'),
                              {'id':attempt_id}).mappings().one()
            if bronze_response_id is not None:
                raw_job=s.execute(text('SELECT job_id FROM bronze.pncp_responses WHERE id=:id'),
                                  {'id':bronze_response_id}).scalar_one()
                if attempt['job_id'] is None or raw_job!=attempt['job_id']:
                    raise ValueError('Bronze receipt belongs to another job')
            values={'id':attempt_id,'outcome':outcome,'status':http_status,'raw':bronze_response_id}
            s.execute(text('''INSERT INTO control.source_attempt_outcomes
                (attempt_id,outcome,http_status,bronze_response_id)
                VALUES (:id,:outcome,:status,:raw) ON CONFLICT (attempt_id) DO NOTHING'''),values)
            row=s.execute(text('''SELECT * FROM control.source_attempt_outcomes
                WHERE attempt_id=:id'''),{'id':attempt_id}).mappings().one()
            if (row['outcome'],row['http_status'],row['bronze_response_id'])!=(
                    outcome,http_status,bronze_response_id):
                raise ValueError('Attempt outcome is immutable')
            return dict(row)

    def adjust_adaptive_rate(self, scope_key, *, throttled=False):
        """Persist the shared interval: halve on throttle, recover gradually on success."""
        if not scope_key or type(throttled) is not bool:
            raise ValueError('Invalid adaptive rate input')
        with self.store.transaction() as s:
            row = s.execute(text('''SELECT min_interval_ms FROM control.source_budgets
                WHERE scope_key=:key FOR UPDATE'''), {'key': scope_key}).mappings().one()
            old = row['min_interval_ms']
            if not 1000 <= old <= 60000:
                raise ValueError('Adaptive interval outside reviewed bounds')
            new = min(60000, old * 2) if throttled else max(1000, int(old * 0.9))
            return s.execute(text('''UPDATE control.source_budgets
                SET min_interval_ms=:interval,
                    next_allowed_at=GREATEST(next_allowed_at,
                        clock_timestamp() + (:interval * interval '1 millisecond'))
                WHERE scope_key=:key RETURNING min_interval_ms'''),
                {'key': scope_key, 'interval': new}).scalar_one()

    def defer_source(self,scope_key,until):
        if (not isinstance(until,datetime) or until.tzinfo is None
                or until.utcoffset() is None):
            raise ValueError('Source deferral requires an offset-aware instant')
        with self.store.transaction() as s:
            return s.execute(text('''UPDATE control.source_budgets
                SET not_before=GREATEST(not_before,:until) WHERE scope_key=:key
                RETURNING not_before'''),{'key':scope_key,'until':until}).scalar_one()

    def enqueue(self,mission_id,*,work_kind,idempotency_key,partition_id=None,
                identity_key=None,payload=None,priority=100):
        if not work_kind or len(idempotency_key)!=64 or not isinstance(payload or {},dict):
            raise ValueError('Invalid queue item')
        with self.store.transaction() as s:
            if partition_id is not None:
                owner=s.execute(text('SELECT mission_id FROM control.source_partitions WHERE id=:id'),
                                {'id':partition_id}).scalar_one()
                if owner!=mission_id:
                    raise ValueError('Queue partition belongs to another mission')
            params={'mission':mission_id,'partition':partition_id,'kind':work_kind,
                    'key':idempotency_key,'identity':identity_key,'payload':_json(payload or {}),
                    'priority':priority}
            s.execute(text('''INSERT INTO control.source_queue
                (mission_id,partition_id,work_kind,idempotency_key,identity_key,payload,priority)
                VALUES (:mission,:partition,:kind,:key,:identity,CAST(:payload AS jsonb),:priority)
                ON CONFLICT (mission_id,work_kind,idempotency_key) DO NOTHING'''),params)
            row=s.execute(text('''SELECT * FROM control.source_queue
                WHERE mission_id=:mission AND work_kind=:kind AND idempotency_key=:key'''),
                params).mappings().one()
            if (row['partition_id']!=partition_id or row['identity_key']!=identity_key
                    or row['payload']!=(payload or {}) or row['priority']!=priority):
                raise ValueError('Existing queue item cannot be silently changed')
            return dict(row)

    def claim_partition(self,mission_id,owner,*,priority_floor=None,priority_ceiling=None,
                        resource=None):
        with self.store.transaction() as s:
            row=s.execute(text('''UPDATE control.source_partitions SET state='running',
                lease_owner=:owner,lease_until=clock_timestamp()+interval '5 minutes',
                fencing_token=fencing_token+1
                WHERE id=(SELECT id FROM control.source_partitions
            WHERE mission_id=:mission AND
            (CAST(:resource AS text) IS NULL OR resource=:resource) AND
                    (CAST(:floor AS integer) IS NULL OR priority>=CAST(:floor AS integer)) AND
                    (CAST(:ceiling AS integer) IS NULL OR priority<=CAST(:ceiling AS integer)) AND
                    (state='pending' OR (state='running' AND lease_until<clock_timestamp()))
            ORDER BY priority,(parent_id IS NULL),created_at,id
            LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING *'''),{'mission':mission_id,'owner':owner,'resource':resource,
                                'floor':priority_floor,'ceiling':priority_ceiling}).mappings().one_or_none()
            return dict(row) if row else None

    def claim_queue(self,mission_id,owner,*,work_prefix=None):
        with self.store.transaction() as s:
            row=s.execute(text('''UPDATE control.source_queue SET state='running',lease_owner=:owner,
                lease_until=clock_timestamp()+interval '5 minutes',
                fencing_token=fencing_token+1,attempts=attempts+1
                WHERE id=(SELECT id FROM control.source_queue
                  WHERE mission_id=:mission AND not_before<=clock_timestamp()
                  AND (CAST(:prefix AS text) IS NULL OR work_kind LIKE :pattern)
                  AND (state='pending' OR (state='running' AND lease_until<clock_timestamp()))
                  ORDER BY priority,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED)
                RETURNING *'''),{'mission':mission_id,'owner':owner,
                                'prefix':work_prefix,'pattern':(work_prefix or '')+'%'}).mappings().one_or_none()
            return dict(row) if row else None

    def finish_queue(self,queue_id,owner,token):
        with self.store.transaction() as s:
            row=s.execute(text('''UPDATE control.source_queue SET state='complete',
                lease_owner=NULL,lease_until=NULL
                WHERE id=:id AND lease_owner=:owner AND fencing_token=:token
                  AND state='running' AND lease_until>clock_timestamp()
                RETURNING id'''),{'id':queue_id,'owner':owner,'token':token}).first()
            if row is None:
                raise ValueError('Stale queue claim cannot acknowledge work')

    def release_queue(self,queue_id,owner,token,*,state,not_before=None):
        if state not in ('pending','blocked','failed'):
            raise ValueError('Invalid queue release state')
        if not_before is not None and (not isinstance(not_before,datetime)
                or not_before.tzinfo is None or not_before.utcoffset() is None):
            raise ValueError('Queue deferral needs offset-aware time')
        with self.store.transaction() as s:
            row=s.execute(text('''UPDATE control.source_queue SET state=:state,
                not_before=GREATEST(not_before,COALESCE(CAST(:until AS timestamptz),not_before)),
                lease_owner=NULL,lease_until=NULL
                WHERE id=:id AND lease_owner=:owner AND fencing_token=:token
                  AND state='running' AND lease_until>clock_timestamp()
                RETURNING id'''),{'id':queue_id,'owner':owner,'token':token,
                                  'state':state,'until':not_before}).first()
            if row is None:
                raise ValueError('Stale queue claim cannot release work')

    def advance_partition(self,partition_id,owner,token,*,next_page,state):
        if state not in ('running','partial','complete','reconcile') or next_page<1:
            raise ValueError('Invalid partition progress')
        with self.store.transaction() as s:
            row=s.execute(text('''UPDATE control.source_partitions SET next_page=:page,
                state=CAST(:state AS varchar(16)),
                lease_owner=CASE WHEN CAST(:state AS varchar(16))='running' THEN lease_owner ELSE NULL END,
                lease_until=CASE WHEN CAST(:state AS varchar(16))='running' THEN lease_until ELSE NULL END
                WHERE id=:id AND lease_owner=:owner AND fencing_token=:token
                  AND lease_until>clock_timestamp() AND next_page=:page
                RETURNING id'''),{'id':partition_id,'owner':owner,'token':token,
                                  'page':next_page,'state':state}).first()
            if row is None:
                raise ValueError('Stale claim or page advancement without atomic promotion')
