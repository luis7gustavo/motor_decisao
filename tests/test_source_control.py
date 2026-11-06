"""S02: atomic source budgets, bounded partitions and fenced queue claims."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from app.procurement.pncp import Store
from app.procurement.source_control import SourceControl, _daily_window


def test_store_refuses_principal_destination():
    engine=create_engine(os.environ['DATABASE_URL'])
    try:
        with pytest.raises(ValueError,match='isolated target'):
            Store(engine,expected_database='motor_decisao')
    finally:
        engine.dispose()


@pytest.fixture
def control():
    engine=create_engine(os.environ['DATABASE_URL'])
    store=Store(engine,expected_database=os.environ['RADAR_TEST_DB'])
    yield SourceControl(store),engine
    with engine.begin() as conn:
        conn.execute(text('''TRUNCATE control.source_attempt_outcomes,
            control.source_attempts,control.source_partition_identities,
            control.source_queue,control.source_watermarks,control.source_partitions,
            control.source_missions,control.source_budgets'''))
    engine.dispose()


def setup(control,*,source_limit=5,mission_limit=5,concurrency=1,interval=0,
          enabled=True,daily_limit=None):
    store,_=control
    key='pncp-'+uuid4().hex
    store.register_budget(scope_key=key,source='synthetic-pncp',restriction_scope='test-ip-'+key,
        attempt_limit=source_limit,min_interval_ms=interval,max_concurrency=concurrency,enabled=enabled)
    scope={'synthetic':True}
    if daily_limit is not None:
        scope.update(daily_attempt_limit=daily_limit,daily_timezone='America/Sao_Paulo')
    mission=store.register_mission(mission_key='history-'+key,source='synthetic-pncp',
        budget_scope_key=key,contract_hash='a'*64,fixed_end=date(2026,10,26),
        scope=scope,coverage_start=date(2026,1,1),
        attempt_limit=mission_limit,state='enabled' if enabled else 'blocked')
    return key,mission['id']


def test_blocked_zero_budget_never_reserves(control):
    store,engine=control
    key,mission=setup(control,source_limit=0,mission_limit=0,enabled=False)
    assert store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)['state']=='blocked'
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempts'))==0
        assert conn.scalar(text('SELECT attempts_consumed FROM control.source_budgets WHERE scope_key=:key'),
                           {'key':key})==0


def test_atomic_shared_budget_across_workers_and_no_restart_refund(control):
    store,engine=control
    key,mission=setup(control,source_limit=3,mission_limit=3,concurrency=3)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results=list(pool.map(lambda _:store.reserve_attempt(mission,work_kind='backfill',
                                    request_hash='b'*64),range(8)))
    reserved=[result for result in results if result['state']=='reserved']
    assert len(reserved)==3
    assert sorted(result['sequence_no'] for result in reserved)==[1,2,3]
    assert all(result['state']=='quota_exhausted' for result in results if result not in reserved)
    assert store.reserve_attempt(mission,work_kind='sync',request_hash='c'*64)['state']=='quota_exhausted'
    existing=store.register_budget(scope_key=key,source='synthetic-pncp',
        restriction_scope='test-ip-'+key,attempt_limit=3,min_interval_ms=0,
        max_concurrency=3,enabled=True)
    assert existing['attempts_consumed']==3
    with pytest.raises(ValueError,match='cannot be reset'):
        store.register_budget(scope_key=key,source='synthetic-pncp',
            restriction_scope='test-ip-'+key,attempt_limit=4,min_interval_ms=0,
            max_concurrency=3,enabled=True)
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempts'))==3


def test_paused_budget_amendment_preserves_consumption_and_timing(control):
    store,engine=control
    key,mission=setup(control,source_limit=3,mission_limit=3,interval=1000)
    first=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    assert first['state']=='reserved'
    with engine.begin() as conn:
        before=conn.execute(text('''SELECT attempts_consumed,not_before,next_allowed_at,
            min_interval_ms,max_concurrency FROM control.source_budgets
            WHERE scope_key=:key'''),{'key':key}).mappings().one()
        conn.execute(text('UPDATE control.source_budgets SET enabled=false WHERE scope_key=:key'),
                     {'key':key})
    with pytest.raises(ValueError,match='mission must be paused'):
        store.amend_paused_budget(scope_key=key,source='synthetic-pncp',
            restriction_scope='test-ip-'+key,expected_limit=3,
            expected_consumed=1,new_limit=5)
    with engine.begin() as conn:
        conn.execute(text("UPDATE control.source_missions SET state='paused' WHERE id=:id"),
                     {'id':mission})
    with pytest.raises(ValueError,match='baseline differs'):
        store.amend_paused_budget(scope_key=key,source='synthetic-pncp',
            restriction_scope='test-ip-'+key,expected_limit=3,
            expected_consumed=0,new_limit=5)
    with pytest.raises(ValueError,match='baseline differs'):
        store.amend_paused_budget(scope_key=key,source='synthetic-pncp',
            restriction_scope='wrong',expected_limit=3,
            expected_consumed=1,new_limit=5)
    updated=store.amend_paused_budget(scope_key=key,source='synthetic-pncp',
        restriction_scope='test-ip-'+key,expected_limit=3,
        expected_consumed=1,new_limit=5)
    assert updated['attempt_limit']==5
    assert all(updated[field]==before[field] for field in before.keys())
    assert updated['enabled'] is False
    with pytest.raises(ValueError,match='baseline differs'):
        store.amend_paused_budget(scope_key=key,source='synthetic-pncp',
            restriction_scope='test-ip-'+key,expected_limit=3,
            expected_consumed=1,new_limit=5)
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempts'))==1


def test_daily_cap_is_durable_and_defers_without_reserving(control):
    store,engine=control
    key,mission=setup(control,source_limit=10,mission_limit=10,
                      concurrency=3,daily_limit=2)
    first=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    second=store.reserve_attempt(mission,work_kind='backfill',request_hash='c'*64)
    assert first['state']==second['state']=='reserved'
    store.record_outcome(first['attempt_id'],outcome='transport_error')
    store.record_outcome(second['attempt_id'],outcome='transport_error')
    deferred=SourceControl(store.store).reserve_attempt(
        mission,work_kind='backfill',request_hash='d'*64)
    assert deferred['state']=='deferred'
    assert deferred['reason']=='daily_attempt_limit'
    assert deferred['not_before']>datetime.now(timezone.utc)
    with engine.connect() as conn:
        assert conn.scalar(text('''SELECT count(*) FROM control.source_attempts
            WHERE mission_id=:id'''),{'id':mission})==2
        assert conn.scalar(text('''SELECT attempts_consumed FROM control.source_budgets
            WHERE scope_key=:key'''),{'key':key})==2


def test_daily_cap_uses_sao_paulo_calendar_boundary():
    start,end=_daily_window(datetime(2026,11,4,2,30,tzinfo=timezone.utc),
                            'America/Sao_Paulo')
    assert start==datetime(2026,11,3,3,tzinfo=timezone.utc)
    assert end==datetime(2026,11,4,3,tzinfo=timezone.utc)


def test_finite_test_amendment_keeps_ledger_and_stops_after_deadline(control):
    store, engine = control
    key, mission = setup(control, source_limit=4, mission_limit=4,
                         interval=1000, daily_limit=200)
    first = store.reserve_attempt(mission, work_kind='backfill', request_hash='b'*64)
    assert first['state'] == 'reserved'
    store.record_outcome(first['attempt_id'], outcome='transport_error')
    with engine.begin() as conn:
        conn.execute(text('UPDATE control.source_budgets SET enabled=false WHERE scope_key=:key'),
                     {'key': key})
        conn.execute(text("UPDATE control.source_missions SET state='paused' WHERE id=:id"),
                     {'id': mission})
    start = datetime.now(timezone.utc) - timedelta(hours=25)
    end = start + timedelta(hours=24)
    args = dict(scope_key=key, mission_key='history-'+key, source='synthetic-pncp',
                restriction_scope='test-ip-'+key, expected_source_limit=4,
                expected_source_consumed=1, new_source_limit=100000,
                expected_mission_limit=4, expected_mission_consumed=1,
                new_mission_limit=100000, expected_daily_limit=200,
                new_daily_limit=90000, daily_timezone='America/Sao_Paulo',
                test_start_utc=start.isoformat(), test_end_utc=end.isoformat())
    amended = store.amend_paused_history_test(**args)
    assert amended['mission_consumed'] == amended['source_consumed'] == 1
    with pytest.raises(ValueError, match='baseline'):
        store.amend_paused_history_test(**args)
    with engine.begin() as conn:
        conn.execute(text('UPDATE control.source_budgets SET enabled=true WHERE scope_key=:key'),
                     {'key': key})
        conn.execute(text("UPDATE control.source_missions SET state='enabled' WHERE id=:id"),
                     {'id': mission})
    assert store.reserve_attempt(mission, work_kind='backfill', request_hash='c'*64)['state'] == 'test_window_complete'
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempts WHERE mission_id=:id'),
                           {'id': mission}) == 1
    with engine.begin() as conn:
        conn.execute(text('UPDATE control.source_budgets SET enabled=false WHERE scope_key=:key'),
                     {'key': key})
        conn.execute(text("UPDATE control.source_missions SET state='paused' WHERE id=:id"),
                     {'id': mission})
    next_start = datetime.now(timezone.utc) + timedelta(minutes=10)
    shifted = store.rewindow_paused_history_test(
        scope_key=key, mission_key='history-'+key,
        expected_source_limit=100000, expected_mission_limit=100000,
        expected_source_consumed=1, expected_mission_consumed=1,
        old_start_utc=start.isoformat(), old_end_utc=end.isoformat(),
        new_start_utc=next_start.isoformat(),
        new_end_utc=(next_start + timedelta(hours=24)).isoformat())
    assert shifted['mission_consumed'] == 1
    with pytest.raises(ValueError, match='baseline'):
        store.rewindow_paused_history_test(
            scope_key=key, mission_key='history-'+key,
            expected_source_limit=100000, expected_mission_limit=100000,
            expected_source_consumed=1, expected_mission_consumed=1,
            old_start_utc=start.isoformat(), old_end_utc=end.isoformat(),
            new_start_utc=next_start.isoformat(),
            new_end_utc=(next_start + timedelta(hours=24)).isoformat())
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempts WHERE mission_id=:id'),
                           {'id': mission}) == 1


def test_adaptive_interval_halves_rate_and_recovers_gradually(control):
    store, engine = control
    key, _ = setup(control, source_limit=10, mission_limit=10, interval=1000)
    assert store.adjust_adaptive_rate(key, throttled=True) == 2000
    assert store.adjust_adaptive_rate(key, throttled=True) == 4000
    assert store.adjust_adaptive_rate(key, throttled=False) == 3600
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT min_interval_ms FROM control.source_budgets WHERE scope_key=:key'),
                           {'key': key}) == 3600


def test_paused_history_amendment_is_atomic_and_keeps_old_attempts(control):
    store,engine=control
    key,mission=setup(control,source_limit=3,mission_limit=3)
    reserved=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    store.record_outcome(reserved['attempt_id'],outcome='transport_error')
    with engine.begin() as conn:
        conn.execute(text('UPDATE control.source_budgets SET enabled=false WHERE scope_key=:key'),
                     {'key':key})
        conn.execute(text("UPDATE control.source_missions SET state='paused' WHERE id=:id"),
                     {'id':mission})
    amended=store.amend_paused_history_limits(
        scope_key=key,mission_key='history-'+key,source='synthetic-pncp',
        restriction_scope='test-ip-'+key,expected_source_limit=3,
        expected_source_consumed=1,new_source_limit=6,
        expected_mission_limit=3,expected_mission_consumed=1,
        new_mission_limit=6,daily_attempt_limit=2,daily_timezone='America/Sao_Paulo')
    assert amended['daily_attempt_limit']==2
    with engine.connect() as conn:
        budget=conn.execute(text('''SELECT attempt_limit,attempts_consumed,enabled
            FROM control.source_budgets WHERE scope_key=:key'''),{'key':key}).one()
        row=conn.execute(text('''SELECT attempt_limit,attempts_consumed,state,scope
            FROM control.source_missions WHERE id=:id'''),{'id':mission}).one()
    assert budget==(6,1,False)
    assert row.attempt_limit==6 and row.attempts_consumed==1 and row.state=='paused'
    assert row.scope['daily_attempt_limit']==2
    with pytest.raises(ValueError,match='baseline'):
        store.amend_paused_history_limits(
            scope_key=key,mission_key='history-'+key,source='synthetic-pncp',
            restriction_scope='test-ip-'+key,expected_source_limit=3,
            expected_source_consumed=1,new_source_limit=6,
            expected_mission_limit=3,expected_mission_consumed=1,
            new_mission_limit=6,daily_attempt_limit=2,daily_timezone='America/Sao_Paulo')


def test_failed_caller_keeps_reservation_and_active_lease(control):
    store,engine=control
    key,mission=setup(control,source_limit=3,mission_limit=3,concurrency=1)
    first=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    assert first['state']=='reserved'
    try:
        raise RuntimeError('simulated crash after durable reservation')
    except RuntimeError:
        pass
    assert store.reserve_attempt(mission,work_kind='sync',request_hash='c'*64)['state']=='deferred'
    store.record_outcome(first['attempt_id'],outcome='transport_error')
    second=store.reserve_attempt(mission,work_kind='sync',request_hash='c'*64)
    assert second['state']=='reserved' and second['sequence_no']==2
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT attempts_consumed FROM control.source_budgets WHERE scope_key=:key'),
                           {'key':key})==2
        assert conn.scalar(text('SELECT count(*) FROM control.source_attempt_outcomes'))==1
    with pytest.raises(DBAPIError),engine.begin() as conn:
        conn.execute(text('UPDATE control.source_attempts SET work_kind=\'changed\' WHERE id=:id'),
                     {'id':first['attempt_id']})


def test_mission_cap_and_shared_retry_after_never_decrease(control):
    store,engine=control
    key,mission=setup(control,source_limit=5,mission_limit=1,concurrency=1)
    first=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    store.record_outcome(first['attempt_id'],outcome='http_429',http_status=429)
    far=datetime.now(timezone.utc)+timedelta(minutes=10)
    later=store.defer_source(key,far)
    assert store.defer_source(key,far-timedelta(minutes=5))==later
    assert store.reserve_attempt(mission,work_kind='backfill',request_hash='c'*64)['state']=='quota_exhausted'
    with engine.connect() as conn:
        assert conn.scalar(text('SELECT attempts_consumed FROM control.source_budgets WHERE scope_key=:key'),
                           {'key':key})==1


def test_partition_and_queue_are_idempotent_and_fenced(control):
    store,_=control
    _,mission=setup(control)
    part=store.register_partition(mission,resource='publication',modality_code=6,
        window_start=date(2026,1,1),window_end=date(2026,1,2))
    again=store.register_partition(mission,resource='publication',modality_code=6,
        window_start=date(2026,1,1),window_end=date(2026,1,2))
    assert part['id']==again['id']
    item=store.enqueue(mission,work_kind='detail',idempotency_key='d'*64,
        partition_id=part['id'],identity_key='official-1',payload={'safe':True})
    assert store.enqueue(mission,work_kind='detail',idempotency_key='d'*64,
        partition_id=part['id'],identity_key='official-1',payload={'safe':True})['id']==item['id']
    claim=store.claim_queue(mission,'worker-a')
    assert claim['id']==item['id'] and claim['fencing_token']==1
    with pytest.raises(ValueError,match='Stale queue'):
        store.finish_queue(item['id'],'worker-a',0)
    store.finish_queue(item['id'],'worker-a',1)
    assert store.claim_queue(mission,'worker-b') is None
    pclaim=store.claim_partition(mission,'worker-a')
    assert pclaim['id']==part['id'] and pclaim['fencing_token']==1
    with pytest.raises(ValueError,match='Stale claim'):
        store.advance_partition(part['id'],'worker-a',0,next_page=2,state='partial')
    with pytest.raises(ValueError,match='without atomic promotion'):
        store.advance_partition(part['id'],'worker-a',1,next_page=2,state='partial')
    store.advance_partition(part['id'],'worker-a',1,next_page=1,state='partial')
    assert store.claim_partition(mission,'worker-b') is None


def test_source_attempt_outcome_is_idempotent_and_immutable(control):
    store,_=control
    _,mission=setup(control)
    first=store.reserve_attempt(mission,work_kind='backfill',request_hash='b'*64)
    outcome=store.record_outcome(first['attempt_id'],outcome='timeout')
    assert store.record_outcome(first['attempt_id'],outcome='timeout')['attempt_id']==outcome['attempt_id']
    with pytest.raises(ValueError,match='immutable'):
        store.record_outcome(first['attempt_id'],outcome='http_200',http_status=200)
