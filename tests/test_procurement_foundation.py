"""C1: synthetic persistence contracts, RF-003/004, RN-026/028, RT-014/015."""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from app.procurement.models import Procurement, ProcurementVersion, ProcurementItem
from app.procurement.repository import ProcurementRepository, Snapshot, Item, Unit


@pytest.fixture
def session():
    engine = create_engine(os.environ['DATABASE_URL'])
    assert engine.url.database == os.environ['RADAR_TEST_DB']
    with engine.connect() as conn:
        tx = conn.begin()
        with Session(bind=conn, join_transaction_mode='create_savepoint') as session:
            yield session
        tx.rollback()
    engine.dispose()


@pytest.fixture
def raw(session):
    """Synthetic bytes only; no external requests (RN-026, RT-015)."""
    run, policy, job, raw = [uuid4() for _ in range(4)]
    session.execute(text("INSERT INTO control.pipeline_runs(id,pipeline_name) VALUES (:id,'synthetic-radar')"), {'id': run})
    session.execute(text("""INSERT INTO commercial.system_settings
        (id,namespace,version_no,values,schema_version,author,effective_at,content_hash)
        VALUES (:id,:ns,1,'{}','fixture-1','test',now(),:hash)"""),
        {'id': policy, 'ns': str(policy), 'hash': 'a'*64})
    session.execute(text("""INSERT INTO control.ingestion_jobs
        (id,pipeline_run_id,scope_key,window_start,window_end,source,resource,policy_id)
        VALUES (:id,:run,:scope,'2026-10-07','2026-10-13','fixture','purchases',:policy)"""),
        {'id': job, 'run': run, 'scope': str(job), 'policy': policy})
    body = b'{"synthetic":true}'
    session.execute(text("""INSERT INTO bronze.pncp_responses
        (id,job_id,request_fingerprint,http_status,received_at,body,body_sha256,
        headers_safe,contract_version,source,resource,contract_hash,request_params_safe,schema_version)
        VALUES (:id,:job,:hash,200,now(),:body,:hash,'{}','fixture-1','fixture','purchases',:hash,'{}','fixture-1')"""),
        {'id': raw, 'job': job, 'hash': hashlib.sha256(body).hexdigest(), 'body': body})
    return raw


def snapshot(raw, title='first', **overrides):
    args = dict(source='fixture', source_id='official-1', raw_response_id=raw,
        snapshot={'title': title}, source_manifest={'coverage': 'pending'},
        schema_version='fixture-1', observed_at=datetime.now(timezone.utc),
        items=(Item('1', 'synthetic item', Decimal('2.123456'), 'unit',
                    Decimal('4.123456'), 'known'),),
        units=(Unit('lot-1', 'lote', ('1',), 'complete'),))
    args.update(overrides)
    return Snapshot(**args)


def test_replay_and_return_to_old_content_preserve_observations(session, raw):
    """TST-003/004 partial: content reused; every distinct observation is audited."""
    repo = ProcurementRepository(session)
    first = snapshot(raw)
    a = repo.record(first, actor='test', correlation_id=uuid4())
    for _ in range(10):
        assert repo.record(first, actor='test', correlation_id=uuid4()).id == a.id
    b = repo.record(snapshot(raw, 'second'), actor='test', correlation_id=uuid4())
    back = repo.record(snapshot(raw), actor='test', correlation_id=uuid4())
    assert back.id == a.id and b.id != a.id
    assert session.scalar(select(Procurement.current_version_id)) == a.id
    assert len(session.scalars(select(ProcurementVersion)).all()) == 2
    assert session.scalar(text('SELECT count(*) FROM procurement.version_observations')) == 3
    assert session.scalar(text('SELECT count(*) FROM procurement.version_observations WHERE changed')) == 3


def test_unknowns_and_decimal_roundtrip(session, raw):
    """RN-026: no fabricated zero, timezone or fiscal validation."""
    value = snapshot(raw, items=(Item('1','unknown',None,None,None,'unknown'),))
    v = ProcurementRepository(session).record(value, actor='test', correlation_id=uuid4())
    item = session.scalar(select(ProcurementItem).where(ProcurementItem.version_id == v.id))
    assert item.quantity is None and item.estimated_unit_price is None
    p = session.get(Procurement, v.procurement_id)
    assert p.cnpj is None and p.cnpj_validation_status == 'pendente'
    v2 = ProcurementRepository(session).record(snapshot(raw), actor='test', correlation_id=uuid4())
    item2 = session.scalar(select(ProcurementItem).where(ProcurementItem.version_id == v2.id))
    assert item2.quantity == Decimal('2.123456')
    assert item2.estimated_unit_price == Decimal('4.123456')


@pytest.mark.parametrize('value', [Decimal('NaN'),Decimal('Infinity'),Decimal('-1'),Decimal('0'),Decimal('1.1234567'),1.2])
def test_invalid_quantity_rejected_before_write(session, raw, value):
    """RNF-010: never let NUMERIC silently round/accept nonfinite inputs."""
    with pytest.raises(ValueError):
        ProcurementRepository(session).record(snapshot(raw, items=(Item('1','x',value,'unit',None,'unknown'),)), actor='test', correlation_id=uuid4())
    assert session.scalar(text('SELECT count(*) FROM procurement.procurements')) == 0


@pytest.mark.parametrize('sql', [
    "UPDATE procurement.procurement_versions SET snapshot='{}'",
    "DELETE FROM procurement.procurement_versions",
    "UPDATE procurement.procurement_items SET description='changed'",
    "DELETE FROM procurement.judgment_unit_items",
    "UPDATE procurement.version_observations SET actor='changed'",
    "UPDATE bronze.pncp_responses SET body='changed'",
])
def test_published_history_immutable_even_via_sql(session, raw, sql):
    """RN-028: database protects immutable snapshots/children/audit/Bronze."""
    ProcurementRepository(session).record(snapshot(raw), actor='test', correlation_id=uuid4())
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text(sql))


def test_cross_parent_pointer_rejected(session, raw):
    """TST-039 partial: pointer cannot refer to another procurement."""
    repo = ProcurementRepository(session)
    a = repo.record(snapshot(raw), actor='test', correlation_id=uuid4())
    b = repo.record(snapshot(raw, source_id='official-2'), actor='test', correlation_id=uuid4())
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text('UPDATE procurement.procurements SET current_version_id=:v WHERE id=:p'), {'v':b.id,'p':a.procurement_id})


def test_cross_version_composition_and_missing_item_rejected(session, raw):
    """TST-039: all unit members must exist in the same version."""
    with pytest.raises(ValueError):
        ProcurementRepository(session).record(snapshot(raw, units=(Unit('x','lote',('absent',),'complete'),)), actor='test', correlation_id=uuid4())
    assert session.scalar(text('SELECT count(*) FROM procurement.procurements')) == 0


def test_single_item_unit_requires_exactly_one_member(session, raw):
    """08 integrity: item units cannot silently become empty/groups."""
    with pytest.raises(ValueError):
        ProcurementRepository(session).record(snapshot(raw, units=(Unit('x','item',(),'complete'),)), actor='test', correlation_id=uuid4())


def test_raw_source_mismatch_rejected(session, raw):
    """RN-026: provenance is checked, not supplied as an unverified UUID."""
    with pytest.raises(ValueError):
        ProcurementRepository(session).record(snapshot(raw, source='different'), actor='test', correlation_id=uuid4())


def test_drafts_cannot_become_current_by_sql(session, raw):
    """08: incomplete drafts do not become authoritative current versions."""
    repo = ProcurementRepository(session)
    v = repo.record(snapshot(raw), actor='test', correlation_id=uuid4())
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text("UPDATE procurement.procurement_versions SET publication_state='rascunho' WHERE id=:v"), {'v':v.id})


def test_manifest_change_and_delayed_observation(session, raw):
    """RF-004 partial: resource coverage changes version; late data keeps audit."""
    repo = ProcurementRepository(session)
    now = datetime.now(timezone.utc)
    a = repo.record(snapshot(raw, observed_at=now), actor='test', correlation_id=uuid4())
    b = repo.record(snapshot(raw, observed_at=now+timedelta(seconds=2),
        source_manifest={'coverage':'changed'}), actor='test', correlation_id=uuid4())
    old = repo.record(snapshot(raw, observed_at=now+timedelta(seconds=1)), actor='test', correlation_id=uuid4())
    assert a.id != b.id and old.id == a.id
    assert session.scalar(select(Procurement.current_version_id)) == b.id
    assert session.scalar(text('SELECT count(*) FROM procurement.version_observations WHERE NOT changed')) == 1


def test_conflicting_observation_aborts_without_history_change(session, raw):
    """RN-028: idempotency key collision must not overwrite audit."""
    repo = ProcurementRepository(session)
    value = snapshot(raw)
    first = repo.record(value, actor='test', correlation_id=uuid4())
    with pytest.raises(ValueError):
        repo.record(snapshot(raw,'conflict',observed_at=value.observed_at),actor='test',correlation_id=uuid4())
    assert session.scalar(select(Procurement.current_version_id)) == first.id
    assert session.scalar(text('SELECT count(*) FROM procurement.version_observations')) == 1


def test_database_failure_is_atomic(session, raw):
    """RT-001: failure after identity creation leaves no partial snapshot."""
    with pytest.raises(DBAPIError):
        ProcurementRepository(session).record(snapshot(raw,items=(Item('1','x',None,None,None,'x'*30),)),actor='test',correlation_id=uuid4())
    assert session.scalar(text('SELECT count(*) FROM procurement.procurements')) == 0
    assert session.scalar(text('SELECT count(*) FROM procurement.procurement_versions')) == 0


def test_composite_foreign_key_rejects_cross_version_sql(session, raw):
    """TST-039: test FK independently of the repository membership guard."""
    v = ProcurementRepository(session).record(snapshot(raw),actor='test',correlation_id=uuid4())
    draft, unit = uuid4(), uuid4()
    session.execute(text("""INSERT INTO procurement.procurement_versions
      (id,procurement_id,raw_response_id,version_no,content_hash,observed_at,snapshot,
       schema_version,source_manifest,canonicalizer_version)
      VALUES (:id,:p,:raw,2,:hash,now(),'{}','fixture-1','{}','fixture-1')"""),
      {'id':draft,'p':v.procurement_id,'raw':raw,'hash':'b'*64})
    session.execute(text("""INSERT INTO procurement.judgment_units
        (id,version_id,source_unit_key,kind,composition_status) VALUES (:id,:v,'draft','lote','pending')"""), {'id':unit,'v':draft})
    item = session.scalar(select(ProcurementItem.id).where(ProcurementItem.version_id==v.id))
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text('INSERT INTO procurement.judgment_unit_items(unit_id,item_id,version_id,required) VALUES (:u,:i,:v,true)'), {'u':unit,'i':item,'v':draft})
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text('UPDATE procurement.procurements SET current_version_id=:v WHERE id=:p'), {'v':draft,'p':v.procurement_id})


def test_bronze_hash_must_match_bytes(session, raw):
    """RN-026: even direct inserts cannot invent raw integrity."""
    with pytest.raises(DBAPIError), session.begin_nested():
        session.execute(text("""INSERT INTO bronze.pncp_responses
          (job_id,request_fingerprint,http_status,received_at,body,body_sha256,
           headers_safe,contract_version,source,resource,request_params_safe,schema_version)
          SELECT job_id,request_fingerprint,200,now(),'different',body_sha256,
           '{}','fixture-1',source,resource,'{}','fixture-1' FROM bronze.pncp_responses WHERE id=:id"""), {'id':raw})


def test_schema_acceptance_includes_restrict_and_triggers(session):
    """RT-014: same read-only structural checker used by real rehearsal."""
    from scripts.radar_schema import verify_c1
    report = verify_c1(session.connection().connection.driver_connection, installed=True)
    assert report['table_count'] == 9 and report['trigger_count'] == 8
