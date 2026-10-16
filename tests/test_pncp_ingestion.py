"""HU-001: synthetic official-envelope fixtures, RF-001/RN-026/027/RT-001."""
from datetime import date, datetime, timezone
import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from app.procurement.pncp import Scope, Store, Collector, live_transport
from app.procurement.pncp_transport import Response


def page(number, records, total=3, pages=2):
    return Response(200,{'content-type':'application/json'},json.dumps({
        'data':records,'numeroPagina':number,'totalRegistros':total,'totalPaginas':pages,
        'paginasRestantes':pages-number,'empty':not records}).encode())


def record(identity):
    return {'numeroControlePNCP':identity,'objetoCompra':'Synthetic only',
        'orgaoEntidade':{'cnpj':'SYNTHETIC-NOT-VALIDATED'},
        'dataPublicacaoPncp':'2026-10-10T10:00:00',
        'dataEncerramentoProposta':'2026-10-20T12:00:00',
        'valorTotalEstimado':0,'newField':{'keep':'original'}}


@pytest.fixture
def store():
    engine = create_engine(os.environ['DATABASE_URL'])
    store = Store(engine, expected_database=os.environ['RADAR_TEST_DB'])
    with engine.connect() as conn:
        original_runs = set(conn.execute(text('SELECT id FROM control.pipeline_runs')).scalars())
    yield store
    # This file owns synthetic data only, in the runner-verified disposable DB.
    with engine.begin() as conn:
        added_runs = set(conn.execute(text('SELECT id FROM control.pipeline_runs')).scalars())-original_runs
        conn.execute(text('''TRUNCATE procurement.version_observations,
            procurement.judgment_unit_items, procurement.judgment_units,
            procurement.procurement_items, procurement.procurement_versions,
            procurement.procurements, bronze.pncp_responses, control.ingestion_jobs,
            commercial.system_settings'''))
        for run_id in added_runs:
            conn.execute(text('DELETE FROM control.pipeline_runs WHERE id=:id'),{'id':run_id})
    engine.dispose()


def scope():
    # Synthetic modality code; does not assert an official modality domain.
    return Scope(date(2026,10,7),date(2026,10,13),999,contract_hash='a'*64)


def job(store):
    return store.create(scope(),session_id='synthetic-'+str(uuid4()))


def test_two_pages_replay_and_original_bytes(store):
    """TST-001: three identities, stable versions and durable page advancement."""
    prefix = str(uuid4())
    replies = [page(1,[record(prefix+'a'),record(prefix+'b')]),page(2,[record(prefix+'c')])]
    job_id = job(store)
    calls = []
    def send(url):
        calls.append(url)
        return replies[len(calls)-1]
    outcome = Collector(store,send,sleep=lambda _:None).run(job_id)
    assert outcome['state']=='success' and outcome['next_page']==3
    assert outcome['checkpoint']['processed']==3
    for _ in range(10):
        assert Collector(store,lambda _:pytest.fail('completed job fetched again')).run(job_id)['state']=='success'
    assert len(calls)==2
    with store.engine.connect() as conn:
        raw = conn.execute(text('SELECT body FROM bronze.pncp_responses WHERE job_id=:id ORDER BY page'),{'id':job_id}).scalars().all()
        assert [bytes(value) for value in raw]==[reply.body for reply in replies]
        versions = conn.execute(text("SELECT v.deadline_at,v.snapshot FROM procurement.procurement_versions v JOIN procurement.procurements p ON p.id=v.procurement_id WHERE p.source_id LIKE :prefix"),{'prefix':prefix+'%'}).all()
        assert len(versions)==3 and all(row.deadline_at is None for row in versions)
        assert versions[0].snapshot['newField']=={'keep':'original'}


def test_page_two_failure_then_resume_same_cursor(store):
    """TST-001/042: partial is explicit and resumption never refetches page 1."""
    prefix = str(uuid4())
    replies = iter([page(1,[record(prefix+'a'),record(prefix+'b')]),Response(400,{},b'bad parameters')])
    job_id = job(store)
    result = Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job_id)
    assert result['state']=='partial' and result['next_page']==2
    assert result['checkpoint']['processed']==2
    calls=[]
    def send(url):
        calls.append(url)
        return page(2,[record(prefix+'c')])
    result = Collector(store,send,sleep=lambda _:None).run(job_id)
    assert result['state']=='success' and len(calls)==1 and 'pagina=2' in calls[0]


@pytest.mark.parametrize('reply', [
    Response(200,{},b'not json'),
    page(2,[record('bad-page')]),
    page(1,[{'objetoCompra':'missing identity'}]),
])
def test_bad_page_preserves_bronze_without_advancing(store,reply):
    """RN-026/027: invalid data cannot create a successful checkpoint."""
    job_id=job(store)
    result=Collector(store,lambda _:reply,sleep=lambda _:None).run(job_id)
    assert result['state']=='partial' and result['next_page']==1
    assert result['checkpoint']['processed']==0
    with store.engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM bronze.pncp_responses WHERE job_id=:id'),{'id':job_id})==1


def test_transient_errors_are_counted_and_retry_is_bounded(store):
    """RT-001: no Prefect multiplier and only three attempts per page/session."""
    job_id=job(store)
    calls=[]
    def fail(url):
        calls.append(url)
        raise TimeoutError('synthetic')
    result=Collector(store,fail,sleep=lambda _:None).run(job_id)
    assert len(calls)==3 and result['state']=='partial' and result['request_count']==3
    Collector(store,lambda _:pytest.fail('retry budget renewed')).run(job_id)


def test_long_retry_after_defers_without_another_request(store):
    """RT-001: Retry-After above local retry window pauses instead of hammering."""
    job_id=job(store)
    result=Collector(store,lambda _:Response(429,{'retry-after':'120'},b'limited'),sleep=lambda _:pytest.fail('must defer')).run(job_id)
    assert result['state']=='partial' and result['request_count']==1
    assert result['checkpoint']['not_before'] > datetime.now(timezone.utc).timestamp()
    Collector(store,lambda _:pytest.fail('Retry-After ignored')).run(job_id)


def test_204_does_not_delete_previous_local_data(store):
    """TST-005 partial: empty source query never removes earlier identities."""
    job_id=job(store)
    with store.engine.connect() as conn:
        before=conn.scalar(text('SELECT count(*) FROM procurement.procurements'))
    result=Collector(store,lambda _:Response(204,{},b''),sleep=lambda _:None).run(job_id)
    assert result['state']=='success'
    with store.engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM procurement.procurements'))==before


def test_stale_worker_and_wrong_database_refused(store):
    """TST-039: fencing rejects stale page writes before changing checkpoint."""
    job_id=job(store)
    owner=str(uuid4())
    token=store.claim(job_id,owner)
    with pytest.raises(RuntimeError):
        store.claim(job_id,str(uuid4()))
    with pytest.raises(RuntimeError):
        store.note_attempt(job_id,owner,token+1)
    store.release(job_id,owner,token,'partial','test')
    with pytest.raises(ValueError):
        Store(store.engine,expected_database='motor_decisao')


def test_unverified_live_contract_blocks_before_send(tmp_path):
    """RT-001: unresolved date boundaries cannot be bypassed by a default."""
    with pytest.raises(ValueError):
        live_transport(tmp_path/'not-created')


def test_new_job_replays_content_without_new_version(store):
    """RF-003 partial: repeat acquisition observes content without duplicating it."""
    identity=str(uuid4())
    response=page(1,[record(identity)],total=1,pages=1)
    for _ in range(10):
        Collector(store,lambda _:response,sleep=lambda _:None).run(job(store))
    with store.engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM procurement.procurement_versions v JOIN procurement.procurements p ON p.id=v.procurement_id WHERE p.source_id=:id'),{'id':identity})==1


def test_zero_quota_does_not_count_an_unsent_job_request(store,tmp_path):
    """RT-001: global quota and actual job reservations stay reconcilable."""
    from app.procurement.pncp_transport import Budget
    Budget.create(tmp_path/'zero',session_id='synthetic',limit=0,interval=1)
    job_id=job(store)
    result=Collector(store,Budget(tmp_path/'zero').request).run(job_id)
    assert result['state']=='partial' and result['error_code']=='quota_exhausted'
    assert result['request_count']==0


def test_consistent_empty_200_and_changing_totals(store):
    """TST-001/005: distinguish a verified zero from unstable pagination."""
    empty=Collector(store,lambda _:page(1,[],total=0,pages=1)).run(job(store))
    assert empty['state']=='success'
    prefix=str(uuid4())
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')]),page(2,[record(prefix+'c')],total=4)])
    result=Collector(store,lambda _:next(replies)).run(job(store))
    assert result['state']=='partial' and result['next_page']==2 and result['checkpoint']['processed']==2


def test_prefect_manual_body_uses_existing_job_and_zero_retries(store,monkeypatch):
    """RT-001: flow has no retry multiplier, deployment or implicit schedule."""
    from pipelines.procurement import pncp_flow
    monkeypatch.setattr(pncp_flow,'live_transport',lambda *args:lambda _:Response(204,{},b''))
    result=pncp_flow.collect_pncp.fn(str(job(store)))
    assert result['state']=='success'
    assert pncp_flow.collect_pncp.retries==0


def test_same_body_different_http_status_keeps_both_receipts(store):
    """RN-026: error and successful receipt cannot share a misleading status."""
    identity=str(uuid4())
    good=page(1,[record(identity)],total=1,pages=1)
    replies=iter([Response(500,{},good.body),good])
    job_id=job(store)
    result=Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job_id)
    assert result['state']=='success'
    with store.engine.connect() as conn:
        assert conn.execute(text('SELECT http_status FROM bronze.pncp_responses WHERE job_id=:id ORDER BY created_at'),{'id':job_id}).scalars().all()==[500,200]


def test_json_decimal_never_roundtrips_through_float(store):
    """RNF-010/RT-001: exact source number stays decimal in the snapshot."""
    identity=str(uuid4())
    original=page(1,[record(identity)],total=1,pages=1)
    body=original.body.replace(b'"valorTotalEstimado": 0',b'"valorTotalEstimado": 123456.123456789012345678')
    result=Collector(store,lambda _:Response(200,{},body)).run(job(store))
    assert result['state']=='success'
    with store.engine.connect() as conn:
        value=conn.scalar(text('SELECT v.snapshot FROM procurement.procurement_versions v JOIN procurement.procurements p ON p.id=v.procurement_id WHERE p.source_id=:id'),{'id':identity})
        assert value['valorTotalEstimado']=='123456.123456789012345678'
