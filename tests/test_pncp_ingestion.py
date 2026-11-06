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
        conn.execute(text('''TRUNCATE control.source_attempt_outcomes,
            control.source_attempts, control.source_partition_identities,
            control.source_queue, control.source_watermarks,
            control.source_partitions, control.source_missions,
            control.source_budgets, procurement.version_observations,
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


def test_operator_flow_grant_matches_only_fixed_isolated_scope(tmp_path,monkeypatch):
    """The temporary user grant is bound to this session, window and modality."""
    from app.procurement import pncp_transport
    from app.procurement.pncp_transport import Budget,Response
    monkeypatch.setattr(pncp_transport.time,'sleep',lambda _:None)
    path=tmp_path/'c2-20261013'
    Budget.create(path,session_id='c2-20261013',limit=50,interval=1)
    budget=Budget(path)
    for _ in range(7):
        budget.request('https://pncp.gov.br/api/consulta/v3/api-docs',lambda _:Response(200,{},b'{}'))
    budget.split_operator_budgets()
    contract='a5d87b60a54acfd681e84e0a77d541b0db7065401171508c53c911d12bdd121d'
    authorized=Scope(date(2026,10,6),date(2026,10,14),6,contract)
    send=live_transport(path,authorized,session_id='c2-20261013')
    assert send.__self__.category=='data' and send.__self__.purpose=='collection'
    wrong_window=Scope(date(2026,10,7),date(2026,10,14),6,contract)
    with pytest.raises(ValueError):
        live_transport(path,wrong_window,session_id='c2-20261013')


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


def test_changed_page_totals_require_reconciliation_without_promoting_page(store):
    prefix=str(uuid4())
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')],total=9577,pages=958),
                  page(2,[record(prefix+'c')],total=9562,pages=957)])
    job_id=job(store)
    result=Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job_id)
    assert result['state']=='partial' and result['error_code']=='SourceChanged'
    assert result['next_page']==2 and result['checkpoint']['processed']==2
    change=result['checkpoint']['source_change']
    assert change['reason']=='pagination_totals_changed'
    assert change['page']==2
    assert change['expected']=={'total_records':9577,'total_pages':958}
    assert change['observed']=={'total_records':9562,'total_pages':957}
    assert change['raw_response_id']
    assert change['reconciliation_policy']=='child_partition_replay_required'
    assert change['max_replays']==2 and change['replays_started']==0
    resumed=Collector(store,lambda _:pytest.fail('changed source retried without reconciliation')).run(job_id)
    assert resumed['error_code']=='SourceChanged' and resumed['request_count']==2
    with store.engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM bronze.pncp_responses WHERE job_id=:id'),{'id':job_id})==2
        assert conn.scalar(text('''SELECT count(*) FROM procurement.version_observations
            WHERE correlation_id=:id'''),{'id':job_id})==2


def test_stable_totals_with_identity_moved_between_pages_require_reconciliation(store):
    prefix=str(uuid4())
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')],total=4,pages=2),
                  page(2,[record(prefix+'b'),record(prefix+'d')],total=4,pages=2)])
    job_id=job(store)
    result=Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job_id)
    assert result['state']=='partial' and result['error_code']=='SourceChanged'
    assert result['next_page']==2 and result['checkpoint']['processed']==2
    assert result['checkpoint']['source_change']['reason']=='repeated_identity_across_pages'
    with store.engine.connect() as conn:
        assert conn.scalar(text('SELECT count(*) FROM bronze.pncp_responses WHERE job_id=:id'),{'id':job_id})==2
        assert conn.scalar(text('''SELECT count(*) FROM procurement.version_observations
            WHERE correlation_id=:id'''),{'id':job_id})==2


def test_stable_totals_without_duplicate_still_do_not_prove_universe_complete(store):
    prefix=str(uuid4())
    # An independent source says C should exist, but pagination returns D/E instead.
    # With no snapshot or independent enumeration the adapter cannot discover C.
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')],total=4,pages=2),
                  page(2,[record(prefix+'d'),record(prefix+'e')],total=4,pages=2)])
    result=Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job(store))
    assert result['state']=='success'
    assert result['checkpoint']['coverage_status']=='pagination_consistent_unverified'


def test_saved_bronze_classifies_legacy_error_without_request_or_cursor_advance(store):
    prefix=str(uuid4())
    job_id=job(store)
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')],total=9577,pages=958),
                  page(2,[record(prefix+'c')],total=9562,pages=957)])
    typed=Collector(store,lambda _:next(replies),sleep=lambda _:None).run(job_id)
    assert typed['error_code']=='SourceChanged'
    with store.engine.begin() as conn:
        conn.execute(text('''UPDATE control.ingestion_jobs SET error_code='ValueError',
            checkpoint=checkpoint - 'source_change' WHERE id=:id'''),{'id':job_id})
    before=store.get(job_id)
    classified=store.classify_saved_source_change(job_id,session_id=before['checkpoint']['session'])
    assert classified['error_code']=='SourceChanged'
    assert classified['next_page']==before['next_page']==2
    assert classified['request_count']==before['request_count']==2
    assert classified['records_seen']==before['records_seen']==2
    assert classified['checkpoint']['source_change']['classification_source']=='saved_bronze_page'
    again=store.classify_saved_source_change(job_id,session_id=before['checkpoint']['session'])
    assert again['checkpoint']['source_change']==classified['checkpoint']['source_change']


def test_manual_flow_reports_reconciliation_as_business_outcome(store,monkeypatch):
    from pipelines.procurement import pncp_flow
    prefix=str(uuid4())
    replies=iter([page(1,[record(prefix+'a'),record(prefix+'b')],total=9577,pages=958),
                  page(2,[record(prefix+'c')],total=9562,pages=957)])
    monkeypatch.setattr(pncp_flow,'live_transport',lambda *args,**kwargs:lambda _:next(replies))
    result=pncp_flow.collect_pncp.fn(str(job(store)))
    assert result['state']=='partial'
    assert result['business_outcome']=='reconciliation_required'
    assert result['reconciliation_required'] is True
    assert result['error_code']=='SourceChanged'


def test_prefect_manual_body_uses_existing_job_and_zero_retries(store,monkeypatch):
    """RT-001: flow has no retry multiplier, deployment or implicit schedule."""
    from pipelines.procurement import pncp_flow
    monkeypatch.setattr(pncp_flow,'live_transport',lambda *args,**kwargs:lambda _:Response(204,{},b''))
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


def test_manual_collection_can_stop_after_one_page_and_resume(store):
    """A real validation can persist one bounded page without losing its cursor."""
    prefix=str(uuid4())
    job_id=job(store)
    calls=[]
    replies=[page(1,[record(prefix+'a'),record(prefix+'b')]),
             page(2,[record(prefix+'c')])]
    def send(_url):
        response=replies[len(calls)]
        calls.append(_url)
        return response
    result=Collector(store,send,sleep=lambda _:None).run(job_id,max_pages=1)
    assert result['state']=='partial' and result['error_code']=='sample_page_limit'
    assert result['next_page']==2 and result['records_seen']==2 and result['request_count']==1
    resumed=Collector(store,send,sleep=lambda _:None).run(job_id,max_pages=1)
    assert resumed['state']=='success' and resumed['records_seen']==3 and resumed['request_count']==2
