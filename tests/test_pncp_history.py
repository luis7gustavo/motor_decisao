"""S03: isolated synthetic backfill; no external transport is used."""

from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
import json
import multiprocessing
import os
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text

from app.procurement.history import HistoryBackfill, RESOURCE, UPDATE_RESOURCE, job_uuid, windows
from app.procurement.pncp import Store
from app.procurement.pncp_transport import Response
from app.procurement.sync import SyncCoordinator


def record(identity):
    return {'numeroControlePNCP': identity, 'objetoCompra': 'Synthetic history',
            'orgaoEntidade': {'cnpj': '00000000000000'},
            'dataAtualizacaoGlobal': '2026-01-02T12:00:00+00:00'}


def page(number, data, total, pages):
    return Response(200, {'content-type': 'application/json'}, json.dumps({
        'data': data, 'numeroPagina': number, 'totalRegistros': total,
        'totalPaginas': pages, 'paginasRestantes': pages - number,
        'empty': not data}).encode())


@pytest.fixture
def store():
    engine = create_engine(os.environ['DATABASE_URL'])
    value = Store(engine, expected_database=os.environ['RADAR_TEST_DB'])
    yield value
    with engine.begin() as conn:
        conn.execute(text('''TRUNCATE control.source_attempt_outcomes,
            control.source_attempts,control.source_partition_identities,
            control.source_queue,control.source_watermarks,control.source_partitions,
            control.source_missions,control.source_budgets,
            procurement.version_observations,procurement.judgment_unit_items,
            procurement.judgment_units,procurement.procurement_items,
            procurement.procurement_versions,procurement.procurements,
            bronze.pncp_responses,control.ingestion_jobs,commercial.system_settings'''))
        conn.execute(text("DELETE FROM control.pipeline_runs WHERE pipeline_name='radar-pncp-history'"))
    engine.dispose()


def plan(service, *, start=date(2026, 1, 1), end=date(2026, 1, 2),
         modalities=(6,), window_days=2, limit=20):
    key = uuid4().hex
    return service.plan(mission_key='synthetic-' + key, budget_scope_key=key,
        restriction_scope='test-ip-' + key, contract_hash='a' * 64,
        coverage_start=start, fixed_end=end, modalities=list(modalities),
        window_days=window_days, attempt_limit=limit, enabled=True, page_size=10)


def test_history_plan_keeps_source_cap_separate_from_mission_cap(store):
    service = HistoryBackfill(store, lambda _: Response(204, {}, b''), allow_synthetic=True)
    key = uuid4().hex
    registered = service.plan(mission_key='synthetic-' + key,
        budget_scope_key=key, restriction_scope='test-ip-' + key,
        contract_hash='a' * 64, coverage_start=date(2026, 1, 1),
        fixed_end=date(2026, 1, 1), modalities=[6], window_days=1,
        attempt_limit=2, source_attempt_limit=5, enabled=True, page_size=10)
    assert registered['budget']['attempt_limit'] == 5
    assert registered['mission']['attempt_limit'] == 2
    with pytest.raises(ValueError, match='Shared source cap'):
        service.plan(mission_key='synthetic-invalid-' + key,
            budget_scope_key=key, restriction_scope='test-ip-' + key,
            contract_hash='a' * 64, coverage_start=date(2026, 1, 1),
            fixed_end=date(2026, 1, 1), modalities=[6], window_days=1,
            attempt_limit=2, source_attempt_limit=1, enabled=True, page_size=10)


def test_html_200_is_archived_without_page_promotion(store):
    service = HistoryBackfill(
        store, lambda _: Response(200, {'content-type': 'text/html'},
                                  b'<!doctype html><title>throttled</title>', elapsed_ms=17),
        allow_synthetic=True)
    registered = plan(service, start=date(2026, 1, 1), end=date(2026, 1, 1))
    mission = registered['mission']['id']
    result = service.step(mission)
    assert result == {'state': 'deferred', 'reason': 'html_200'}
    with store.transaction() as session:
        row = session.execute(text('''SELECT p.next_page, b.http_status,
            b.request_params_safe->>'client_elapsed_ms' AS elapsed,
            o.outcome FROM control.source_partitions p
            JOIN control.ingestion_jobs j ON j.scope_key=p.scope_key
            JOIN bronze.pncp_responses b ON b.job_id=j.id
            JOIN control.source_attempt_outcomes o ON o.bronze_response_id=b.id
            WHERE p.mission_id=:mission'''), {'mission': mission}).one()
    assert (row.next_page, row.http_status, row.elapsed, row.outcome) == (1, 200, '17', 'html_200')


def test_history_plan_registers_both_accessible_listing_resources(store):
    urls = []
    def send(url):
        urls.append(url)
        return Response(204, {}, b'')
    service = HistoryBackfill(store, send, allow_synthetic=True)
    key = uuid4().hex
    registered = service.plan(mission_key='synthetic-' + key,
        budget_scope_key=key, restriction_scope='test-ip-' + key,
        contract_hash='a' * 64, coverage_start=date(2026, 1, 1),
        fixed_end=date(2026, 1, 1), modalities=[6], window_days=1,
        attempt_limit=10, enabled=True, page_size=10,
        historical_resources=(RESOURCE, UPDATE_RESOURCE))
    assert {part['resource'] for part in registered['partitions']} == {RESOURCE, UPDATE_RESOURCE}
    assert service.step(registered['mission']['id'], resource_filter=UPDATE_RESOURCE)['state'] == 'complete'
    assert urlsplit(urls[0]).path.endswith(UPDATE_RESOURCE)
    assert service.step(registered['mission']['id'], resource_filter=RESOURCE)['state'] == 'complete'
    with store.transaction() as s:
        assert s.execute(text("SELECT work_kind FROM control.source_attempts "
                              "WHERE mission_id=:id ORDER BY sequence_no"),
                         {'id': registered['mission']['id']}).scalars().all() == [
            'backfill', 'backfill']


def _two_page_send(url):
    number = int(parse_qs(urlsplit(url).query)['pagina'][0])
    return page(1, [record('process-a')], 2, 2) if number == 1 else page(2, [record('process-b')], 2, 2)


def _process_step(mission_id, results, two_pages=False):
    engine = create_engine(os.environ['DATABASE_URL'])
    try:
        isolated = Store(engine, expected_database=os.environ['RADAR_TEST_DB'])
        sender = _two_page_send if two_pages else lambda _: Response(204, {}, b'')
        worker = HistoryBackfill(isolated, sender, allow_synthetic=True)
        results.put(worker.step(UUID(mission_id))['state'])
    finally:
        engine.dispose()


def test_windows_have_no_gaps_or_overlap():
    assert list(windows(date(2026, 1, 1), date(2026, 1, 6), 2)) == [
        (date(2026, 1, 1), date(2026, 1, 2)),
        (date(2026, 1, 3), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 6))]


def test_multiple_modalities_windows_empty_partition_and_restart(store):
    calls = []
    def send(url):
        query = parse_qs(urlsplit(url).query)
        calls.append(query)
        if query['codigoModalidadeContratacao'] == ['6'] and query['dataInicial'] == ['20260101']:
            return page(1, [record('synthetic-a')], 2, 2) if query['pagina'] == ['1'] else page(2, [record('synthetic-b')], 2, 2)
        return page(1, [], 0, 1)
    first = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(first, end=date(2026, 1, 4), modalities=(6, 7), window_days=2)
    mission = planned['mission']['id']
    assert len(planned['partitions']) == 4
    assert first.step(mission)['state'] == 'page_committed'
    # New worker object has no in-memory cursor or seen-ID set.
    restarted = HistoryBackfill(store, send, allow_synthetic=True)
    outcomes = [restarted.step(mission) for _ in range(5)]
    assert [o['state'] for o in outcomes] == ['complete'] * 4 + ['idle']
    status = restarted.status(mission)
    assert status['partitions'] == {'complete': 4}
    assert status['identities'] == 2 and status['attempts_consumed'] == 5
    assert len(restarted.audit(mission)) == 4
    with store.transaction() as s:
        assert s.scalar(text("""SELECT count(*) FROM bronze.pncp_responses WHERE job_id IN
            (SELECT id FROM control.ingestion_jobs WHERE source='pncp' AND resource='/v1/contratacoes/publicacao')""")) == 5
        assert s.scalar(text("SELECT count(*) FROM procurement.procurements WHERE source_id LIKE 'synthetic-%'")) == 2
        sizes = s.execute(text("""SELECT octet_length(checkpoint::text) FROM control.ingestion_jobs WHERE pipeline_run_id IN
            (SELECT id FROM control.pipeline_runs WHERE pipeline_name='radar-pncp-history')""")).scalars().all()
        assert max(sizes) < 512
    assert len(calls) == 5


def test_mutable_total_keeps_bronze_and_splits_without_advancing_parent(store):
    def send(url):
        query = parse_qs(urlsplit(url).query)
        number = int(query['pagina'][0])
        if query['dataInicial'] == query['dataFinal']:
            identity = 'split-a' if query['dataInicial'] == ['20260101'] else 'split-b'
            return page(1, [record(identity)], 1, 1)
        return page(1, [record('split-a')], 2, 2) if number == 1 else page(2, [record('split-b')], 3, 2)
    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service)
    mission = planned['mission']['id']
    parent = planned['partitions'][0]
    assert service.step(mission)['state'] == 'page_committed'
    assert service.step(mission) == {'state': 'reconcile', 'reason': 'pagination_totals_changed'}
    audit = service.audit(mission)
    assert len(audit) == 3
    assert audit[0]['state'] == 'reconcile' and audit[0]['next_page'] == 2
    children = [p for p in audit if p['parent_id'] == str(parent['id'])]
    assert len(children) == 2 and all(p['state'] == 'pending' for p in children)
    with store.transaction() as s:
        job = s.execute(text('SELECT checkpoint,next_page FROM control.ingestion_jobs WHERE id=:id'),
                        {'id': job_uuid(parent['id'])}).one()
        assert job.next_page == 2
        assert job.checkpoint['source_change']['reason'] == 'pagination_totals_changed'
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses WHERE job_id=:id'),
                        {'id': job_uuid(parent['id'])}) == 2
        assert s.scalar(text('SELECT count(*) FROM control.source_partition_identities WHERE partition_id=:id'),
                        {'id': parent['id']}) == 1
    assert service.status(mission)['attempts_consumed'] == 2
    assert service.step(mission)['state'] == 'complete'
    assert service.step(mission)['state'] == 'complete'
    final_audit = service.audit(mission)
    assert sum(p['state'] == 'complete' for p in final_audit) == 2
    assert any(p['state'] == 'reconcile' for p in final_audit)
    assert service.step(mission)['state'] == 'idle'


def test_retry_after_is_durable_and_does_not_refund_attempt(store):
    calls = []
    def send(url):
        calls.append(url)
        return Response(429, {'retry-after': '120'}, b'throttled')
    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service, limit=2)
    mission = planned['mission']['id']
    assert service.step(mission) == {'state': 'deferred', 'reason': 'http_429'}
    second = HistoryBackfill(store, send, allow_synthetic=True).step(mission)
    assert second['state'] == 'deferred' and len(calls) == 1
    with store.transaction() as s:
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_missions WHERE id=:id'),
                        {'id': mission}) == 1
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 1


@pytest.mark.parametrize('retry_after', ['240', 'http-date'])
def test_retry_after_beyond_job_window_survives_restart(store, retry_after):
    calls = []
    earliest = datetime.now(timezone.utc) + timedelta(seconds=230)

    def send(url):
        calls.append(url)
        value = (format_datetime(datetime.now(timezone.utc) + timedelta(seconds=240),
                                 usegmt=True) if retry_after == 'http-date' else retry_after)
        return Response(429, {'retry-after': value}, b'throttled')

    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service, limit=3)
    mission = planned['mission']['id']
    assert service.step(mission) == {'state': 'deferred', 'reason': 'http_429'}
    assert HistoryBackfill(store, send, allow_synthetic=True).step(mission)['state'] == 'deferred'
    with store.transaction() as s:
        not_before = s.scalar(text('SELECT not_before FROM control.source_budgets '
                                   'WHERE scope_key=:key'), {'key': planned['budget']['scope_key']})
        assert not_before >= earliest
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 1
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 1
    assert len(calls) == 1


def test_transient_retry_cap_is_total_across_worker_restarts(store):
    calls = []

    def send(url):
        calls.append(url)
        return Response(503, {}, b'unavailable')

    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service, limit=10)
    mission = planned['mission']['id']
    for _ in range(3):
        assert HistoryBackfill(store, send, allow_synthetic=True).step(mission) == {
            'state': 'deferred', 'reason': 'http_503'}
        # Advance the synthetic clock boundary without sleeping through backoff.
        with store.transaction() as s:
            s.execute(text("UPDATE control.source_budgets SET not_before=clock_timestamp()-interval '1 second', "
                           "next_allowed_at=clock_timestamp()-interval '1 second' WHERE scope_key=:key"),
                      {'key': planned['budget']['scope_key']})
    assert HistoryBackfill(store, send, allow_synthetic=True).step(mission) == {
        'state': 'partial', 'reason': 'retry_exhausted'}
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 3
        assert s.scalar(text('SELECT count(*) FROM control.source_attempt_outcomes')) == 3
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 3
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_missions WHERE id=:id'),
                        {'id': mission}) == 3
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_budgets WHERE scope_key=:key'),
                        {'key': planned['budget']['scope_key']}) == 3
    assert len(calls) == 3
    split = service.split_exhausted_windows(mission)
    assert len(split) == 1
    assert split[0]['children'] == [['2026-01-01', '2026-01-01'],
                                    ['2026-01-02', '2026-01-02']]
    assert service.split_exhausted_windows(mission) == []
    with store.transaction() as s:
        assert s.scalar(text("SELECT count(*) FROM control.source_partitions "
                             "WHERE mission_id=:id AND state='reconcile'"),
                        {'id': mission}) == 1
        assert s.scalar(text("SELECT count(*) FROM control.source_partitions "
                             "WHERE mission_id=:id AND parent_id IS NOT NULL"),
                        {'id': mission}) == 2
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_missions WHERE id=:id'),
                        {'id': mission}) == 3


def test_failed_wide_window_uses_three_monthly_children_per_pass(store):
    calls = []

    def send(url):
        calls.append(url)
        return Response(503, {}, b'unavailable') if len(calls) == 1 else Response(204, {}, b'')

    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service, start=date(2023, 1, 1), end=date(2023, 3, 31),
                   window_days=90, limit=30)
    mission = planned['mission']['id']
    parent_id = planned['partitions'][0]['id']
    assert service.step(mission)['reason'] == 'http_503'
    with store.transaction() as s:
        s.execute(text("UPDATE control.source_budgets SET "
                       "not_before=clock_timestamp()-interval '1 second', "
                       "next_allowed_at=clock_timestamp()-interval '1 second' "
                       "WHERE scope_key=:key"),
                  {'key': planned['budget']['scope_key']})
    split = service.split_exhausted_windows(mission)
    assert split[0]['children'] == [
        ['2023-01-01', '2023-01-30'],
        ['2023-01-31', '2023-03-01'],
        ['2023-03-02', '2023-03-31'],
    ]
    assert [service.step(mission)['state'] for _ in range(3)] == ['complete'] * 3
    coordinator = SyncCoordinator(service)
    assert coordinator.reconcile_parent(parent_id)['state'] == 'second_pass_planned'
    assert [service.step(mission)['state'] for _ in range(3)] == ['complete'] * 3
    assert coordinator.reconcile_parent(parent_id)['state'] == 'reconciled_sem_snapshot'


def test_stable_totals_with_moved_identity_stays_reconcile(store):
    def send(url):
        number = int(parse_qs(urlsplit(url).query)['pagina'][0])
        return page(number, [record('same-id')], 2, 2)
    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service, start=date(2026, 1, 1), end=date(2026, 1, 1))
    mission = planned['mission']['id']
    assert service.step(mission)['state'] == 'page_committed'
    assert service.step(mission) == {'state': 'reconcile', 'reason': 'repeated_identity_across_pages'}
    audit = service.audit(mission)
    assert audit[0]['next_page'] == 2 and audit[0]['state'] == 'reconcile'
    assert service.status(mission)['identities'] == 1


def test_disabled_mission_never_creates_job_or_sends(store):
    def forbidden(url):
        pytest.fail('disabled mission reached transport')
    service = HistoryBackfill(store, forbidden, allow_synthetic=True)
    key = uuid4().hex
    planned = service.plan(mission_key='synthetic-disabled-' + key,
        budget_scope_key=key, restriction_scope='test-ip-' + key,
        contract_hash='a' * 64, coverage_start=date(2026, 1, 1),
        fixed_end=date(2026, 1, 1), modalities=[6], window_days=1,
        attempt_limit=0, enabled=False, page_size=10)
    assert service.step(planned['mission']['id']) == {'state': 'blocked'}
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM control.ingestion_jobs')) == 0
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 0


def test_two_processes_share_one_attempt_budget(store):
    service = HistoryBackfill(store, lambda _: pytest.fail('parent sent'), allow_synthetic=True)
    planned = plan(service, window_days=1, limit=1)
    mission = str(planned['mission']['id'])
    context = multiprocessing.get_context('fork')
    results = context.Queue()
    workers = [context.Process(target=_process_step, args=(mission, results)) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=30)
    assert [worker.exitcode for worker in workers] == [0, 0]
    states = sorted(results.get(timeout=1) for _ in workers)
    assert states == ['complete', 'quota_exhausted']
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 1
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_missions WHERE id=:id'),
                        {'id': planned['mission']['id']}) == 1


def test_two_processes_share_persistent_spacing(store):
    service = HistoryBackfill(store, lambda _: pytest.fail('parent sent'), allow_synthetic=True)
    planned = plan(service, window_days=1, limit=2)
    mission = str(planned['mission']['id'])
    with store.transaction() as s:
        s.execute(text('UPDATE control.source_budgets SET min_interval_ms=60000 WHERE scope_key=:key'),
                  {'key': planned['budget']['scope_key']})
    context = multiprocessing.get_context('fork')
    results = context.Queue()
    workers = [context.Process(target=_process_step, args=(mission, results)) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=30)
    assert [worker.exitcode for worker in workers] == [0, 0]
    assert sorted(results.get(timeout=1) for _ in workers) == ['complete', 'deferred']
    with store.transaction() as s:
        assert s.scalar(text('SELECT attempts_consumed FROM control.source_missions WHERE id=:id'),
                        {'id': planned['mission']['id']}) == 1
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 1


def test_process_exit_after_page_one_resumes_at_page_two(store):
    service = HistoryBackfill(store, _two_page_send, allow_synthetic=True)
    planned = plan(service)
    mission = planned['mission']['id']
    context = multiprocessing.get_context('fork')
    results = context.Queue()
    first = context.Process(target=_process_step, args=(str(mission), results, True))
    first.start()
    first.join(timeout=30)
    assert first.exitcode == 0 and results.get(timeout=1) == 'page_committed'
    assert service.audit(mission)[0]['next_page'] == 2
    restarted = HistoryBackfill(store, _two_page_send, allow_synthetic=True)
    assert restarted.step(mission)['state'] == 'complete'
    assert restarted.step(mission)['state'] == 'idle'
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM procurement.procurements')) == 2
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 2
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 2


def test_crash_after_bronze_retries_same_page_without_duplicate_projection(store, monkeypatch):
    calls = []
    def send(url):
        calls.append(url)
        return page(1, [record('crash-a')], 1, 1)
    service = HistoryBackfill(store, send, allow_synthetic=True)
    planned = plan(service)
    mission = planned['mission']['id']
    part = planned['partitions'][0]
    def crash(*args, **kwargs):
        raise SystemExit('simulated worker termination after durable Bronze')
    monkeypatch.setattr(service, '_promote', crash)
    with pytest.raises(SystemExit):
        service.step(mission)
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 1
        assert s.scalar(text('SELECT count(*) FROM procurement.procurements')) == 0
        assert s.scalar(text('SELECT next_page FROM control.source_partitions WHERE id=:id'),
                        {'id': part['id']}) == 1
        # Simulate the lease deadline after a worker process dies.
        s.execute(text("UPDATE control.source_partitions SET lease_until=clock_timestamp()-interval '1 second' WHERE id=:id"),
                  {'id': part['id']})
    restarted = HistoryBackfill(store, send, allow_synthetic=True)
    assert restarted.step(mission)['state'] == 'complete'
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM bronze.pncp_responses')) == 2
        assert s.scalar(text("SELECT count(*) FROM procurement.procurements WHERE source_id='crash-a'")) == 1
        assert s.scalar(text('SELECT count(*) FROM control.source_partition_identities WHERE partition_id=:id'),
                        {'id': part['id']}) == 1
    assert restarted.status(mission)['attempts_consumed'] == 2
