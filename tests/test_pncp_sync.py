"""S04 offline sync/reconciliation: only synthetic responses and isolated DB."""

from datetime import date, datetime, timezone
import json
import os
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from app.procurement.history import HistoryBackfill, RESOURCE, UPDATE_RESOURCE, job_uuid
from app.procurement.enrichment import EnrichmentQueue
from app.procurement.pncp import Store
from app.procurement.pncp_transport import Response
from app.procurement.sync import SyncCoordinator


def record(identity='old-bid', *, state='Aberta', updated='2026-01-01T12:00:00+00:00'):
    return {'numeroControlePNCP': identity, 'objetoCompra': 'Synthetic old procurement',
            'orgaoEntidade': {'cnpj': '00000000000000'},
            'dataAtualizacaoGlobal': updated, 'situacaoCompraNome': state,
            'dataPublicacaoPncp': '2026-01-01T09:00:00+00:00'}


def page(number, records, total=None, pages=1):
    total = len(records) if total is None else total
    return Response(200, {'content-type': 'application/json'}, json.dumps({
        'data': records, 'numeroPagina': number, 'totalRegistros': total,
        'totalPaginas': pages, 'paginasRestantes': pages - number,
        'empty': not records}).encode())


def query(url):
    return urlsplit(url).path, parse_qs(urlsplit(url).query)


@pytest.fixture
def store():
    engine = create_engine(os.environ['DATABASE_URL'])
    isolated = Store(engine, expected_database=os.environ['RADAR_TEST_DB'])
    yield isolated
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


def mission(worker, *, end=date(2026, 1, 2), limit=50):
    key = uuid4().hex
    planned = worker.plan(mission_key='synthetic-sync-' + key,
        budget_scope_key=key, restriction_scope='test-ip-' + key,
        contract_hash='a' * 64, coverage_start=date(2026, 1, 1),
        fixed_end=end, modalities=[6], window_days=2, attempt_limit=limit,
        enabled=True, page_size=10)
    return planned['mission']['id'], planned


def cycle():
    return datetime(2026, 1, 6, 12, tzinfo=timezone.utc)


def test_old_procurement_updated_and_cancelled_with_independent_watermarks(store):
    calls = []
    def send(url):
        path, params = query(url)
        calls.append((path, params))
        if path.endswith(RESOURCE) and params['dataInicial'] == ['20260101']:
            return page(1, [record()])
        if path.endswith(UPDATE_RESOURCE) and params['dataInicial'] == ['20260105']:
            return page(1, [record(state='Cancelada', updated='2026-01-05T13:00:00+00:00')])
        return Response(204, {}, b'')
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, planned = mission(worker)
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    sync_plan = coordinator.plan_sync(mission_id, cycle_at=cycle())
    assert len(sync_plan['partitions']) == 6 and not sync_plan['gaps']
    assert coordinator.plan_sync(mission_id, cycle_at=cycle())['cycle_pass'] == sync_plan['cycle_pass']
    assert all(r['state'] == 'complete' for r in coordinator.run_sync(mission_id, max_pages=6))
    assert coordinator._watermark(mission_id, RESOURCE, 6) == date(2026, 1, 5)
    assert coordinator._watermark(mission_id, UPDATE_RESOURCE, 6) == date(2026, 1, 5)
    with store.transaction() as s:
        current = s.execute(text("""SELECT v.official_status FROM procurement.procurements p
            JOIN procurement.procurement_versions v ON v.id=p.current_version_id
            WHERE p.source_id='old-bid'""")).scalar_one()
        assert current == 'Cancelada'
        assert s.scalar(text("SELECT count(*) FROM procurement.procurement_versions")) == 2
        assert s.scalar(text("SELECT count(*) FROM procurement.version_observations")) == 2
        original_raw = s.execute(text("""SELECT r.id FROM bronze.pncp_responses r
            JOIN control.ingestion_jobs j ON j.id=r.job_id
            WHERE j.scope_key=:key AND r.http_status=200"""),
            {'key': planned['partitions'][0]['scope_key']}).scalar_one()
        attempts = s.scalar(text('SELECT count(*) FROM control.source_attempts'))
    assert coordinator.replay_raw(mission_id, original_raw)['network_calls'] == 0
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == attempts
        assert s.execute(text("""SELECT v.official_status FROM procurement.procurements p
            JOIN procurement.procurement_versions v ON v.id=p.current_version_id
            WHERE p.source_id='old-bid'""")).scalar_one() == 'Cancelada'
    assert any(path.endswith(UPDATE_RESOURCE) for path, _ in calls)


def test_older_failed_day_blocks_watermark_even_when_later_days_succeed(store):
    def send(url):
        _, params = query(url)
        if params['dataInicial'] == ['20260103']:
            return Response(400, {}, b'bad request')
        return Response(204, {}, b'')
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, _ = mission(worker)
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    coordinator.plan_sync(mission_id, cycle_at=cycle(), resources=(RESOURCE,))
    assert worker.step(mission_id, priority_ceiling=39)['state'] == 'partial'
    assert worker.step(mission_id, priority_ceiling=39)['state'] == 'complete'
    assert worker.step(mission_id, priority_ceiling=39)['state'] == 'complete'
    assert coordinator.advance_watermark(mission_id, resource=RESOURCE, modality=6) == date(2026, 1, 2)
    assert coordinator._watermark(mission_id, RESOURCE, 6) == date(2026, 1, 2)
    audit = worker.audit(mission_id)
    assert any(p['start'] == '2026-01-03' and p['state'] == 'partial' for p in audit)
    assert sum(p['state'] == 'complete' for p in audit) == 3


def test_empty_update_does_not_erase_old_record(store):
    def send(url):
        path, params = query(url)
        if path.endswith(RESOURCE) and params['dataInicial'] == ['20260101']:
            return page(1, [record()])
        return Response(204, {}, b'')
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, _ = mission(worker)
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    coordinator.plan_sync(mission_id, cycle_at=cycle(), resources=(UPDATE_RESOURCE,))
    assert all(r['state'] == 'complete' for r in coordinator.run_sync(mission_id, max_pages=3))
    with store.transaction() as s:
        assert s.scalar(text("SELECT count(*) FROM procurement.procurements WHERE source_id='old-bid'")) == 1
        assert s.scalar(text('SELECT count(*) FROM procurement.procurement_versions')) == 1


def test_same_content_newer_time_then_late_older_change_does_not_regress(store):
    def send(url):
        path, params = query(url)
        day = params['dataInicial'][0]
        if path.endswith(RESOURCE) and day == '20260101':
            return page(1, [record()])
        if path.endswith(UPDATE_RESOURCE) and day == '20260103':
            return page(1, [record(updated='2026-01-03T12:00:00+00:00')])
        if path.endswith(UPDATE_RESOURCE) and day == '20260104':
            return page(1, [record(state='Cancelada', updated='2026-01-02T12:00:00+00:00')])
        return Response(204, {}, b'')
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, _ = mission(worker)
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    coordinator.plan_sync(mission_id, cycle_at=cycle(), resources=(UPDATE_RESOURCE,))
    assert all(r['state'] == 'complete' for r in coordinator.run_sync(mission_id, max_pages=3))
    with store.transaction() as s:
        versions = s.scalar(text('SELECT count(*) FROM procurement.procurement_versions'))
        assert versions == 2  # same content/time change reuses the first business version
        current = s.execute(text("""SELECT v.official_status FROM procurement.procurements p
            JOIN procurement.procurement_versions v ON v.id=p.current_version_id
            WHERE p.source_id='old-bid'""")).scalar_one()
        assert current == 'Aberta'
        statuses = s.execute(text("""SELECT ordering_status,source_updated_at,observed_resource
            FROM procurement.version_observations ORDER BY created_at""")).all()
        assert [r.ordering_status for r in statuses] == ['initial', 'same_content', 'stale_replay']
        assert statuses[1].source_updated_at.date() == date(2026, 1, 3)
        assert statuses[1].observed_resource == UPDATE_RESOURCE


def test_equal_and_naive_source_times_keep_current_value(store):
    def send(url):
        path, params = query(url)
        day = params['dataInicial'][0]
        if path.endswith(RESOURCE) and day == '20260101':
            return page(1, [record()])
        if path.endswith(UPDATE_RESOURCE) and day == '20260103':
            return page(1, [record(state='Cancelada')])
        if path.endswith(UPDATE_RESOURCE) and day == '20260104':
            return page(1, [record(state='Revogada', updated='2026-01-04T13:00:00')])
        return Response(204, {}, b'')
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, _ = mission(worker)
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    coordinator.plan_sync(mission_id, cycle_at=cycle(), resources=(UPDATE_RESOURCE,))
    coordinator.run_sync(mission_id, max_pages=3)
    with store.transaction() as s:
        current = s.execute(text("""SELECT v.official_status FROM procurement.procurements p
            JOIN procurement.procurement_versions v ON v.id=p.current_version_id
            WHERE p.source_id='old-bid'""")).scalar_one()
        assert current == 'Aberta'
        statuses = s.execute(text("""SELECT ordering_status FROM procurement.version_observations
            ORDER BY created_at""")).scalars().all()
        assert statuses == ['initial', 'ambiguous_equal', 'unknown_source_time']


def test_reconciliation_requires_two_identical_child_passes(store):
    def send(url):
        _, params = query(url)
        start, end, number = (params[k][0] for k in ('dataInicial', 'dataFinal', 'pagina'))
        if start == end:
            return page(1, [record('part-a' if start == '20260101' else 'part-b')])
        return page(1, [record('part-a')], 2, 2) if number == '1' else page(2, [record('part-b')], 3, 2)
    worker = HistoryBackfill(store, send, allow_synthetic=True)
    mission_id, planned = mission(worker)
    parent = planned['partitions'][0]
    assert worker.step(mission_id)['state'] == 'page_committed'
    assert worker.step(mission_id)['state'] == 'reconcile'
    assert worker.step(mission_id)['state'] == 'complete'
    assert worker.step(mission_id)['state'] == 'complete'
    coordinator = SyncCoordinator(worker)
    assert coordinator.reconcile_parent(parent['id'])['state'] == 'second_pass_planned'
    assert worker.step(mission_id)['state'] == 'complete'
    assert worker.step(mission_id)['state'] == 'complete'
    assert coordinator.reconcile_parent(parent['id'])['state'] == 'reconciled_sem_snapshot'
    with store.transaction() as s:
        checkpoint = s.execute(text('SELECT checkpoint FROM control.ingestion_jobs WHERE id=:id'),
                               {'id': job_uuid(parent['id'])}).scalar_one()
        assert checkpoint['coverage_status'] == 'reconciled_sem_snapshot'
        assert checkpoint['reconciliation']['absolute_completeness'] is False
    assert worker.audit(mission_id)[0]['state'] == 'complete'


def test_missing_contract_resources_are_explicit_gaps():
    gaps = SyncCoordinator.resource_gaps({'resource_gates': {
        'procurement_detail': 'blocked_current_contract',
        'items': 'blocked_missing_validated_read_contract',
        'results': 'blocked_missing_validated_read_contract',
        'documents': 'blocked_missing_validated_read_contract_and_storage_policy'}})
    assert {r['resource'] for r in gaps} == {'procurement_detail', 'items', 'results', 'documents'}


def test_changed_attachment_retains_both_raw_observations(store):
    worker = HistoryBackfill(store, lambda url: Response(204, {}, b''), allow_synthetic=True)
    mission_id, _ = mission(worker)
    bodies = [b'attachment version A', b'attachment version B']
    calls = []

    def send(url):
        calls.append(url)
        return Response(200, {'content-type': 'application/pdf'}, bodies.pop(0))

    queue = EnrichmentQueue(store, send, allow_synthetic=True)
    url = 'https://pncp.gov.br/api/consulta/v1/contratacoes/attachment'
    for cycle in ('2026-01-06', '2026-01-13'):
        queue.enqueue(mission_id, kind='documents', identity_key='old-bid',
                      url=url, cycle_key=cycle, contract_hash='a' * 64)
    first = queue.step(mission_id)
    second = queue.step(mission_id)
    assert [first['state'], second['state']] == ['raw_collected', 'raw_collected']
    assert not first['domain_complete'] and not second['domain_complete']
    assert len(calls) == 2
    with store.transaction() as s:
        rows = s.execute(text("""SELECT body,body_sha256 FROM bronze.pncp_responses
            WHERE schema_version='pncp-enrich-raw-1' ORDER BY received_at,id""")).all()
        assert [r.body for r in rows] == [b'attachment version A', b'attachment version B']
        assert rows[0].body_sha256 != rows[1].body_sha256
        assert s.scalar(text("SELECT count(*) FROM control.source_attempts WHERE work_kind='enrich'")) == 2
        assert s.scalar(text("SELECT count(*) FROM control.source_queue WHERE state='complete'")) == 2
        assert s.scalar(text('SELECT count(*) FROM procurement.procurements')) == 0


def test_enrichment_budget_reserves_backfill_progress(store):
    worker = HistoryBackfill(store, lambda url: Response(204, {}, b''), allow_synthetic=True)
    mission_id, _ = mission(worker, limit=2)
    calls = []
    def send(url):
        calls.append(url)
        return Response(204, {}, b'')

    queue = EnrichmentQueue(store, send, allow_synthetic=True)
    queue.enqueue(mission_id, kind='items', identity_key='old-bid',
                  url='https://pncp.gov.br/api/consulta/v1/contratacoes/items',
                  cycle_key='one', contract_hash='a' * 64)
    assert queue.step(mission_id)['state'] == 'raw_collected'
    queue.enqueue(mission_id, kind='items', identity_key='old-bid',
                  url='https://pncp.gov.br/api/consulta/v1/contratacoes/items',
                  cycle_key='two', contract_hash='a' * 64)
    assert queue.step(mission_id)['state'] == 'quota_reserved_for_backfill'
    assert len(calls) == 1
    with store.transaction() as s:
        assert s.scalar(text('SELECT count(*) FROM control.source_attempts')) == 1
