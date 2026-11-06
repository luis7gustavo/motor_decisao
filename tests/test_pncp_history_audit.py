"""The scheduled batch always leaves a local, conservative coverage audit."""
from types import SimpleNamespace
from unittest.mock import Mock

from pipelines.procurement import history_flow
from scripts.radar_pncp_history import audit_summary


def test_audit_summary_keeps_gaps_visible():
    complete = [{'state': 'complete'}]
    assert audit_summary(complete, [])['coverage_status'] == 'pagination_consistent_unverified'
    assert audit_summary(complete, ['documents'])['coverage_status'] == 'partial'
    assert audit_summary(complete + [{'state': 'reconcile'}], [])['partition_states'] == {
        'complete': 1, 'reconcile': 1}
    assert audit_summary([], [])['coverage_status'] == 'not_started'


def test_audit_summary_separates_fixed_history_from_recurring_work():
    partitions = [{'state': 'complete', 'pass_no': 1},
                  {'state': 'reconcile', 'pass_no': 2},
                  {'state': 'pending', 'pass_no': 110000},
                  {'state': 'complete', 'pass_no': 1010000}]
    summary = audit_summary(partitions, [])
    assert summary['historical_partition_states'] == {'complete': 1, 'reconcile': 1}
    assert summary['recurring_partition_states'] == {'pending': 1, 'complete': 1}
    assert summary['historical_coverage_status'] == 'partial'
    assert summary['coverage_status'] == 'partial'
    assert audit_summary([partitions[0], partitions[2]], [])['historical_coverage_status'] == (
        'pagination_consistent_unverified')


def test_flow_uses_summary_audit(monkeypatch):
    calls = []
    def fake_history_main(argv):
        calls.append(argv)
        print('{"coverage_status":"partial"}')
        return 0
    monkeypatch.setattr(history_flow, 'history_main', fake_history_main)
    assert history_flow._invoke('audit', 1)['exit_code'] == 0
    assert calls == [['audit', '--steps', '1', '--summary-only']]


def test_scheduled_partial_batch_still_runs_read_only_audit(monkeypatch):
    invoked = []
    monkeypatch.setattr(history_flow, 'load_source_policy', lambda _: {})
    monkeypatch.setattr(history_flow, 'policy_plan', lambda _: {'ready': True, 'gates': []})
    monkeypatch.setattr(history_flow, 'get_run_logger', lambda: Mock())
    monkeypatch.setattr(history_flow, 'get_run_context', lambda: SimpleNamespace(
        flow_run=SimpleNamespace(expected_start_time=None)))
    monkeypatch.setattr(history_flow, '_record', lambda result: result)

    def invoke(command, steps, resource=None):
        invoked.append((command, steps, resource))
        if command == 'audit':
            return {'command': command, 'exit_code': 0,
                    'result': {'coverage_status': 'partial', 'partition_count': 2}}
        return {'command': command, 'exit_code': 3, 'result': {'state': 'deferred'}}

    monkeypatch.setattr(history_flow, '_invoke', invoke)
    result = history_flow.run_history.fn(mode='sync')
    assert [command for command, _, _ in invoked] == ['backfill', 'backfill', 'audit']
    assert invoked[0][2] == 'publication'
    assert invoked[1][2] == 'update'
    assert result['business_outcome'] == 'partial_or_deferred'
    assert result['audit']['result']['coverage_status'] == 'partial'


def test_scheduled_backfill_visits_publication_and_update(monkeypatch):
    invoked = []
    monkeypatch.setattr(history_flow, 'load_source_policy', lambda _: {})
    monkeypatch.setattr(history_flow, 'policy_plan', lambda _: {'ready': True, 'gates': []})
    monkeypatch.setattr(history_flow, 'get_run_logger', lambda: Mock())
    monkeypatch.setattr(history_flow, 'get_run_context', lambda: SimpleNamespace(
        flow_run=SimpleNamespace(expected_start_time=None)))
    monkeypatch.setattr(history_flow, '_record', lambda result: result)
    def invoke(command, steps, resource=None):
        invoked.append((command, steps, resource))
        return {'command': command, 'exit_code': 0,
                'result': {'coverage_status': 'partial'}}
    monkeypatch.setattr(history_flow, '_invoke', invoke)
    result = history_flow.run_history.fn(mode='backfill')
    assert [(command, resource) for command, _, resource in invoked] == [
        ('backfill', 'publication'), ('backfill', 'update'), ('audit', None)]
    assert result['business_outcome'] == 'batch_completed'


def test_shared_prefect_worker_cannot_send_history_requests(monkeypatch):
    monkeypatch.setenv('PREFECT_API_URL', 'http://prefect-server:4200/api')
    monkeypatch.setattr(history_flow, 'get_run_logger', Mock())
    monkeypatch.setattr(history_flow, '_record', lambda result: result)
    monkeypatch.setattr(
        history_flow, '_invoke',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('network path')),
    )

    result = history_flow.run_history.fn(mode='backfill')

    assert result['business_outcome'] == 'blocked_runtime_control_plane'
    assert result['network_calls_when_blocked'] == 0
