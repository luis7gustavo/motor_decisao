"""The dry plan must reject values that the live source gate would reject."""

from copy import deepcopy

import pytest

import app.procurement.history as history_module
from app.procurement.history import HistoryBackfill
from app.procurement.pncp import RESOURCE
from app.procurement.source_policy import load_source_policy
from scripts.radar_pncp_history import policy_plan


def _ready_policy():
    policy = deepcopy(load_source_policy('config/contracts/pncp-history-mission-policy.json'))
    policy['enabled'] = True
    history = policy['history']
    history['coverage_start'] = '2026-10-13'
    history['coverage_start_status'] = 'verified'
    history['modality_catalog'] = [6]
    history['modality_catalog_status'] = 'verified_accessible'
    history['fixed_end_filter_semantics_validated'] = True
    policy['resource_gates']['publication'] = 'enabled'
    policy['resource_gates']['global_update'] = 'enabled'
    budget = policy['mission_budget']
    budget.pop('adaptive_rate_test', None)
    budget.pop('test_window_start_utc', None)
    budget.pop('test_window_end_utc', None)
    budget['effective_restriction_scope'] = 'test-public-read'
    budget['stable_budget_scope_key'] = 'pncp:test-public-read'
    budget['external_attempts_authorized_now'] = 12
    budget['shared_source_budget_total_cap'] = 12
    return policy


@pytest.mark.parametrize(('field', 'value', 'gate'), [
    ('effective_min_interval_ms', 1, 'effective_min_interval_ms_unresolved'),
    ('effective_min_interval_ms', True, 'effective_min_interval_ms_unresolved'),
    ('effective_max_concurrency', 2, 'effective_max_concurrency_unresolved'),
    ('request_timeout_seconds', 120, 'request_timeout_unresolved'),
    ('external_attempts_authorized_now', '12', 'attempt_budget_zero'),
    ('external_attempts_authorized_now', True, 'attempt_budget_zero'),
    ('shared_source_budget_total_cap', None, 'attempt_budget_zero'),
    ('shared_source_budget_total_cap', 11, 'attempt_budget_zero'),
    ('daily_attempt_limit', 0, 'daily_attempt_limit_unresolved'),
    ('daily_attempt_limit', True, 'daily_attempt_limit_unresolved'),
])
def test_dry_plan_rejects_unusable_source_limits(field, value, gate):
    policy = _ready_policy()
    assert policy_plan(policy)['ready']
    policy['mission_budget'][field] = value
    result = policy_plan(policy)
    assert not result['ready']
    assert gate in result['gates']


def test_dry_plan_rejects_unstable_scope_key():
    policy = _ready_policy()
    policy['mission_budget']['stable_budget_scope_key'] = '  '
    result = policy_plan(policy)
    assert not result['ready']
    assert 'stable_budget_scope_key_unresolved' in result['gates']


def test_accessible_coverage_start_can_satisfy_gate():
    policy = _ready_policy()
    policy['history']['coverage_start_status'] = 'verified_accessible'
    assert policy_plan(policy)['ready']


def test_accessible_scope_matches_live_gate(monkeypatch):
    policy = _ready_policy()
    policy['history']['coverage_start_status'] = 'verified_accessible'
    monkeypatch.setattr(history_module, 'load_source_policy', lambda _: policy)
    monkeypatch.setattr(history_module, 'require_source_hold', lambda *_: None)
    HistoryBackfill(None, None)._live_gate(resource=RESOURCE)
    policy['history']['modality_catalog_status'] = 'pending'
    with pytest.raises(ValueError, match='Historical source network gate is blocked'):
        HistoryBackfill(None, None)._live_gate(resource=RESOURCE)


def test_live_gate_requires_root_consumer_hold(monkeypatch, tmp_path):
    policy = _ready_policy()
    monkeypatch.setattr(history_module, 'load_source_policy', lambda _: policy)
    monkeypatch.setattr(history_module, 'HOLD_PATH', tmp_path / 'absent.lock')
    with pytest.raises(ValueError, match='PNCP source hold is absent'):
        HistoryBackfill(None, None)._live_gate(resource=RESOURCE)
