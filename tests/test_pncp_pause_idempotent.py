"""Regression for queued runs repeating the finite-window pause."""

from contextlib import contextmanager

import pytest

from app.procurement.source_policy import load_source_policy
from scripts import radar_pncp_s06_activate_history as activation


class Result:
    def __init__(self, value):
        self.value = value

    def mappings(self):
        return self

    def one(self):
        return self.value

    def scalar_one(self):
        return self.value


class FakeSession:
    def __init__(self, store):
        self.store = store

    def execute(self, query, params):
        sql = str(query)
        if "SELECT attempt_limit,attempts_consumed,enabled" in sql:
            return Result(self.store.budget.copy())
        if "SELECT id,attempt_limit,attempts_consumed,state" in sql:
            return Result(self.store.mission.copy())
        if "SELECT count(*) FROM control.source_attempts" in sql:
            return Result(self.store.unmatched)
        if "UPDATE control.source_budgets" in sql:
            self.store.budget["enabled"] = params["enabled"]
            self.store.updates += 1
            return Result(None)
        if "UPDATE control.source_missions" in sql:
            self.store.mission["state"] = params["state"]
            self.store.updates += 1
            return Result(None)
        raise AssertionError(f"unexpected query: {sql[:80]}")


class FakeStore:
    def __init__(self, policy):
        budget_policy = policy["mission_budget"]
        self.budget = {
            "attempt_limit": budget_policy["shared_source_budget_total_cap"],
            "attempts_consumed": 13512,
            "enabled": True,
        }
        self.mission = {
            "id": "synthetic-mission",
            "attempt_limit": budget_policy["external_attempts_authorized_now"],
            "attempts_consumed": 13413,
            "state": "enabled",
            "budget_scope_key": activation.BUDGET_KEY,
            "contract_hash": policy["contract_sha256"],
            "scope": {
                "daily_attempt_limit": budget_policy["daily_attempt_limit"],
                "daily_timezone": policy["timezone"],
                "test_window_start_utc": budget_policy["test_window_start_utc"],
                "test_window_end_utc": budget_policy["test_window_end_utc"],
            },
        }
        self.unmatched = 0
        self.updates = 0

    @contextmanager
    def transaction(self):
        yield FakeSession(self)


class FakeEngine:
    def dispose(self):
        pass


@pytest.fixture
def isolated_control(monkeypatch):
    policy = load_source_policy("config/contracts/pncp-history-mission-policy.json")
    store = FakeStore(policy)
    monkeypatch.setattr(activation, "require_source_hold", lambda *_: None)
    monkeypatch.setattr(activation, "target_store", lambda: (store, FakeEngine()))
    return store


def test_repeated_pause_is_successful_noop_after_first_transition(isolated_control):
    activation.set_running(False)
    activation.set_running(False)

    assert isolated_control.budget["enabled"] is False
    assert isolated_control.mission["state"] == "paused"
    assert isolated_control.updates == 2  # The second call must not write.


def test_flow_pause_called_twice_keeps_deployment_first(monkeypatch, isolated_control):
    from pipelines.procurement.history_flow import _pause_finite_test
    from scripts import radar_pncp_schedule as schedule

    calls = []

    async def fake_set_pause(config, delete, paused):
        calls.append((config, delete, paused, isolated_control.budget["enabled"]))

    monkeypatch.setattr(schedule, "set_pause", fake_set_pause)
    config = {"synthetic": True}
    _pause_finite_test(config)
    _pause_finite_test(config)

    assert calls == [(config, False, True, True), (config, False, True, False)]
    assert isolated_control.updates == 2


def test_repeated_pause_still_rejects_ledger_drift(isolated_control):
    activation.set_running(False)
    isolated_control.unmatched = 1

    with pytest.raises(ValueError, match="History source state or ledger drift"):
        activation.set_running(False)


def test_repeated_pause_rejects_half_paused_state(isolated_control):
    activation.set_running(False)
    isolated_control.mission["state"] = "enabled"

    with pytest.raises(ValueError, match="History source state or ledger drift"):
        activation.set_running(False)
