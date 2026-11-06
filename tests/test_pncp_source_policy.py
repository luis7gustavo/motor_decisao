import pytest

from app.procurement.source_policy import load_source_policy


def test_live_policy_has_one_operator_grant_and_remains_blocked():
    policy = load_source_policy("config/contracts/pncp-publicacao-policy.json")
    assert policy["enabled"] is False
    assert policy["limits_reviewed"] is False
    assert policy["operator_authorization"]["session"] == "c2-20261013"


def test_history_mission_policy_uses_finite_shared_budget():
    policy = load_source_policy("config/contracts/pncp-history-mission-policy.json")
    budget = policy["mission_budget"]
    assert type(budget["external_attempts_authorized_now"]) is int
    assert budget["external_attempts_authorized_now"] > 0
    assert budget["shared_source_budget_total_cap"] >= budget["external_attempts_authorized_now"]
    assert budget["effective_min_interval_ms"] >= 1000
    assert budget["effective_max_concurrency"] == 1
    assert policy["history"]["coverage_start"] == "2019-01-01"


def test_nested_duplicate_key_is_rejected(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text('{"enabled":false,"grant":{"session":"c2","session":"other"}}')
    with pytest.raises(ValueError, match="Duplicate source policy key: session"):
        load_source_policy(path)


def test_top_level_duplicate_key_is_rejected(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text('{"enabled":false,"enabled":true}')
    with pytest.raises(ValueError, match="Duplicate source policy key: enabled"):
        load_source_policy(path)
