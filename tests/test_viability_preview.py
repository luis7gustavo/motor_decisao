from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.viability_preview import evaluate, load_synthetic_fixture


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "viability_base.json"


def base():
    return load_synthetic_fixture(FIXTURE)


def test_complete_lot_uses_all_items_and_reconciles_cash():
    scenario, today = base()
    result = evaluate(scenario, today=today)
    assert result.status == "viável"
    assert result.reasons == ()
    assert result.revenue == Decimal("13300.00")
    assert result.variable_cost == Decimal("10300.00")
    assert result.tax == Decimal("798.00")
    assert result.profit == Decimal("1622.00")
    assert result.revenue - result.variable_cost - result.fixed_cost - result.finance_cost - result.tax - result.risk_reserve == result.profit
    assert result.base_capital_need == Decimal("11978.00")
    assert result.capital_need == Decimal("12178.00")
    assert result.exposure_days == 75  # Guarantee remains tied up after receipt.
    assert result.snapshot_hash == evaluate(scenario, today=today).snapshot_hash
    assert result.snapshot_hash != evaluate(scenario, today=date(2026, 11, 5)).snapshot_hash
    assert evaluate(replace(scenario, items=scenario.items[:1]), today=today).status == "pendente"


@pytest.mark.parametrize("change, reason, expected", [
    ("expired_quote", "quote_expired:B", "bloqueada"),
    ("mismatch", "technical_mismatch:B", "bloqueada"),
    ("retification", "source_version_changed:A", "pendente"),
    ("low_capital", "capital_insufficient", "bloqueada"),
    ("missing_freight", "cost_incomplete:variable", "pendente"),
])
def test_guards_precede_positive_result(change, reason, expected):
    scenario, today = base()
    if change == "expired_quote":
        items = scenario.items[:1] + (replace(scenario.items[1], quote_valid_until=date(2026, 11, 3)),)
        scenario = replace(scenario, items=items)
    elif change == "mismatch":
        scenario = replace(scenario, items=scenario.items[:1] + (replace(scenario.items[1], compatible=False),))
    elif change == "retification":
        scenario = replace(scenario, source_version=3)
    elif change == "low_capital":
        scenario = replace(scenario, available_capital=Decimal("10000"))
    else:
        scenario = replace(scenario, costs=(replace(scenario.costs[0], amount=None),) + scenario.costs[1:])
    result = evaluate(scenario, today=today)
    assert result.status == expected
    assert reason in result.reasons
    if change == "missing_freight":
        assert result.profit is None and result.margin is None


def test_delayed_receipt_needs_new_finance_terms_and_cannot_improve_exposure():
    scenario, today = base()
    initial = evaluate(scenario, today=today)
    delayed = evaluate(replace(scenario, receipt_due=date(2027, 2, 2)), today=today)
    assert delayed.status == "pendente"
    assert "finance_terms_expired_for_receipt" in delayed.reasons
    assert delayed.capital_need >= initial.capital_need
    assert delayed.exposure_days > initial.exposure_days


def test_invalid_float_and_unjustified_zero_do_not_become_viable():
    scenario, today = base()
    with pytest.raises(ValueError):
        evaluate(replace(scenario, tax_rate=0.06), today=today)
    zero = evaluate(replace(scenario, costs=(replace(scenario.costs[0], amount=Decimal("0")),) + scenario.costs[1:]), today=today)
    assert zero.status == "pendente"
    assert "zero_cost_unjustified:variable" in zero.reasons
    with pytest.raises(ValueError):
        evaluate(replace(scenario, guarantee_return_due=date(2026, 11, 3)), today=today)
    with pytest.raises(ValueError):
        evaluate(replace(scenario, items=(replace(scenario.items[0], quantity=Decimal("1.1234567")),) + scenario.items[1:]), today=today)
