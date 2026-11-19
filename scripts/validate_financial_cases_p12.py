"""Check proposed P12 synthetic examples without touching product or data services."""

import json
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


CENT = Decimal("0.01")
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "radar_finance_p12" / "cases.json"
FINANCIAL_FIELDS = (
    "revenue", "acquisition", "costs", "profit",
    "base_capital_need", "capital_need", "ending_cash",
)


def decimal_text(value):
    if not isinstance(value, str):
        raise ValueError("Decimal input must be text")
    number = Decimal(value)
    if not number.is_finite() or number < 0:
        raise ValueError("Decimal input must be finite and nonnegative")
    return number


def money(value):
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def cash(events):
    balance = Decimal("0")
    peak = Decimal("0")
    for event in sorted(events, key=lambda row: (date.fromisoformat(row["date"]), row["direction"] != "out")):
        amount = decimal_text(event["amount"])
        if amount != money(amount):
            raise ValueError("Cash event is not in cents")
        if event["direction"] == "out":
            balance -= amount
        elif event["direction"] == "in":
            balance += amount
        else:
            raise ValueError("Unknown cash direction")
        peak = max(peak, -balance)
    return money(peak), money(balance)


def _expect(actual, expected, context):
    if actual != decimal_text(expected):
        raise ValueError(f"{context}: expected {expected}, got {actual}")


def validate_case(case):
    context = case["id"]
    keys = [row["key"] for row in case["items"]]
    if not keys or len(keys) != len(set(keys)) or set(keys) != set(case["required_item_keys"]):
        raise ValueError(f"{context}: incomplete or duplicate judgment unit")
    cost_ids = [row["id"] for row in case["costs"]]
    if len(cost_ids) != len(set(cost_ids)):
        raise ValueError(f"{context}: duplicate cost")

    reasons = [f"sale_unit_missing:{row['key']}" for row in case["items"] if row["sale_unit"] is None]
    expected = case["expected"]
    if reasons:
        if expected["missing_reasons"] != reasons or any(expected[name] is not None for name in FINANCIAL_FIELDS):
            raise ValueError(f"{context}: missing inputs must propagate to null outputs")
        if case["events"]:
            raise ValueError(f"{context}: incomplete case has cash events")
        return
    if expected["missing_reasons"]:
        raise ValueError(f"{context}: unexpected missing reason")

    revenue = sum((money(decimal_text(row["quantity"]) * decimal_text(row["sale_unit"]))
                   for row in case["items"]), Decimal("0"))
    acquisition = sum((money(decimal_text(row["quantity"]) * decimal_text(row["acquisition_unit"]))
                       for row in case["items"]), Decimal("0"))
    costs = sum((decimal_text(row["amount"]) for row in case["costs"]), Decimal("0"))
    risk = decimal_text(case["risk_reserve"])
    liquidity = decimal_text(case["liquidity_reserve"])
    profit = revenue - acquisition - costs - risk
    events = case["events"]
    by_kind = lambda kind: sum((decimal_text(row["amount"]) for row in events if row["kind"] == kind), Decimal("0"))
    if by_kind("receipt") != revenue or by_kind("acquisition") != acquisition:
        raise ValueError(f"{context}: receipts/acquisition disagree with items")
    if by_kind("guarantee_deposit") != by_kind("guarantee_return"):
        raise ValueError(f"{context}: refundable guarantee does not balance")
    event_costs = [row for row in events if row["kind"] == "cost"]
    if len(event_costs) != len(case["costs"]) or {row.get("cost_id") for row in event_costs} != set(cost_ids):
        raise ValueError(f"{context}: cost event missing or repeated")
    for row in event_costs:
        cost = next(cost for cost in case["costs"] if cost["id"] == row["cost_id"])
        if decimal_text(row["amount"]) != decimal_text(cost["amount"]):
            raise ValueError(f"{context}: cost amount differs from event")
    if any(row["kind"] not in {"receipt", "acquisition", "cost", "guarantee_deposit", "guarantee_return"}
           for row in events):
        raise ValueError(f"{context}: unknown event kind")
    peak, ending = cash(events)
    actual = {
        "revenue": revenue, "acquisition": acquisition, "costs": costs,
        "profit": profit, "base_capital_need": peak,
        "capital_need": peak + liquidity, "ending_cash": ending,
    }
    for name, value in actual.items():
        _expect(value, expected[name], f"{context}.{name}")
    if ending != profit + risk:
        raise ValueError(f"{context}: economic/cash reconciliation failed")


def validate(payload):
    if (payload["schema_version"], payload["evidence_class"], payload["origin"], payload["currency"]) != (
        "p12-e2-v1", "E2", "synthetic_manual", "BRL"
    ):
        raise ValueError("Unexpected fixture origin or version")
    cases = payload["cases"]
    ids = [case["id"] for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate case ID")
    for case in cases:
        validate_case(case)
    portfolio = payload["portfolio"]
    selected = [case for case in cases if case["id"] in portfolio["scenario_ids"]]
    if len(selected) != len(portfolio["scenario_ids"]):
        raise ValueError("Portfolio references an unknown scenario")
    events = [event for case in selected for event in case["events"]]
    peak, ending = cash(events)
    liquidity = sum((decimal_text(case["liquidity_reserve"]) for case in selected), Decimal("0"))
    for name, actual in (
        ("base_capital_need", peak), ("liquidity_reserve", liquidity),
        ("capital_need", peak + liquidity), ("ending_cash", ending)
    ):
        _expect(actual, portfolio["expected"][name], f"portfolio.{name}")
    return len(cases), len(selected)


def main():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"),
                         parse_float=lambda _: (_ for _ in ()).throw(ValueError("JSON floats forbidden")))
    cases, portfolio_cases = validate(payload)
    print(f"P12 synthetic fixture valid: {cases} cases, {portfolio_cases} portfolio scenarios")


if __name__ == "__main__":
    main()
