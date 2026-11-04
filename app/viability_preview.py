"""Pure, synthetic viability calculation for a complete judgment unit.

No database, PNCP, historical price, or external side effect belongs here.
The caller must supply current evidence and a complete unit before a scenario
can be classified as viable for *human* prioritization.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path


CENT = Decimal("0.01")
FORMULA_VERSION = "offline-v0.1"
REQUIRED_COST_KINDS = frozenset({"variable", "fixed", "finance"})


@dataclass(frozen=True)
class Item:
    key: str
    quantity: Decimal | None
    acquisition_unit: Decimal | None
    sale_unit: Decimal | None
    acquisition_due: date | None
    quote_valid_until: date | None
    compatible: bool | None
    stock_confirmed: bool | None
    source_version: int


@dataclass(frozen=True)
class Cost:
    kind: str
    amount: Decimal | None
    due: date | None
    zero_reason: str | None = None


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    source_version: int | None
    source_fresh: bool | None
    participation_deadline: date | None
    required_item_keys: tuple[str, ...]
    items: tuple[Item, ...]
    logistics_feasible: bool | None
    obligations_resolved: bool | None
    costs: tuple[Cost, ...]
    tax_rate: Decimal | None
    tax_due: date | None
    receipt_due: date | None
    finance_valid_through: date | None
    risk_reserve: Decimal | None
    liquidity_reserve: Decimal | None
    guarantee: Decimal
    guarantee_due: date | None
    guarantee_return_due: date | None
    available_capital: Decimal | None
    minimum_margin: Decimal | None


@dataclass(frozen=True)
class Evaluation:
    scenario_id: str
    as_of_date: date
    snapshot_hash: str
    formula_version: str
    status: str
    reasons: tuple[str, ...]
    revenue: Decimal | None
    variable_cost: Decimal | None
    fixed_cost: Decimal | None
    finance_cost: Decimal | None
    tax: Decimal | None
    risk_reserve: Decimal | None
    profit: Decimal | None
    margin: Decimal | None
    base_capital_need: Decimal | None
    capital_need: Decimal | None
    exposure_days: int | None


def load_synthetic_fixture(path: Path) -> tuple[Scenario, date]:
    """Load versioned demo input; decimal values must be JSON strings."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload["scenario"]

    def number(value):
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("Fixture decimals must be strings, never JSON floats")
        return Decimal(value)

    def day(value):
        return date.fromisoformat(value) if value is not None else None

    items = tuple(Item(
        key=row["key"], quantity=number(row["quantity"]),
        acquisition_unit=number(row["acquisition_unit"]), sale_unit=number(row["sale_unit"]),
        acquisition_due=day(row["acquisition_due"]), quote_valid_until=day(row["quote_valid_until"]),
        compatible=row["compatible"], stock_confirmed=row["stock_confirmed"],
        source_version=row["source_version"],
    ) for row in raw["items"])
    costs = tuple(Cost(
        kind=row["kind"], amount=number(row["amount"]), due=day(row["due"]),
        zero_reason=row.get("zero_reason"),
    ) for row in raw["costs"])
    scenario = Scenario(
        scenario_id=raw["scenario_id"], source_version=raw["source_version"],
        source_fresh=raw["source_fresh"], participation_deadline=day(raw["participation_deadline"]),
        required_item_keys=tuple(raw["required_item_keys"]), items=items,
        logistics_feasible=raw["logistics_feasible"],
        obligations_resolved=raw["obligations_resolved"], costs=costs,
        tax_rate=number(raw["tax_rate"]), tax_due=day(raw["tax_due"]),
        receipt_due=day(raw["receipt_due"]), finance_valid_through=day(raw["finance_valid_through"]),
        risk_reserve=number(raw["risk_reserve"]), liquidity_reserve=number(raw["liquidity_reserve"]),
        guarantee=number(raw["guarantee"]), guarantee_due=day(raw["guarantee_due"]),
        guarantee_return_due=day(raw["guarantee_return_due"]),
        available_capital=number(raw["available_capital"]),
        minimum_margin=number(raw["minimum_margin"]),
    )
    return scenario, date.fromisoformat(payload["as_of_date"])


def _decimal(value: Decimal | None, *, allow_one: bool = True) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("Monetary inputs must be finite Decimal values")
    if value < 0 or value > Decimal("1000000000000") or (not allow_one and value >= 1):
        raise ValueError("Numeric input outside documented bounds")
    return value


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _snapshot_hash(scenario: Scenario, today: date) -> str:
    body = json.dumps({"scenario": asdict(scenario), "as_of_date": today},
                      sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def evaluate(scenario: Scenario, *, today: date) -> Evaluation:
    """Evaluate one immutable scenario; known blocks outrank missing evidence."""
    blocked: list[str] = []
    pending: list[str] = []
    if scenario.source_fresh is not True or scenario.source_version is None:
        pending.append("official_source_freshness_or_version_missing")
    if scenario.participation_deadline is None:
        pending.append("participation_deadline_missing")
    elif scenario.participation_deadline < today:
        blocked.append("participation_deadline_expired")
    if scenario.logistics_feasible is False:
        blocked.append("logistics_infeasible")
    elif scenario.logistics_feasible is None:
        pending.append("logistics_unverified")
    if scenario.obligations_resolved is False:
        blocked.append("obligation_infeasible")
    elif scenario.obligations_resolved is None:
        pending.append("obligations_unresolved")

    keys = [item.key for item in scenario.items]
    if len(keys) != len(set(keys)) or len(scenario.required_item_keys) != len(set(scenario.required_item_keys)):
        raise ValueError("Duplicate item in judgment unit")
    if not scenario.required_item_keys or set(keys) != set(scenario.required_item_keys):
        pending.append("judgment_unit_incomplete")
    for item in scenario.items:
        if scenario.source_version is not None and item.source_version != scenario.source_version:
            pending.append(f"source_version_changed:{item.key}")
        if item.quote_valid_until is None:
            pending.append(f"quote_validity_missing:{item.key}")
        elif item.quote_valid_until < today:
            blocked.append(f"quote_expired:{item.key}")
        if item.compatible is False:
            blocked.append(f"technical_mismatch:{item.key}")
        elif item.compatible is None:
            pending.append(f"technical_match_missing:{item.key}")
        if item.stock_confirmed is False:
            blocked.append(f"stock_insufficient:{item.key}")
        elif item.stock_confirmed is None:
            pending.append(f"stock_unconfirmed:{item.key}")
        if item.quantity is None or item.acquisition_unit is None or item.sale_unit is None or item.acquisition_due is None:
            pending.append(f"item_financial_input_missing:{item.key}")
        else:
            if _decimal(item.quantity) == 0:
                raise ValueError("Item quantity must be positive")
            _decimal(item.acquisition_unit)
            _decimal(item.sale_unit)
            if any(value > Decimal("1000000") or value.as_tuple().exponent < -6
                   for value in (item.quantity, item.acquisition_unit, item.sale_unit)):
                raise ValueError("Item quantity/price exceeds documented limits")

    cost_kinds = [cost.kind for cost in scenario.costs]
    if len(cost_kinds) != len(set(cost_kinds)) or set(cost_kinds) - REQUIRED_COST_KINDS:
        raise ValueError("Duplicate or unsupported cost kind")
    for kind in sorted(REQUIRED_COST_KINDS - set(cost_kinds)):
        pending.append(f"cost_missing:{kind}")
    for cost in scenario.costs:
        if cost.amount is None or cost.due is None:
            pending.append(f"cost_incomplete:{cost.kind}")
        else:
            _decimal(cost.amount)
            if cost.amount == 0 and not cost.zero_reason:
                pending.append(f"zero_cost_unjustified:{cost.kind}")

    for name in ("tax_rate", "risk_reserve", "liquidity_reserve", "available_capital", "minimum_margin"):
        value = getattr(scenario, name)
        if value is None:
            pending.append(f"{name}_missing")
        else:
            _decimal(value, allow_one=name not in {"tax_rate", "minimum_margin"})
    if scenario.tax_due is None or scenario.receipt_due is None:
        pending.append("cash_dates_missing")
    if scenario.finance_valid_through is None:
        pending.append("finance_terms_validity_missing")
    elif scenario.receipt_due is not None and scenario.receipt_due > scenario.finance_valid_through:
        pending.append("finance_terms_expired_for_receipt")
    _decimal(scenario.guarantee)
    if scenario.guarantee and (scenario.guarantee_due is None or scenario.guarantee_return_due is None):
        pending.append("guarantee_dates_missing")
    if (scenario.guarantee and scenario.guarantee_due is not None
            and scenario.guarantee_return_due is not None
            and scenario.guarantee_return_due < scenario.guarantee_due):
        raise ValueError("Guarantee cannot be returned before deposit")

    financial_fields_ready = not any(
        reason.startswith(("item_financial_input_missing", "cost_missing", "cost_incomplete", "zero_cost_unjustified"))
        or reason in {"tax_rate_missing", "risk_reserve_missing", "liquidity_reserve_missing",
                      "available_capital_missing", "minimum_margin_missing", "cash_dates_missing",
                      "guarantee_dates_missing", "judgment_unit_incomplete"}
        for reason in pending
    )
    values = None
    if financial_fields_ready:
        with localcontext() as context:
            context.prec = 50
            acquisition = sum((_money(item.quantity * item.acquisition_unit) for item in scenario.items), Decimal(0))
            revenue = sum((_money(item.quantity * item.sale_unit) for item in scenario.items), Decimal(0))
            by_kind = {cost.kind: _money(cost.amount) for cost in scenario.costs}
            variable = acquisition + by_kind["variable"]
            fixed = by_kind["fixed"]
            finance = by_kind["finance"]
            tax = _money(revenue * scenario.tax_rate)
            risk_reserve = _money(scenario.risk_reserve)
            liquidity_reserve = _money(scenario.liquidity_reserve)
            guarantee = _money(scenario.guarantee)
            profit = revenue - variable - fixed - finance - tax - risk_reserve
            margin = profit / revenue if revenue > 0 else None

            # On the same day, outputs are processed before inputs.
            events = [(item.acquisition_due, 0, -_money(item.quantity * item.acquisition_unit))
                      for item in scenario.items]
            events.extend((cost.due, 0, -_money(cost.amount)) for cost in scenario.costs)
            events.append((scenario.tax_due, 0, -tax))
            events.append((scenario.receipt_due, 1, revenue))
            if guarantee:
                events.extend(((scenario.guarantee_due, 0, -guarantee),
                               (scenario.guarantee_return_due, 1, guarantee)))
            balance = Decimal(0)
            lowest = Decimal(0)
            for _, _, amount in sorted(events):
                balance += amount
                lowest = min(lowest, balance)
            base_need = -lowest
            capital_need = base_need + liquidity_reserve
            first_outflow = min(
                (day for day, priority, amount in events if priority == 0 and amount < 0),
                default=scenario.receipt_due,
            )
            last_recovery = max(scenario.receipt_due,
                                scenario.guarantee_return_due if guarantee else scenario.receipt_due)
            exposure_days = max(0, (last_recovery - first_outflow).days)
            if margin is None:
                pending.append("margin_not_calculable")
            elif margin < scenario.minimum_margin:
                blocked.append("margin_below_policy")
            if capital_need > scenario.available_capital:
                blocked.append("capital_insufficient")
            values = (revenue, variable, fixed, finance, tax, risk_reserve,
                      profit, margin, base_need, capital_need, exposure_days)

    status = "bloqueada" if blocked else "pendente" if pending else "viável"
    return Evaluation(
        scenario_id=scenario.scenario_id, as_of_date=today,
        snapshot_hash=_snapshot_hash(scenario, today),
        formula_version=FORMULA_VERSION, status=status,
        reasons=tuple(dict.fromkeys(blocked + pending)),
        revenue=values[0] if values else None,
        variable_cost=values[1] if values else None,
        fixed_cost=values[2] if values else None,
        finance_cost=values[3] if values else None,
        tax=values[4] if values else None,
        risk_reserve=values[5] if values else None,
        profit=values[6] if values else None,
        margin=values[7] if values else None,
        base_capital_need=values[8] if values else None,
        capital_need=values[9] if values else None,
        exposure_days=values[10] if values else None,
    )
