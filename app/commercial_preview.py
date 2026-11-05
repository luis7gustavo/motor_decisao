"""Synthetic, in-memory commercial journey with human events only."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.viability_preview import Scenario, evaluate


ZERO = Decimal("0")
FACT_EVENTS = frozenset({
    "participation_recorded", "result_recorded", "winner_recorded",
    "contract_started", "delivery_recorded", "delivery_accepted",
    "receipt_recorded", "receipt_reconciled",
})


@dataclass(frozen=True)
class Event:
    event_id: str
    kind: str
    actor: str
    occurred_at: datetime
    source_version: int
    reference: str = ""
    reason: str = ""
    snapshot_hash: str = ""
    viability_status: str = ""
    item_key: str = ""
    quantity: Decimal = ZERO
    amount: Decimal = ZERO
    contract_items: tuple[tuple[str, Decimal], ...] = ()


@dataclass(frozen=True)
class Case:
    opportunity_id: str
    source_version: int
    required_items: tuple[tuple[str, Decimal], ...]
    amount_due: Decimal
    state: str = "E04"
    review_pending: bool = True
    approved_snapshot_hash: str = ""
    delivered_items: tuple[tuple[str, Decimal], ...] = ()
    received_amount: Decimal = ZERO
    contract_locked: bool = False
    events: tuple[Event, ...] = ()


def apply_event(case: Case, event: Event, *, review_scenario: Scenario | None = None) -> Case:
    """Return a new case; reject invalid transitions without changing the input."""
    requirements = dict(case.required_items)
    if (not case.opportunity_id or not requirements or len(requirements) != len(case.required_items)
            or any(not key or quantity <= ZERO for key, quantity in case.required_items)
            or case.amount_due <= ZERO):
        raise ValueError("case requires a complete synthetic judgment unit")
    if not event.event_id or not event.actor or event.occurred_at.tzinfo is None:
        raise ValueError("event requires identity, actor and timezone")
    if event.quantity < ZERO or event.amount < ZERO:
        raise ValueError("negative quantity or amount")
    for previous in case.events:
        if previous.event_id == event.event_id:
            if previous == event:
                return case
            raise ValueError("conflicting event id")
    if case.events and event.occurred_at < case.events[-1].occurred_at:
        raise ValueError("event time goes backwards")
    if event.kind in FACT_EVENTS and not event.reference:
        raise ValueError("external fact requires evidence reference")

    current = case
    if event.kind == "review_approved":
        if case.state != "E04" and not (case.review_pending and case.state in {"E08", "E10", "E11", "E13", "E14", "E15"}):
            raise ValueError("review is not pending")
        if not event.reason or review_scenario is None:
            raise ValueError("favorable review requires evaluated scenario and human reason")
        if review_scenario.scenario_id != case.opportunity_id or review_scenario.source_version != case.source_version:
            raise ValueError("review uses outdated source version or opportunity")
        review_day = event.occurred_at.astimezone(ZoneInfo("America/Sao_Paulo")).date()
        evaluation = evaluate(review_scenario, today=review_day)
        if (evaluation.status != "viável" or event.viability_status != evaluation.status
                or event.snapshot_hash != evaluation.snapshot_hash or evaluation.revenue is None):
            raise ValueError("favorable review requires viable snapshot and human reason")
        if event.source_version != case.source_version:
            raise ValueError("review uses outdated source version")
        projected = tuple((item.key, item.quantity) for item in review_scenario.items)
        if (not projected or any(key not in review_scenario.required_item_keys or quantity is None or quantity <= ZERO
                                 for key, quantity in projected)
                or set(key for key, _ in projected) != set(review_scenario.required_item_keys)):
            raise ValueError("review requires complete judgment unit")
        if case.contract_locked and (dict(projected) != dict(case.required_items) or evaluation.revenue != case.amount_due):
            raise ValueError("new analysis conflicts with evidenced contract terms")
        current = replace(case, state="E07" if case.state == "E04" else case.state,
                          review_pending=False, approved_snapshot_hash=event.snapshot_hash,
                          required_items=case.required_items if case.contract_locked else projected,
                          amount_due=case.amount_due if case.contract_locked else evaluation.revenue)
    elif event.kind == "discarded":
        if case.state not in {"E04", "E07"} or not event.reason:
            raise ValueError("discard requires eligible state and reason")
        current = replace(case, state="E06", review_pending=False)
    elif event.kind == "participation_recorded":
        if case.state != "E07" or case.review_pending or not case.approved_snapshot_hash:
            raise ValueError("participation requires current human approval")
        if event.source_version != case.source_version:
            raise ValueError("participation uses outdated source version")
        current = replace(case, state="E08")
    elif event.kind == "source_changed":
        if case.state not in {"E04", "E07", "E08", "E10", "E11", "E13", "E14", "E15"}:
            raise ValueError("source change requires an active case")
        if event.source_version <= case.source_version or not event.reference:
            raise ValueError("source change requires newer evidenced version")
        current = replace(case, source_version=event.source_version, review_pending=True,
                          state="E04" if case.state == "E07" else case.state)
    elif event.kind == "result_recorded":
        if case.state != "E08":
            raise ValueError("result requires recorded participation")
        current = replace(case, state="E10")
    elif event.kind == "winner_recorded":
        if case.state != "E10":
            raise ValueError("winner requires evidenced result")
        current = replace(case, state="E11")
    elif event.kind == "contract_started":
        if case.state != "E11" or case.review_pending:
            raise ValueError("contract requires winner and separate evidence")
        contract_items = dict(event.contract_items)
        if (len(contract_items) != len(event.contract_items)
                or any(not key or quantity <= ZERO for key, quantity in event.contract_items)
                or contract_items != dict(case.required_items) or event.amount != case.amount_due):
            raise ValueError("contract terms require complete reviewed unit and value")
        current = replace(case, state="E13", contract_locked=True)
    elif event.kind == "delivery_recorded":
        if case.state != "E13" or event.quantity <= ZERO or event.item_key not in requirements:
            raise ValueError("delivery requires execution and positive quantity")
        delivered = dict(case.delivered_items)
        delivered[event.item_key] = delivered.get(event.item_key, ZERO) + event.quantity
        if delivered[event.item_key] > requirements[event.item_key]:
            raise ValueError("delivery exceeds complete unit")
        current = replace(case, delivered_items=tuple(sorted(delivered.items())))
    elif event.kind == "delivery_accepted":
        if case.state != "E13" or dict(case.delivered_items) != requirements:
            raise ValueError("acceptance requires complete delivery")
        current = replace(case, state="E14")
    elif event.kind == "receipt_recorded":
        if case.state not in {"E13", "E14"} or event.amount <= ZERO:
            raise ValueError("receipt requires execution and positive amount")
        received = case.received_amount + event.amount
        if received > case.amount_due:
            raise ValueError("receipt exceeds amount due")
        current = replace(case, received_amount=received)
    elif event.kind == "receipt_reconciled":
        if case.state != "E14" or case.received_amount != case.amount_due:
            raise ValueError("reconciliation requires accepted delivery and full receipt")
        current = replace(case, state="E15")
    elif event.kind == "closed":
        if case.state != "E15" or case.review_pending or not event.reason:
            raise ValueError("normal closure requires reconciled receipt and reason")
        current = replace(case, state="E16")
    else:
        raise ValueError(f"unsupported event: {event.kind}")
    return replace(current, events=case.events + (event,))
