"""Replay a synthetic commercial journey; no external commercial action."""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app.commercial_preview import Case, Event, apply_event
from app.viability_preview import evaluate, load_synthetic_fixture


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo"


def replay() -> Case:
    payload = json.loads((FIXTURES / "commercial_journey.json").read_text(encoding="utf-8"))
    if payload.get("synthetic") is not True:
        raise ValueError("commercial preview accepts only synthetic fixtures")
    scenario, today = load_synthetic_fixture(FIXTURES / "viability_base.json")
    first = evaluate(scenario, today=today)
    if first.status != "viável" or first.revenue is None:
        raise ValueError("base viability fixture is not viable")
    case = Case(
        opportunity_id=scenario.scenario_id,
        source_version=scenario.source_version,
        required_items=tuple((item.key, item.quantity) for item in scenario.items),
        amount_due=first.revenue,
    )
    origin = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)
    for row in payload["events"]:
        version = row["source_version"]
        status = ""
        snapshot = ""
        revised = None
        if row["kind"] == "review_approved":
            revised = replace(scenario, source_version=version,
                              items=tuple(replace(item, source_version=version) for item in scenario.items))
            evaluation = evaluate(revised, today=today)
            status, snapshot = evaluation.status, evaluation.snapshot_hash
        event = Event(
            event_id=row["event_id"], kind=row["kind"], actor="synthetic-operator",
            occurred_at=origin + timedelta(minutes=row["minute"]),
            source_version=version, reference=row.get("reference", ""),
            reason=row.get("reason", ""), snapshot_hash=snapshot,
            viability_status=status, item_key=row.get("item_key", ""),
            quantity=Decimal(row.get("quantity", "0")), amount=Decimal(row.get("amount", "0")),
            contract_items=tuple((key, Decimal(quantity)) for key, quantity in row.get("contract_items", [])),
        )
        case = apply_event(case, event, review_scenario=revised)
    return case


def main() -> None:
    case = replay()
    print(json.dumps({
        "data_kind": "synthetic_commercial_journey",
        "opportunity_id": case.opportunity_id,
        "state": case.state,
        "source_version": case.source_version,
        "review_pending": case.review_pending,
        "required_items": [(key, str(qty)) for key, qty in case.required_items],
        "delivered_items": [(key, str(qty)) for key, qty in case.delivered_items],
        "amount_due": str(case.amount_due),
        "received_amount": str(case.received_amount),
        "events": [{"id": row.event_id, "kind": row.kind, "source_version": row.source_version,
                    "snapshot_hash": row.snapshot_hash or None} for row in case.events],
        "warning": "All facts and evidence are invented; no proposal, purchase, payment or contact was sent.",
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
